import asyncio
import json
from datetime import datetime
import os
import logging
from typing import List, Dict, Iterable, Tuple, Any

import duckdb
import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from psycopg2 import pool

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata._urns.urn_defs import SchemaFieldUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn
from zeta_lab.utilities.qtrack_db import create_duckdb_tables, check_postgres_tables_exist
from zeta_lab.utilities.tool import NameUtil, get_system_biz_id, get_system_tgt_srv_id, get_owner_srv_id, get_system_id, \
    get_biz_id, get_sql_obj_type


class ConvertQtrackSource(Source):
    def __init__(self, config: dict, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        self.setup_logger(self.config.get('prj_id', ''), self.config.get('logger_path', ''))
        self.logger.info(" ================================================================")
        self.logger.info(" ingesting source")
        self.logger.info(" ================================================================")
        self.logger.info("Initializing ConvertQtrackSource")
        self.system_biz_id = self.config['system_biz_id']
        self.duckdb_conn = duckdb.connect(self.config["duckdb_path"])
        self.logger.info(f"Connected to DuckDB at {self.config['duckdb_path']}")
        self.pg_pool = self.get_postgres_pool()
        self.logger.info("Initialized PostgreSQL connection pool")
        self.batch_size = self.config.get("batch_size", 100)
        self.max_workers = self.config.get("max_workers", 5)
        self.logger.info(f"Batch size: {self.batch_size}, Max workers: {self.max_workers}")
        self.initialize_databases()
        self.table_id_map = {}
        self.column_id_map = {}
        self.next_table_id = 1
        self.next_column_id = 1
        self.column_order = {}  # 테이블별 컬럼 순서를 추적하기 위한 딕셔너리
        self.properties_cache = {}  # 메모리 캐시

    @classmethod
    def create(cls, config_dict, ctx):
        return cls(config_dict, ctx)

    def initialize_databases(self):
        self.logger.info("Initializing databases")
        create_duckdb_tables(self.duckdb_conn)
        check_postgres_tables_exist(self.pg_pool, self.config['target_config'])

    def get_table_id(self, table_urn: str) -> int:
        if table_urn not in self.table_id_map:
            self.table_id_map[table_urn] = self.next_table_id
            self.next_table_id += 1
        return self.table_id_map[table_urn]

    def get_column_id(self, table_id: int, column_name: str) -> int:
        key = (table_id, column_name)
        if key not in self.column_id_map:
            self.column_id_map[key] = self.next_column_id
            self.next_column_id += 1
        return self.column_id_map[key]

    def get_next_column_order(self, table_urn: str) -> int:
        order = self.column_order.get(table_urn, 0)
        self.column_order[table_urn] = order + 1
        return order

    def extract_column_name(self, column_urn: str) -> str:
        """
        Extract column name from schema field URN
        """
        try:
            schema_field = SchemaFieldUrn.from_string(column_urn)
            return schema_field.field_path
        except Exception as e:
            self.logger.error(f"Error extracting column name from URN {column_urn}: {e}")
            return '*'  # Return default value in case of error

    def prefetch_dataset_properties(self, lineage_data: List[Dict]) -> None:
        """
        lineage 처리 전에 필요한 모든 데이터셋의 프로퍼티를 미리 조회
        """
        # 모든 필요한 URN을 수집
        needed_urns = set()
        for row in lineage_data:
            downstream_urn = row[0]  # row = (urn, metadata)
            metadata = eval(row[1])

            needed_urns.add(downstream_urn)
            for upstream in metadata.get('upstreams', []):
                needed_urns.add(upstream['dataset'])

        # 아직 캐시되지 않은 URN만 필터링
        urns_to_fetch = [urn for urn in needed_urns if urn not in self.properties_cache]

        # 청크 단위로 조회
        chunk_size = 50
        for i in range(0, len(urns_to_fetch), chunk_size):
            chunk = urns_to_fetch[i:i + chunk_size]
            self.fetch_properties_chunk(chunk)

    def fetch_properties_chunk(self, urns: List[str]) -> None:
        """
        여러 URN의 프로퍼티를 한 번에 조회
        """
        for urn in urns:
            try:
                url = f"{self.config['datahub_api']['server']}/aspects/{DatasetUrn.url_encode(urn)}?aspect=datasetProperties&version=0"
                with requests.get(url, timeout=self.config['datahub_api']['timeout_sec']) as response:
                    if response.status_code == 200:
                        self.properties_cache[urn] = response.json()
                    else:
                        self.properties_cache[urn] = {}
            except Exception as e:
                self.logger.error(f"Error fetching properties for {urn}: {e}")
                self.properties_cache[urn] = {}

    def get_dataset_properties(self, dataset_urn: str) -> Dict:
        """
        캐시된 프로퍼티 반환
        """
        return self.properties_cache.get(dataset_urn, {})

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        query = """
        SELECT urn, metadata FROM metadata_aspect_v2
        WHERE aspect_name = 'upstreamLineage' AND version = 0
        """
        results = self.duckdb_conn.execute(query).fetchall()

        # 모든 필요한 프로퍼티를 미리 조회
        self.prefetch_dataset_properties(results)

        for index, row in enumerate(results):
            downstream_urn = row[0]
            metadata = eval(row[1])  # Assuming metadata is stored as a string representation of a dict
            self.logger.info(f"{index + 1}/{len(results)}, Processing lineage for {downstream_urn}")
            self.process_lineage(downstream_urn, metadata)
            self.logger.info(f"{((index + 1) / len(results)) * 100} %, Processed lineage for {downstream_urn}")

        self.logger.info(f"Processed {len(results)} lineage records")

        # populate ais0103,ais0112,ais0080,ais0081
        self.populate_ais0103()
        self.populate_ais0112()
        self.populate_ais0080()
        self.populate_ais0081()

        # After processing all lineage records
        self.logger.info("Starting asynchronous transfer to PostgreSQL")
        asyncio.run(self.transfer_to_postgresql())

        return []

    def create_table_info(self, stream_urn: str, table_id: int,
                          query_custom_keys: Dict, stream_properties: Dict) -> None:
        prj_id = query_custom_keys.get('prj_id', '')
        file_id = int(query_custom_keys.get('file_id', 0))
        sql_id = int(query_custom_keys.get('sql_id', 0))
        obj_id = int(query_custom_keys.get('obj_id', 0))
        func_id = int(query_custom_keys.get('func_id', 0))
        query_type = query_custom_keys.get('query_type', '')
        query_type_props_str = query_custom_keys.get('query_type_props', '{}')

        # query_type_props가 문자열이므로 JSON 형식으로 파싱
        if isinstance(query_type_props_str, str):
            try:
                query_type_props = json.loads(query_type_props_str)
            except json.JSONDecodeError:
                query_type_props = {}  # 파싱 오류 시 기본값으로 빈 딕셔너리 사용
        else:
            query_type_props = query_type_props_str  # 이미 딕셔너리인 경우 그대로 사용

        # 이제 query_type_props에서 'temporary' 키를 안전하게 가져올 수 있음
        TEMPORARY_TABLE = '$tb'
        REGULAR_TABLE = 'tbl'
        FILE_TABLE = 'fil'
        stream_dataset = DatasetUrn.from_string(stream_urn)
        stream_content = stream_dataset.get_dataset_name()
        stream_table = NameUtil.get_table_name(stream_content)
        sql_obj_type = TEMPORARY_TABLE if query_type_props.get('temporary', False) else REGULAR_TABLE
        sql_obj_type = get_sql_obj_type(stream_table)
        sql_obj_type = FILE_TABLE if 's3://' in stream_table else sql_obj_type
        system_biz_id = self.system_biz_id if not self.system_biz_id else get_system_biz_id(stream_properties)
        system_tgt_srv_id = get_system_tgt_srv_id(stream_properties)
        owner_srv_id = get_owner_srv_id(stream_properties)
        system_id = get_system_id(stream_properties)
        biz_id = get_biz_id(stream_properties)

        try:
            self.duckdb_conn.execute("""
                        INSERT OR REPLACE INTO ais0102 
                        (prj_id, file_id, sql_id, table_id, obj_id, func_id, query_type, sql_obj_type, table_urn, system_biz_id,system_tgt_srv_id,owner_srv_id,system_id,biz_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?)
                    """, (
                prj_id, file_id, sql_id, table_id, obj_id, func_id, query_type, sql_obj_type, stream_urn, system_biz_id,
                system_tgt_srv_id, owner_srv_id, system_id, biz_id))
            self.logger.debug(f"Inserted/Updated record in ais0102 for table_urn: {stream_urn}")
        except duckdb.Error as e:
            self.logger.error(f"Error inserting into ais0102: {e}")

    def process_lineage(self, downstream_urn: str, metadata: Dict) -> None:
        upstreams = metadata.get('upstreams', [])
        fine_grained_lineages = metadata.get('fineGrainedLineages', [])
        downstream_properties = self.get_dataset_properties(downstream_urn)
        downstream_table_id = self.get_table_id(downstream_urn)

        # Process each upstream table - table level lineage only
        for upstream in upstreams:
            upstream_urn = upstream['dataset']
            query_custom_keys = upstream.get('query_custom_keys', {})
            upstream_properties = self.get_dataset_properties(upstream_urn)
            upstream_table_id = self.get_table_id(upstream_urn)

            # Create ais0102 entry (table info)
            self.create_table_info(upstream_urn, upstream_table_id, query_custom_keys, upstream_properties)
            self.create_table_info(downstream_urn, downstream_table_id, query_custom_keys, downstream_properties)

        # Process column level lineage separately
        if fine_grained_lineages:
            # Group all upstream URNs and their properties for column processing
            upstream_info = {
                upstream['dataset']: {
                    'table_id': self.get_table_id(upstream['dataset']),
                    'properties': self.get_dataset_properties(upstream['dataset']),
                    'query_custom_keys': upstream.get('query_custom_keys', {})
                }
                for upstream in upstreams
            }

            self.process_column_level_lineage(fine_grained_lineages, upstream_info,
                                              downstream_urn, downstream_table_id,
                                              downstream_properties)
        else:
            # If no fine-grained lineage exists, create virtual mappings for each upstream
            for upstream in upstreams:
                upstream_urn = upstream['dataset']
                query_custom_keys = upstream.get('query_custom_keys', {})
                upstream_properties = self.get_dataset_properties(upstream_urn)
                upstream_table_id = self.get_table_id(upstream_urn)

                self.create_virtual_column_lineage(
                    upstream_urn, downstream_urn,
                    query_custom_keys, upstream_table_id,
                    downstream_table_id,
                    upstream_properties, downstream_properties
                )

    def process_column_level_lineage(self, fine_grained_lineages: List[Dict],
                                     upstream_info: Dict,
                                     downstream_urn: str,
                                     downstream_table_id: int,
                                     downstream_properties: Dict) -> None:
        """
        Process fine-grained lineage information for column-level mapping

        Args:
            fine_grained_lineages: List of fine-grained lineage information
            upstream_info: Dictionary containing information about upstream tables
            downstream_urn: URN of the downstream table
            downstream_table_id: ID of the downstream table
            downstream_properties: Properties of the downstream table
        """
        for lineage in fine_grained_lineages:
            upstreams = lineage.get('upstreams', [])
            downstreams = lineage.get('downstreams', [])
            transform_operation = lineage.get('transformOperation', '')

            for upstream_col in upstreams:
                # Extract the table URN from the column URN
                # Format: "urn:li:schemaField:(urn:li:dataset:(platform,name,env),field_path)"
                upstream_table_urn = upstream_col[upstream_col.find("(") + 1:upstream_col.rfind(",")]
                if upstream_table_urn not in upstream_info:
                    self.logger.warning(f"Upstream table {upstream_table_urn} not found in upstream_info")
                    continue

                upstream_data = upstream_info[upstream_table_urn]

                for downstream_col in downstreams:
                    self.create_column_lineage_entry(
                        upstream_table_urn, downstream_urn,
                        upstream_col, downstream_col,
                        transform_operation,
                        upstream_data['query_custom_keys'],
                        upstream_data['table_id'],
                        downstream_table_id,
                        upstream_data['properties'],
                        downstream_properties
                    )

    def create_virtual_column_lineage(self, upstream_urn: str, downstream_urn: str,
                                      query_custom_keys: Dict, upstream_table_id: int,
                                      downstream_table_id: int,
                                      downstream_properties: Dict, upstream_properties: Dict) -> None:
        """
        Create virtual '*' column mapping when no fine-grained lineage exists
        """
        upstream_virtual_col = f"urn:li:schemaField:({upstream_urn},*)"
        downstream_virtual_col = f"urn:li:schemaField:({downstream_urn},*)"

        self.create_column_lineage_entry(
            upstream_urn, downstream_urn,
            upstream_virtual_col, downstream_virtual_col,
            '', query_custom_keys,
            upstream_table_id, downstream_table_id,
            upstream_properties, downstream_properties
        )

    def create_column_lineage_entry(self, upstream_urn: str, downstream_urn: str,
                                    upstream_col_urn: str, downstream_col_urn: str,
                                    transform_operation: str, query_custom_keys: Dict,
                                    upstream_table_id: int, downstream_table_id: int,
                                    upstream_properties: Dict, downstream_properties: Dict) -> None:
        """
        Create an entry in the ais0113 table for column-level lineage
        """
        try:
            # Extract column names from URNs
            upstream_col_name = self.extract_column_name(upstream_col_urn)
            downstream_col_name = self.extract_column_name(downstream_col_urn)

            # Get column IDs
            upstream_col_id = self.get_column_id(upstream_table_id, upstream_col_name)
            downstream_col_id = self.get_column_id(downstream_table_id, downstream_col_name)

            # Process URNs for owner and table information
            upstream_dataset = DatasetUrn.from_string(upstream_urn)
            downstream_dataset = DatasetUrn.from_string(downstream_urn)

            upstream_content = upstream_dataset.get_dataset_name()
            downstream_content = downstream_dataset.get_dataset_name()

            upstream_owner = NameUtil.get_schema(upstream_content).upper()
            upstream_table = NameUtil.get_table_name(upstream_content)
            upstream_sql_obj_type = get_sql_obj_type(upstream_table)
            upstream_unique_owner = NameUtil.get_unique_owner_name(upstream_content).upper()
            upstream_unique_owner_tgt_srv_id = NameUtil.get_unique_owner_tgt_srv_id(upstream_content).upper()
            upstream_system_biz_id = self.system_biz_id if not self.system_biz_id else get_system_biz_id(upstream_properties)

            downstream_owner = NameUtil.get_schema(downstream_content).upper()
            downstream_table = NameUtil.get_table_name(downstream_content)
            downstream_sql_obj_type = get_sql_obj_type(downstream_table)
            downstream_unique_owner = NameUtil.get_unique_owner_name(downstream_content).upper()
            downstream_unique_owner_tgt_srv_id = NameUtil.get_unique_owner_tgt_srv_id(downstream_content).upper()
            downstream_system_biz_id = self.system_biz_id if not self.system_biz_id else get_system_biz_id(downstream_properties)

            # 컬럼 순서 가져오기
            upstream_col_order_no = self.get_next_column_order(upstream_urn)
            downstream_col_order_no = self.get_next_column_order(downstream_urn)

            # Insert into ais0113
            self.duckdb_conn.execute("""
                INSERT INTO ais0113 (
                    prj_id, file_id, sql_id, table_id, col_id, obj_id, func_id,
                    owner_name, table_name, caps_table_name, sql_obj_type,
                    col_name, caps_col_name, col_value_yn, col_expr,
                    col_name_org, caps_col_name_org,
                    unique_owner_name, unique_owner_tgt_srv_id,
                    system_biz_id,
                    call_prj_id, call_file_id, call_sql_id, call_table_id, call_col_id,
                    call_obj_id, call_func_id, call_owner_name, call_table_name,
                    call_caps_table_name, call_sql_obj_type,
                    call_col_name, call_caps_col_name, call_col_value_yn, call_col_expr,
                    call_col_name_org, call_caps_col_name_org,
                    call_unique_owner_name, call_unique_owner_tgt_srv_id,
                    call_system_biz_id,
                    col_order_no,
                    call_col_order_no, 
                    adj_col_order_no,
                    call_adj_col_order_no,                                        
                    cond_mapping, data_maker, mapping_kind
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?)
            """, (
                query_custom_keys.get('prj_id', ''),
                int(query_custom_keys.get('file_id', 0)),
                int(query_custom_keys.get('sql_id', 0)),
                upstream_table_id,
                upstream_col_id,
                int(query_custom_keys.get('obj_id', 0)),
                int(query_custom_keys.get('func_id', 0)),
                upstream_owner,
                upstream_table,
                upstream_table.upper(),
                upstream_sql_obj_type,
                upstream_col_name,
                upstream_col_name.upper() if upstream_col_name != '*' else '*',
                'N',  # col_value_yn
                transform_operation,  # col_expr
                upstream_col_name,  # col_name_org
                upstream_col_name.upper() if upstream_col_name != '*' else '*',  # caps_col_name_org
                upstream_unique_owner,
                upstream_unique_owner_tgt_srv_id,
                upstream_system_biz_id,
                query_custom_keys.get('prj_id', ''),
                int(query_custom_keys.get('file_id', 0)),
                int(query_custom_keys.get('sql_id', 0)),
                downstream_table_id,
                downstream_col_id,
                int(query_custom_keys.get('obj_id', 0)),
                int(query_custom_keys.get('func_id', 0)),
                downstream_owner,
                downstream_table,
                downstream_table.upper(),
                downstream_sql_obj_type,
                downstream_col_name,
                downstream_col_name.upper() if downstream_col_name != '*' else '*',
                'N',  # call_col_value_yn
                '',  # call_col_expr
                downstream_col_name,  # call_col_name_org
                downstream_col_name.upper() if downstream_col_name != '*' else '*',  # call_caps_col_name_org
                downstream_unique_owner,
                downstream_unique_owner_tgt_srv_id,
                downstream_system_biz_id,
                upstream_col_order_no,
                downstream_col_order_no,
                upstream_col_order_no,
                downstream_col_order_no,
                1,  # cond_mapping
                'ingest_cli',  # data_maker
                ''  # mapping_kind
            ))
            self.logger.debug(f"Created ais0113 entry for {upstream_col_name} -> {downstream_col_name}")
        except Exception as e:
            self.logger.error(f"Error creating ais0113 entry: {e}")

    def populate_table_with_batch(self, table_name: str, df_insert: pd.DataFrame, batch_size: int = 1000):
        total_rows = len(df_insert)
        processed_rows = 0

        # 컬럼 목록 준비
        columns = ', '.join(df_insert.columns)
        placeholders = ', '.join(['?'] * len(df_insert.columns))  # DuckDB는 ? 를 placeholder로 사용

        insert_query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
        """

        while processed_rows < total_rows:
            try:
                # 현재 배치 준비
                current_batch = df_insert.iloc[processed_rows:processed_rows + batch_size]
                batch_values = [tuple(row) for row in current_batch.values]

                # DuckDB batch insert
                self.duckdb_conn.executemany(insert_query, batch_values)

                processed_rows += len(current_batch)
                self.logger.info(f"Processed {processed_rows}/{total_rows} rows for {table_name}")

            except Exception as e:
                self.logger.error(f"Error processing batch at row {processed_rows}: {e}")
                raise

    def populate_ais0103(self):
        self.logger.info("Populating ais0103 from ais0113")
        try:

            # SQL 쿼리
            sql_query = """
                select
                    distinct
                    a.prj_id,
                    a.file_id,
                    a.sql_id,
                    a.table_id,
                    a.col_id,
                    a.caps_col_name
                from 
                (
                    select 
                        prj_id,
                        file_id,
                        sql_id,
                        table_id,
                        col_id,
                        caps_col_name
                    from
                        ais0113
                    union all
                    select 
                        call_prj_id,
                        call_file_id,
                        call_sql_id,
                        call_table_id,
                        call_col_id,
                        call_caps_col_name
                    from
                        ais0113
                ) A
            """

            # 쿼리 실행 및 데이터 가져오기
            df = self.duckdb_conn.execute(sql_query).df()

            # ais0103 테이블의 컬럼 순서에 맞게 데이터 프레임 재구성
            columns_order = [
                'prj_id', 'file_id', 'sql_id', 'table_id', 'col_id', 'caps_col_name'
            ]

            df_insert = df[columns_order]

            # 결과를 ais0103 테이블에 batch로 삽입
            self.populate_table_with_batch('ais0103', df_insert)

        except duckdb.Error as e:
            self.logger.error(f"Error populating ais0103: {e}")

    def populate_ais0112(self):
        self.logger.info("Populating ais0112 from ais0113")
        try:

            # SQL 쿼리
            sql_query = """
                select
                    distinct
                    prj_id, file_id, sql_id, table_id, call_prj_id,
                    call_file_id, call_sql_id, call_table_id, obj_id, func_id,
                    owner_name, table_name, caps_table_name, sql_obj_type, call_obj_id,
                    call_func_id, call_owner_name, call_table_name, call_caps_table_name, call_sql_obj_type,
                    unique_owner_name, call_unique_owner_name, unique_owner_tgt_srv_id, call_unique_owner_tgt_srv_id, 2 as cond_mapping_bit,
                    data_maker, mapping_kind, system_biz_id, call_system_biz_id                    
                from
                    ais0113
            """

            # 쿼리 실행 및 데이터 가져오기
            df = self.duckdb_conn.execute(sql_query).df()

            # ais0112 테이블의 컬럼 순서에 맞게 데이터 프레임 재구성
            columns_order = [
                'prj_id', 'file_id', 'sql_id', 'table_id', 'call_prj_id',
                'call_file_id', 'call_sql_id', 'call_table_id', 'obj_id', 'func_id',
                'owner_name', 'table_name', 'caps_table_name', 'sql_obj_type', 'call_obj_id',
                'call_func_id', 'call_owner_name', 'call_table_name', 'call_caps_table_name', 'call_sql_obj_type',
                'unique_owner_name', 'call_unique_owner_name', 'unique_owner_tgt_srv_id',
                'call_unique_owner_tgt_srv_id', 'cond_mapping_bit',
                'data_maker', 'mapping_kind', 'system_biz_id', 'call_system_biz_id'
            ]

            df_insert = df[columns_order]

            # 결과를 ais0112 테이블에 batch로 삽입
            self.populate_table_with_batch('ais0112', df_insert)

        except duckdb.Error as e:
            self.logger.error(f"Error populating ais0112: {e}")

    def populate_ais0080(self):
        self.logger.info("Populating ais0080 from ais0081")
        try:

            # SQL 쿼리
            sql_query = """
                SELECT DISTINCT
                    src_prj_id, src_owner_name, src_caps_table_name, src_table_name, src_table_name_org,
                    src_table_type, src_mte_table_id,
                    tgt_prj_id, tgt_owner_name, tgt_caps_table_name, tgt_table_name, tgt_table_name_org,
                    tgt_table_type, tgt_mte_table_id,
                    src_owner_tgt_srv_id, tgt_owner_tgt_srv_id,
                    2 as cond_mapping_bit, mapping_kind,
                    src_system_biz_id, tgt_system_biz_id,
                    src_db_instance_org, src_schema_org, tgt_db_instance_org, tgt_schema_org,
                    src_system_id, src_biz_id, tgt_system_id, tgt_biz_id                                              
                FROM ais0081
            """

            # 쿼리 실행 및 데이터 가져오기
            df = self.duckdb_conn.execute(sql_query).df()

            # ais0080 테이블의 컬럼 순서에 맞게 데이터 프레임 재구성
            columns_order = [
                'src_prj_id', 'src_owner_name', 'src_caps_table_name', 'src_table_name', 'src_table_name_org',
                'src_table_type', 'src_mte_table_id',
                'tgt_prj_id', 'tgt_owner_name', 'tgt_caps_table_name', 'tgt_table_name', 'tgt_table_name_org',
                'tgt_table_type', 'tgt_mte_table_id',
                'src_owner_tgt_srv_id', 'tgt_owner_tgt_srv_id',
                'cond_mapping_bit', 'mapping_kind',
                'src_system_biz_id', 'tgt_system_biz_id',
                'src_db_instance_org', 'src_schema_org', 'tgt_db_instance_org', 'tgt_schema_org',
                'src_system_id', 'src_biz_id', 'tgt_system_id', 'tgt_biz_id'
            ]

            df_insert = df[columns_order]

            # 결과를 ais0080 테이블에 batch로 삽입
            self.populate_table_with_batch('ais0080', df_insert)

        except duckdb.Error as e:
            self.logger.error(f"Error populating ais0080: {e}")

    def populate_ais0081(self):
        self.logger.info("Populating ais0081 from ais0113")
        try:
            # SQL 쿼리
            sql_query = """
                SELECT DISTINCT
                    prj_id AS src_prj_id, 
                    owner_name AS src_owner_name, 
                    caps_table_name AS src_caps_table_name, 
                    table_name AS src_table_name,
                    caps_table_name AS src_table_name_org,
                    sql_obj_type AS src_table_type, 
                    cast(file_id as VARCHAR) AS src_mte_table_id,
                    CASE WHEN caps_col_name = '*' THEN '[*+*]' ELSE caps_col_name END AS src_caps_col_name, 
                    CASE WHEN col_name = '*' THEN '[*+*]' ELSE col_name END AS src_col_name, 
                    col_value_yn AS src_col_value_yn,
                    col_id AS src_mte_col_id,
                    call_prj_id AS tgt_prj_id, 
                    call_owner_name AS tgt_owner_name, 
                    call_caps_table_name AS tgt_caps_table_name, 
                    call_table_name AS tgt_table_name, 
                    call_caps_table_name AS tgt_table_name_org,
                    call_sql_obj_type AS tgt_table_type,
                    cast(call_file_id as VARCHAR) AS tgt_mte_table_id,
                    CASE WHEN call_caps_col_name = '*' THEN '[*+*]' ELSE call_caps_col_name END  AS tgt_caps_col_name, 
                    CASE WHEN call_col_name = '*' THEN '[*+*]' ELSE call_col_name END  AS tgt_col_name, 
                    call_col_value_yn AS tgt_col_value_yn,
                    call_col_id AS tgt_mte_col_id,
                    unique_owner_tgt_srv_id AS src_owner_tgt_srv_id, 
                    call_unique_owner_tgt_srv_id AS tgt_owner_tgt_srv_id, 
                    cond_mapping, 
                    mapping_kind, 
                    data_maker,
                    system_biz_id AS src_system_biz_id,
                    call_system_biz_id AS tgt_system_biz_id,
                    CASE 
                        WHEN split_part(unique_owner_name, '.', 2) = '' THEN '[owner_undefined]' 
                        WHEN split_part(unique_owner_name, '.', 2) IS NULL THEN split_part(unique_owner_name, '.', 1)
                        ELSE split_part(unique_owner_name, '.', 1)
                    END AS src_db_instance_org,
                    CASE 
                        WHEN split_part(unique_owner_name, '.', 2) = '' THEN '[owner_undefined]'
                        WHEN split_part(unique_owner_name, '.', 2) IS NULL THEN split_part(unique_owner_name, '.', 1)
                        ELSE split_part(unique_owner_name, '.', 2)
                    END AS src_schema_org,
                    CASE 
                        WHEN split_part(call_unique_owner_name, '.', 2) = '' THEN '[owner_undefined]' 
                        WHEN split_part(call_unique_owner_name, '.', 2) IS NULL THEN split_part(call_unique_owner_name, '.', 1)
                        ELSE split_part(call_unique_owner_name, '.', 1)
                    END AS tgt_db_instance_org,
                    CASE 
                        WHEN split_part(call_unique_owner_name, '.', 2) = '' THEN '[owner_undefined]'
                        WHEN split_part(call_unique_owner_name, '.', 2) IS NULL THEN split_part(call_unique_owner_name, '.', 1)
                        ELSE split_part(call_unique_owner_name, '.', 2)
                    END AS tgt_schema_org,          
                    split_part(system_biz_id, '_', 1) AS src_system_id,
                    CASE 
                        WHEN split_part(system_biz_id, '_', 1) LIKE '[owner%' AND split_part(system_biz_id, '_', 2) = 'undefined' THEN 'undefined'
                        ELSE split_part(system_biz_id, '_', 2)
                    END AS src_biz_id,      
                    split_part(system_biz_id, '_', 1) AS tgt_system_id,
                    CASE 
                        WHEN split_part(call_system_biz_id, '_', 1) LIKE 'owner%' AND split_part(call_system_biz_id, '_', 2) = 'undefined' THEN 'undefined'
                        ELSE split_part(call_system_biz_id, '_', 2)
                    END AS tgt_biz_id                                               
                FROM ais0113
            """

            # 쿼리 실행 및 데이터 가져오기
            df = self.duckdb_conn.execute(sql_query).df()

            # ais0081 테이블의 컬럼 순서에 맞게 데이터 프레임 재구성
            columns_order = [
                'src_prj_id', 'src_owner_name', 'src_caps_table_name', 'src_table_name', 'src_table_name_org',
                'src_table_type', 'src_mte_table_id',
                'src_caps_col_name', 'src_col_name', 'src_col_value_yn', 'src_mte_col_id',
                'tgt_prj_id', 'tgt_owner_name', 'tgt_caps_table_name', 'tgt_table_name', 'tgt_table_name_org',
                'tgt_table_type', 'tgt_mte_table_id',
                'tgt_caps_col_name', 'tgt_col_name', 'tgt_col_value_yn', 'tgt_mte_col_id',
                'src_owner_tgt_srv_id', 'tgt_owner_tgt_srv_id',
                'cond_mapping', 'mapping_kind', 'data_maker',
                'src_system_biz_id', 'tgt_system_biz_id',
                'src_db_instance_org', 'src_schema_org', 'tgt_db_instance_org', 'tgt_schema_org',
                'src_system_id', 'src_biz_id', 'tgt_system_id', 'tgt_biz_id'
            ]

            df_insert = df[columns_order]

            # 결과를 ais0081 테이블에 batch로 삽입
            self.populate_table_with_batch('ais0081', df_insert)

        except duckdb.Error as e:
            self.logger.error(f"Error populating ais0081: {e}")

    async def transfer_to_postgresql(self):
        self.logger.info("Starting asynchronous batch transfer to PostgreSQL")

        try:
            # Delete existing records first
            await self.delete_existing_records()

            # Transfer ais0102
            await self.transfer_ais0102()

            # Transfer ais0103
            await self.transfer_table('ais0103')

            # Transfer ais0112
            await self.transfer_table('ais0112')

            # Transfer ais0113
            await self.transfer_table('ais0113')

            # Transfer ais0080
            await self.transfer_table_with_sequence('ais0080')

            # Transfer ais0081
            await self.transfer_table_with_sequence('ais0081')

        except Exception as e:
            self.logger.error(f"Error during transfer to PostgreSQL: {e}")

        self.logger.info("Completed asynchronous batch transfer to PostgreSQL")

    async def transfer_ais0102(self):
        self.logger.info("Transferring ais0102 to PostgreSQL")

        # Get total count
        total_count = self.duckdb_conn.execute("SELECT COUNT(*) FROM ais0102").fetchone()[0]

        offset = 0
        sem = asyncio.Semaphore(self.max_workers)
        tasks = []

        while offset < total_count:
            # Fetch batch from DuckDB
            query = f"""
                SELECT 
                    prj_id, file_id, sql_id, table_id, obj_id, func_id,
                    query_type, sql_obj_type, table_urn
                FROM ais0102 
                LIMIT {self.batch_size} 
                OFFSET {offset}
            """
            batch = self.duckdb_conn.execute(query).fetchall()

            if not batch:
                break

            # Process batch
            processed_batch = []
            for row in batch:
                prj_id, file_id, sql_id, table_id, obj_id, func_id, \
                    query_type, sql_obj_type, table_urn = row

                # Process table_urn to get table_name
                try:
                    dataset_urn = DatasetUrn.from_string(table_urn)
                    table_content = dataset_urn.get_dataset_name()
                    table_name = NameUtil.get_table_name(table_content)
                    caps_table_name = table_name.upper()
                    owner_name = NameUtil.get_schema(table_content)
                    unique_owner_name = NameUtil.get_unique_owner_name(table_content)
                    unique_owner_tgt_srv_id = NameUtil.get_unique_owner_tgt_srv_id(table_content)
                except Exception as e:
                    self.logger.error(f"Error processing table_urn {table_urn}: {e}")
                    continue

                # Process query_type
                pg_query_type = query_type[0] if query_type else None
                if pg_query_type == 'I':
                    pg_query_type = 'C'

                # Set sql_state based on query_type
                sql_state_map = {
                    'C': 'INSERT',
                    'S': 'SELECT',
                    'I': 'INSERT',
                    'U': 'UPDATE',
                    'D': 'DELETE'
                }
                sql_state = sql_state_map.get(query_type[0] if query_type else '', None)

                # Prepare row for PostgreSQL
                processed_row = (
                    prj_id,  # prj_id
                    float(file_id),  # file_id
                    float(sql_id),  # sql_id
                    float(table_id),  # table_id
                    float(obj_id),  # obj_id
                    float(func_id),  # func_id
                    table_name,  # table_name
                    caps_table_name,  # caps_table_name
                    owner_name,  # owner_name
                    pg_query_type,  # query_type
                    None,  # query_line_no
                    sql_obj_type,  # sql_obj_type
                    None,  # inlineview_yn
                    None,  # dblink_name
                    None,  # table_alias_name
                    None,  # inlineview_src
                    sql_state,  # sql_state
                    None,  # column_no
                    None,  # table_depth
                    None,  # table_order_no
                    None,  # rel_table_id
                    None,  # rel_flow_id
                    None,  # dbc_mapping_yn
                    None,  # teradata_sql_id
                    unique_owner_name,  # unique_owner_name
                    unique_owner_tgt_srv_id,  # unique_owner_tgt_srv_id
                    None,  # sql_name
                    None,  # system_biz_id
                    None  # fl_tbl_uid
                )
                processed_batch.append(processed_row)

            # Create and start task for batch insert
            if processed_batch:
                task = asyncio.create_task(self.insert_ais0102_batch(sem, processed_batch))
                tasks.append(task)

            offset += self.batch_size
            self.logger.info(f"Created task for {min(offset, total_count)}/{total_count} records for ais0102")

        # Wait for all tasks to complete
        await asyncio.gather(*tasks)

    async def insert_ais0102_batch(self, sem: asyncio.Semaphore, batch: List[Tuple]):
        async with sem:
            insert_query = """
                INSERT INTO ais0102 (
                    prj_id, file_id, sql_id, table_id, obj_id, func_id,
                    table_name, caps_table_name, owner_name, query_type,
                    query_line_no, sql_obj_type, inlineview_yn, dblink_name,
                    table_alias_name, inlineview_src, sql_state, column_no,
                    table_depth, table_order_no, rel_table_id, rel_flow_id,
                    dbc_mapping_yn, teradata_sql_id, unique_owner_name,
                    unique_owner_tgt_srv_id, sql_name, system_biz_id, fl_tbl_uid
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT DO NOTHING
            """

            conn = await asyncio.to_thread(self.pg_pool.getconn)
            try:
                cur = await asyncio.to_thread(conn.cursor)
                try:
                    await asyncio.to_thread(
                        psycopg2.extras.execute_batch,
                        cur, insert_query, batch, page_size=100
                    )
                    await asyncio.to_thread(conn.commit)
                    self.logger.info(f"Inserted {len(batch)} records into ais0102")
                finally:
                    await asyncio.to_thread(cur.close)
            except Exception as e:
                await asyncio.to_thread(conn.rollback)
                self.logger.error(f"Error inserting batch into ais0102: {e}")
            finally:
                await asyncio.to_thread(self.pg_pool.putconn, conn)

    async def transfer_table(self, table_name: str):
        self.logger.info(f"Transferring {table_name} to PostgreSQL")

        # Get total count
        total_count = self.duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        # Get column information
        columns_info = self.get_columns_info(table_name)

        offset = 0
        sem = asyncio.Semaphore(self.max_workers)
        tasks = []

        while offset < total_count:
            # Fetch batch from DuckDB
            query = f"SELECT * FROM {table_name} LIMIT {self.batch_size} OFFSET {offset}"
            batch = self.duckdb_conn.execute(query).fetchall()

            if not batch:
                break

            # Create and start task for batch insert
            task = asyncio.create_task(self.insert_batch(sem, table_name, columns_info, batch))
            tasks.append(task)

            offset += self.batch_size
            self.logger.info(f"Created task for {min(offset, total_count)}/{total_count} records for {table_name}")

        # Wait for all tasks to complete
        await asyncio.gather(*tasks)

    async def transfer_table_with_sequence(self, table_name: str):
        self.logger.info(f"Transferring {table_name} to PostgreSQL with sequence")

        # Get total count
        total_count = self.duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        # Get column information
        columns_info = self.get_columns_info(table_name)

        offset = 0
        sem = asyncio.Semaphore(self.max_workers)
        tasks = []

        while offset < total_count:
            # Fetch batch from DuckDB
            query = f"SELECT * FROM {table_name} LIMIT {self.batch_size} OFFSET {offset}"
            batch = self.duckdb_conn.execute(query).fetchall()

            if not batch:
                break

            # Create and start task for batch insert
            task = asyncio.create_task(self.insert_batch_with_sequence(sem, table_name, columns_info, batch))
            tasks.append(task)

            offset += self.batch_size
            self.logger.info(f"Created task for {min(offset, total_count)}/{total_count} records for {table_name}")

        # Wait for all tasks to complete
        await asyncio.gather(*tasks)

    async def delete_existing_records(self):
        self.logger.info("Deleting existing records from target tables")

        delete_queries = [
            "DELETE FROM ais0102 WHERE prj_id = %s",
            "DELETE FROM ais0103 WHERE prj_id = %s",
            "DELETE FROM ais0112 WHERE prj_id = %s",
            "DELETE FROM ais0113 WHERE prj_id = %s",
            "DELETE FROM ais0080 WHERE src_prj_id = %s",
            "DELETE FROM ais0081 WHERE src_prj_id = %s"
        ]

        try:
            conn = await asyncio.to_thread(self.pg_pool.getconn)
            try:
                cur = await asyncio.to_thread(conn.cursor)
                try:
                    prj_id = self.config.get('prj_id', '')  # prj_id를 설정에서 가져오거나 적절한 방법으로 설정

                    for query in delete_queries:
                        await asyncio.to_thread(cur.execute, query, (prj_id,))
                        deleted_rows = cur.rowcount
                        self.logger.info(f"Deleted {deleted_rows} rows using query: {query}")

                    await asyncio.to_thread(conn.commit)
                    self.logger.info("Successfully deleted existing records from ais0080 and ais0081")
                finally:
                    await asyncio.to_thread(cur.close)
            except Exception as e:
                await asyncio.to_thread(conn.rollback)
                self.logger.error(f"Error deleting existing records: {e}")
            finally:
                await asyncio.to_thread(self.pg_pool.putconn, conn)
        except Exception as e:
            self.logger.error(f"Error in database operation for deleting existing records: {e}")

    def get_columns_info(self, table_name: str) -> List[Tuple[str, Any]]:
        query = f"DESCRIBE {table_name}"
        columns_info = self.duckdb_conn.execute(query).fetchall()
        return [(col[0], col[1]) for col in columns_info]

    async def insert_batch(self, sem: asyncio.Semaphore, table_name: str, columns_info: List[Tuple[str, Any]],
                           batch: List[Tuple]):
        async with sem:
            # Prepare INSERT statement with appropriate placeholders
            columns = [col[0] for col in columns_info]
            placeholders = [self.get_placeholder(col[1]) for col in columns_info]
            insert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT DO NOTHING
            """

            # Convert batch data according to column types
            converted_batch = [self.convert_row(row, columns_info) for row in batch]

            conn = await asyncio.to_thread(self.pg_pool.getconn)
            try:
                cur = await asyncio.to_thread(conn.cursor)
                try:
                    await asyncio.to_thread(
                        psycopg2.extras.execute_batch,
                        cur, insert_query, converted_batch, page_size=100
                    )
                    await asyncio.to_thread(conn.commit)
                    self.logger.debug(f"Inserted {len(batch)} records into {table_name}")
                finally:
                    await asyncio.to_thread(cur.close)
            except Exception as e:
                await asyncio.to_thread(conn.rollback)
                self.logger.error(f"Error inserting batch into {table_name}: {e}")
            finally:
                await asyncio.to_thread(self.pg_pool.putconn, conn)

    async def insert_batch_with_sequence(self, sem: asyncio.Semaphore, table_name: str,
                                         columns_info: List[Tuple[str, Any]], batch: List[Tuple]):
        async with sem:
            # Prepare INSERT statement with appropriate placeholders
            duckdb_columns = [col[0] for col in columns_info]
            placeholders = [self.get_placeholder(col[1]) for col in columns_info]

            # Find the indices of src_prj_id and tgt_prj_id
            src_prj_id_index = duckdb_columns.index('src_prj_id')
            tgt_prj_id_index = duckdb_columns.index('tgt_prj_id')

            insert_query = f"""
            INSERT INTO {table_name} (
                seq_id, 
                src_system_tgt_srv_id, 
                tgt_system_tgt_srv_id, 
                {', '.join(duckdb_columns)}
            )
            VALUES (
                nextval('seq_{table_name}'), 
                AP_COMMON_FN_SYSTEM_TGTSRVID(%s)::varchar(100), 
                AP_COMMON_FN_SYSTEM_TGTSRVID(%s)::varchar(100), 
                {', '.join(['%s' for _ in duckdb_columns])}
            )
            ON CONFLICT DO NOTHING
        """
            self.logger.debug(f"Insert query for {table_name}: {insert_query}")

            # Convert batch data according to column types
            converted_batch = [self.convert_row_for_postgres(row, columns_info, src_prj_id_index, tgt_prj_id_index) for
                               row in batch]

            # Log the number of columns and first row for debugging
            self.logger.debug(f"Number of columns in data: {len(converted_batch[0])}")
            self.logger.debug(
                f"Number of placeholders in query: {len(duckdb_columns) + 2}")  # +2 for src_system_tgt_srv_id and tgt_system_tgt_srv_id
            self.logger.debug(f"First row of data: {converted_batch[0]}")

            conn = await asyncio.to_thread(self.pg_pool.getconn)
            try:
                cur = await asyncio.to_thread(conn.cursor)
                try:
                    await asyncio.to_thread(
                        psycopg2.extras.execute_batch,
                        cur, insert_query, converted_batch, page_size=100
                    )
                    await asyncio.to_thread(conn.commit)
                    self.logger.debug(f"Inserted {len(batch)} records into {table_name}")
                except Exception as e:
                    self.logger.error(f"Error inserting batch into {table_name}: {e}")
                    self.logger.error(f"Problematic row: {converted_batch[0]}")
                    raise
                finally:
                    await asyncio.to_thread(cur.close)
            except Exception as e:
                await asyncio.to_thread(conn.rollback)
                self.logger.error(f"Error in database operation for {table_name}: {e}")
            finally:
                await asyncio.to_thread(self.pg_pool.putconn, conn)

    def get_placeholder(self, col_type: str) -> str:
        if 'INTEGER' in col_type.upper() or 'NUMERIC' in col_type.upper() or 'DECIMAL' in col_type.upper() or 'FLOAT' in col_type.upper():
            return '%s::numeric'
        else:
            return '%s'

    def convert_row(self, row: Tuple, columns_info: List[Tuple[str, Any]]) -> Tuple:
        converted_row = []
        for value, (_, col_type) in zip(row, columns_info):
            if value == '' or value is None:
                converted_row.append(None)
            elif 'INTEGER' in col_type.upper():
                converted_row.append(int(value) if value is not None else None)
            elif 'NUMERIC' in col_type.upper() or 'DECIMAL' in col_type.upper() or 'FLOAT' in col_type.upper():
                converted_row.append(float(value) if value is not None else None)
            elif 'BOOLEAN' in col_type.upper():
                converted_row.append(bool(value) if value is not None else None)
            else:
                converted_row.append(str(value))
        return tuple(converted_row)

    def convert_row_for_postgres(self, row: Tuple, columns_info: List[Tuple[str, Any]], src_prj_id_index: int,
                                 tgt_prj_id_index: int) -> Tuple:
        converted_row = []
        for i, (value, (_, col_type)) in enumerate(zip(row, columns_info)):
            if value == '' or value is None:
                converted_row.append(None)
            elif 'INTEGER' in col_type.upper():
                converted_row.append(int(value) if value is not None else None)
            elif 'NUMERIC' in col_type.upper() or 'DECIMAL' in col_type.upper() or 'FLOAT' in col_type.upper():
                converted_row.append(float(value) if value is not None else None)
            elif 'BOOLEAN' in col_type.upper():
                converted_row.append(bool(value) if value is not None else None)
            else:
                converted_row.append(str(value))

        # Add src_prj_id and tgt_prj_id at the beginning for AP_COMMON_FN_SYSTEM_TGTSRVID
        return (converted_row[src_prj_id_index], converted_row[tgt_prj_id_index], *converted_row)

    def get_postgres_pool(self):
        self.logger.info("Creating PostgreSQL connection pool")
        pg_config = self.config['target_config']
        try:
            pool = psycopg2.pool.SimpleConnectionPool(
                1, 10,
                host=pg_config['host_port'].split(':')[0],
                port=pg_config['host_port'].split(':')[1],
                database=pg_config['database'],
                user=pg_config['username'],
                password=pg_config['password']
            )
            self.logger.info("Successfully created PostgreSQL connection pool")
            return pool
        except Exception as e:
            self.logger.error(f"Failed to create PostgreSQL connection pool: {e}")
            raise

    def get_report(self):
        self.logger.info("Generating report")
        return self.report

    def close(self):
        self.logger.info("Closing connections")
        try:
            self.duckdb_conn.close()
            self.logger.info("Closed DuckDB connection")
        except Exception as e:
            self.logger.error(f"Error closing DuckDB connection: {e}")

        try:
            self.pg_pool.closeall()
            self.logger.info("Closed all PostgreSQL connections")
        except Exception as e:
            self.logger.error(f"Error closing PostgreSQL connections: {e}")

    def setup_logger(self, prj_id: str, logger_path: str):
        # 현재 날짜를 가져옵니다
        current_date = datetime.now().strftime("%Y-%m-%d")

        # 로그 파일명을 생성합니다
        log_filename = f"analyzer_{prj_id}.log_{current_date}"

        # 전체 로그 파일 경로를 생성합니다
        full_log_path = os.path.join(logger_path, log_filename)

        # 로그 디렉토리가 존재하지 않으면 생성합니다
        os.makedirs(logger_path, exist_ok=True)

        # 로거를 설정합니다
        self.logger = logging.getLogger("analyzer")
        self.logger.setLevel(logging.INFO)

        # 파일 핸들러를 생성합니다 (파일이 있으면 append, 없으면 생성)
        file_handler = logging.FileHandler(full_log_path, mode='a')

        # 커스텀 포매터를 생성합니다
        class CustomFormatter(logging.Formatter):
            def format(self, record):
                level_map = {
                    'INFO': 'info',
                    'WARNING': 'warn',
                    'ERROR': 'eror',
                    'DEBUG': 'dbug'
                }
                record.levelname = level_map.get(record.levelname, record.levelname.lower())
                return super().format(record)

        formatter = CustomFormatter('[%(asctime)s] [%(levelname)s] %(message)s',
                                    datefmt='%Y.%m.%d %H:%M:%S')
        file_handler.setFormatter(formatter)

        # 핸들러를 로거에 추가합니다
        self.logger.addHandler(file_handler)