import asyncio
import json
from datetime import datetime
import os
import logging
from typing import List, Dict, Iterable, Tuple, Any

import duckdb
import psycopg2
import psycopg2.extras
import requests
from psycopg2 import pool

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata._urns.urn_defs import SchemaFieldUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn
from zeta_lab.utilities.qtrack_db import create_duckdb_tables, check_postgres_tables_exist, populate_ais0080, \
    populate_ais0081
from zeta_lab.utilities.tool import NameUtil, get_system_biz_id, get_system_tgt_srv_id, get_owner_srv_id, get_system_id, \
    get_system_name, get_biz_id, get_biz_name


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
        self.duckdb_conn = duckdb.connect(self.config["duckdb_path"])
        self.logger.info(f"Connected to DuckDB at {self.config['duckdb_path']}")
        self.pg_pool = self.get_postgres_pool()
        self.logger.info("Initialized PostgreSQL connection pool")
        self.batch_size = self.config.get("batch_size", 1000)
        self.max_workers = self.config.get("max_workers", 5)
        self.logger.info(f"Batch size: {self.batch_size}, Max workers: {self.max_workers}")
        self.initialize_databases()
        self.table_id_map = {}
        self.column_id_map = {}
        self.next_table_id = 1
        self.next_column_id = 1
        self.column_order = {}  # 테이블별 컬럼 순서를 추적하기 위한 딕셔너리

    @classmethod
    def create(cls, config_dict, ctx):
        return cls(config_dict, ctx)

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

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        query = """
        SELECT urn, metadata FROM metadata_aspect_v2
        WHERE aspect_name = 'upstreamLineage' AND version = 0
        """
        results = self.duckdb_conn.execute(query).fetchall()

        for row in results:
            downstream = row[0]
            metadata = eval(row[1])  # Assuming metadata is stored as a string representation of a dict
            self.process_lineage(downstream, metadata)

        self.logger.info(f"Processed {len(results)} lineage records")

        # populate ais0080,ais0081
        populate_ais0080(self.duckdb_conn)
        populate_ais0081(self.duckdb_conn)

        # After processing all lineage records
        self.logger.info("Starting asynchronous transfer to PostgreSQL")
        asyncio.run(self.transfer_to_postgresql())

        return []

    def process_lineage(self, downstream: str, metadata: Dict) -> None:
        upstreams = metadata.get('upstreams', [])
        fine_grained_lineages = metadata.get('fineGrainedLineages', [])

        downstream_table_id = self.get_table_id(downstream)
        upstream_table_ids = {upstream['dataset']: self.get_table_id(upstream['dataset']) for upstream in upstreams}

        downstream_properties = self.get_dataset_properties(downstream)

        for upstream in upstreams:
            upstream_urn = upstream['dataset']
            upstream_properties = self.get_dataset_properties(upstream_urn)
            query_custom_keys = upstream.get('query_custom_keys', {})

            # 기존 ais0102 처리 및 ais0112 추가
            self.process_table_lineage(downstream, upstream_urn, query_custom_keys,
                                       downstream_table_id, upstream_table_ids[upstream_urn],
                                       downstream_properties, upstream_properties)
            self.populate_ais0112(upstream_urn, downstream, query_custom_keys)

        for upstream in upstreams:
            upstream_urn = upstream['dataset']
            upstream_properties = self.get_dataset_properties(upstream_urn)
            query_custom_keys = upstream.get('query_custom_keys', {})

            # 기존 ais0103 처리 및 ais0113 추가
            self.process_column_lineage(downstream, upstream_urn, fine_grained_lineages, query_custom_keys,
                                        downstream_table_id, upstream_table_ids[upstream_urn],
                                        downstream_properties, upstream_properties)
            self.populate_ais0113(upstream_urn, downstream, fine_grained_lineages, query_custom_keys)

    def populate_ais0112(self, upstream_urn: str, downstream_urn: str, query_custom_keys: Dict) -> None:
        upstream_data = self.get_ais0102_data(upstream_urn, query_custom_keys)
        downstream_data = self.get_ais0102_data(downstream_urn, query_custom_keys)

        if not upstream_data or not downstream_data:
            self.logger.warning(f"Missing data for ais0112: upstream={upstream_urn}, downstream={downstream_urn}")
            return

        try:
            self.duckdb_conn.execute("""
                INSERT OR REPLACE INTO ais0112 (
                    prj_id, file_id, sql_id, table_id, obj_id, func_id, owner_name, table_name, caps_table_name, sql_obj_type,
                    unique_owner_name, unique_owner_tgt_srv_id, system_biz_id,
                    call_prj_id, call_file_id, call_sql_id, call_table_id, call_obj_id, call_func_id, call_owner_name,
                    call_table_name, call_caps_table_name, call_sql_obj_type, call_unique_owner_name,
                    call_unique_owner_tgt_srv_id, call_system_biz_id, cond_mapping_bit, data_maker, mapping_kind
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (*upstream_data, *downstream_data, 2, '', ''))  # cond_mapping_bit, data_maker, mapping_kind는 기본값으로 설정
            self.logger.debug(f"Inserted/Updated record in ais0112 for {upstream_urn} -> {downstream_urn}")
        except duckdb.Error as e:
            self.logger.error(f"Error inserting into ais0112: {e}")

    def populate_ais0113(self, upstream_urn: str, downstream_urn: str, fine_grained_lineages: List[Dict],
                         query_custom_keys: Dict) -> None:
        # 컬럼 순서 초기화
        self.column_order = {upstream_urn: 0, downstream_urn: 0}

        if not fine_grained_lineages:
            # 가상 '*' 컬럼 처리
            upstream_data = self.get_ais0103_data(upstream_urn, f"urn:li:schemaField:({upstream_urn},*)",
                                                  query_custom_keys)
            downstream_data = self.get_ais0103_data(downstream_urn, f"urn:li:schemaField:({downstream_urn},*)",
                                                    query_custom_keys)

            if upstream_data and downstream_data:
                self.insert_ais0113_record(upstream_data, downstream_data, '', upstream_urn, downstream_urn)
        else:
            for lineage in fine_grained_lineages:
                upstreams = lineage.get('upstreams', [])
                downstreams = lineage.get('downstreams', [])
                transform_operation = lineage.get('transformOperation', '')

                for upstream_col in upstreams:
                    for downstream_col in downstreams:
                        upstream_data = self.get_ais0103_data(upstream_urn, upstream_col, query_custom_keys)
                        downstream_data = self.get_ais0103_data(downstream_urn, downstream_col, query_custom_keys)

                        if upstream_data and downstream_data:
                            self.insert_ais0113_record(upstream_data, downstream_data, transform_operation,
                                                       upstream_urn, downstream_urn)

    def insert_ais0113_record(self, upstream_data: Tuple, downstream_data: Tuple, transform_operation: str,
                              upstream_urn: str, downstream_urn: str):
        try:
            # 컬럼 순서 가져오기
            col_order_no = self.get_next_column_order(upstream_urn)
            call_col_order_no = self.get_next_column_order(downstream_urn)

            self.duckdb_conn.execute("""
                INSERT OR REPLACE INTO ais0113 (
                    prj_id, file_id, sql_id, table_id, col_id, obj_id, func_id, owner_name, table_name, caps_table_name, 
                    sql_obj_type, col_name, caps_col_name, col_value_yn, col_expr, col_name_org, caps_col_name_org, 
                    unique_owner_name, unique_owner_tgt_srv_id, col_order_no, adj_col_order_no, system_biz_id,
                    call_prj_id, call_file_id, call_sql_id, call_table_id, call_col_id, call_obj_id, call_func_id, call_owner_name, call_table_name, call_caps_table_name,
                    call_sql_obj_type, call_col_name, call_caps_col_name, call_col_value_yn, call_col_expr, call_col_name_org, call_caps_col_name_org, 
                    call_unique_owner_name, call_unique_owner_tgt_srv_id, call_col_order_no, call_adj_col_order_no, call_system_biz_id,
                    cond_mapping, data_maker, mapping_kind
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                *upstream_data[:17],  # prj_id to caps_col_name_org
                upstream_data[17], upstream_data[18],  # unique_owner_name, unique_owner_tgt_srv_id
                col_order_no, col_order_no,  # col_order_no, adj_col_order_no
                upstream_data[20],  # system_biz_id
                *downstream_data[:17],  # call_prj_id to call_caps_col_name_org
                downstream_data[17], downstream_data[18],  # call_unique_owner_name, call_unique_owner_tgt_srv_id
                call_col_order_no, call_col_order_no,  # call_col_order_no, call_adj_col_order_no
                downstream_data[20],  # call_system_biz_id
                1, 'ingest_cli', ''  # cond_mapping, data_maker, mapping_kind
            ))
            self.logger.debug(f"Inserted/Updated record in ais0113 for {upstream_data[11]} -> {downstream_data[11]}")
        except duckdb.Error as e:
            self.logger.error(f"Error inserting into ais0113: {e}")

    def get_next_column_order(self, table_urn: str) -> int:
        order = self.column_order.get(table_urn, 0)
        self.column_order[table_urn] = order + 1
        return order

    def get_ais0102_data(self, table_urn: str, query_custom_keys: Dict) -> Tuple:
        query = """
            SELECT prj_id, file_id, sql_id, table_id, obj_id, func_id, 
                   CASE 
                    WHEN LOWER(table_urn) LIKE '%s3://%' THEN 'fil'
                    ELSE COALESCE(sql_obj_type, '')
                   END AS sql_obj_type,
                   COALESCE(table_urn, '') as table_urn,
                   COALESCE(system_biz_id, '') as system_biz_id,
                   COALESCE(system_tgt_srv_id, '') as system_tgt_srv_id,
                   COALESCE(owner_srv_id, '') as owner_srv_id,
                   COALESCE(system_id, '') as system_id,
                   COALESCE(system_name, '') as system_name,
                   COALESCE(biz_id, '') as biz_id,
                   COALESCE(biz_name, '') as biz_name
            FROM ais0102 
            WHERE table_urn = ? AND prj_id = ? AND file_id = ? AND sql_id = ? AND obj_id = ? AND func_id = ?
        """
        params = [
            table_urn,
            query_custom_keys.get('prj_id', ''),
            int(query_custom_keys.get('file_id', 0)),
            int(query_custom_keys.get('sql_id', 0)),
            int(query_custom_keys.get('obj_id', 0)),
            int(query_custom_keys.get('func_id', 0))
        ]
        result = self.duckdb_conn.execute(query, params).fetchone()

        if result:
            dataset_urn = DatasetUrn.from_string(table_urn)
            table_content = dataset_urn.get_dataset_name()

            owner_name = NameUtil.get_schema(table_content)
            table_name = NameUtil.get_table_name(table_content)
            unique_owner_name = NameUtil.get_unique_owner_name(table_content)
            unique_owner_tgt_srv_id = NameUtil.get_unique_owner_tgt_srv_id(table_content)

            return (
                *result[:6],  # prj_id, file_id, sql_id, table_id, obj_id, func_id
                owner_name.upper(),
                table_name,
                table_name.upper(),  # caps_table_name
                result[6].lower(),  # sql_obj_type
                unique_owner_name,
                unique_owner_tgt_srv_id,
                result[8]  # system_biz_id
            )
        return None

    def get_ais0103_data(self, table_urn: str, column_urn: str, query_custom_keys: Dict) -> Tuple:
        query = """
            SELECT a.prj_id, a.file_id, a.sql_id, a.table_id, a.col_id, 
                   a.obj_id, a.func_id, 
                   CASE 
                        WHEN LOWER(b.table_urn) LIKE '%s3://%' THEN 'fil'
                        ELSE COALESCE(b.sql_obj_type, '')
                   END AS sql_obj_type,
                   COALESCE(a.column_urn, '') as column_urn,
                   COALESCE(a.transform_operation, '') as transform_operation,
                   COALESCE(b.system_biz_id, '') as system_biz_id
            FROM ais0103 a
            JOIN ais0102 b ON a.table_id = b.table_id
            WHERE b.table_urn = ? AND a.column_urn = ? 
                AND a.prj_id = ? AND a.file_id = ? AND a.sql_id = ? AND a.obj_id = ? AND a.func_id = ?
        """
        params = [
            table_urn,
            column_urn,
            query_custom_keys.get('prj_id', ''),
            int(query_custom_keys.get('file_id', 0)),
            int(query_custom_keys.get('sql_id', 0)),
            int(query_custom_keys.get('obj_id', 0)),
            int(query_custom_keys.get('func_id', 0))
        ]
        result = self.duckdb_conn.execute(query, params).fetchone()

        if result:
            dataset_urn = DatasetUrn.from_string(table_urn)
            table_content = dataset_urn.get_dataset_name()

            owner_name = NameUtil.get_schema(table_content)
            table_name = NameUtil.get_table_name(table_content)
            unique_owner_name = NameUtil.get_unique_owner_name(table_content)
            unique_owner_tgt_srv_id = NameUtil.get_unique_owner_tgt_srv_id(table_content)

            schema_field_urn = SchemaFieldUrn.from_string(column_urn)
            col_name = schema_field_urn.field_path if schema_field_urn.field_path != '*' else '*'

            return (
                *result[:7],  # prj_id, file_id, sql_id, table_id, col_id, obj_id, func_id
                owner_name.upper(),
                table_name,
                table_name.upper(),  # caps_table_name
                result[7].lower(),  # sql_obj_type
                col_name,
                col_name.upper() if col_name != '*' else '*',  # caps_col_name
                'N',  # col_value_yn (default to 'Y')
                '',  # col_expr (empty for now)
                col_name,  # col_name_org
                col_name.upper() if col_name != '*' else '*',  # caps_col_name_org
                unique_owner_name,
                unique_owner_tgt_srv_id,
                result[9],  # transform_operation
                result[10]  # system_biz_id
            )
        return None

    def get_dataset_properties(self, dataset_urn: str) -> Dict:
        url = f"{self.config['datahub_api']['server']}/aspects/{DatasetUrn.url_encode(dataset_urn)}?aspect=datasetProperties&version=0"
        try:
            self.logger.info(f"Fetching dataset properties for {dataset_urn}")
            with requests.get(url, timeout=self.config['datahub_api']['timeout_sec']) as response:
                if response.status_code == 200:
                    self.logger.info(f"Successfully fetched dataset properties for {dataset_urn}")
                    return response.json()
                else:
                    self.logger.debug(f"Failed to get dataset properties for {dataset_urn}: HTTP {response.status_code}")
                    if response.status_code == 404:
                        self.logger.warning(f"Dataset not found: {dataset_urn}. Using empty properties.")
                        return {}  # Return empty dict if dataset not found
                    elif response.status_code == 500:
                        self.logger.error(
                            "Server error occurred. This might be due to the dataset not existing or other server-side issues.")
                    return {}
        except requests.Timeout:
            self.logger.error(f"The request timed out of {self.config['datahub_api']['timeout_sec']} sec")
        except requests.RequestException as e:
            self.logger.error(f"Unexpected error when trying to reach {url}: {e}")

    def process_table_lineage(self, downstream: str, upstream_urn: str, query_custom_keys: Dict,
                              downstream_table_id: int, upstream_table_id: int,
                              downstream_properties: Dict, upstream_properties: Dict) -> None:
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
        TEMPORARY_TABLE = '$TB'
        REGULAR_TABLE = 'TBL'
        FILE_TABLE = 'FIL'
        upstream_sql_obj_type = TEMPORARY_TABLE if query_type_props.get('temporary', False) else REGULAR_TABLE
        upstream_sql_obj_type = FILE_TABLE if 's3://' in upstream_urn else upstream_sql_obj_type
        downstream_sql_obj_type = TEMPORARY_TABLE if query_type_props.get('temporary', False) else REGULAR_TABLE
        downstream_sql_obj_type = FILE_TABLE if 's3://' in downstream else downstream_sql_obj_type


        self.insert_ais0102(prj_id, file_id, sql_id, obj_id, func_id, query_type,
                            upstream_urn, upstream_sql_obj_type, upstream_table_id, upstream_properties)
        self.insert_ais0102(prj_id, file_id, sql_id, obj_id, func_id, query_type,
                            downstream, downstream_sql_obj_type, downstream_table_id, downstream_properties)

    def insert_ais0102(self, prj_id: str, file_id: int, sql_id: int, obj_id: int, func_id: int,
                       query_type: str, table_urn: str, sql_obj_type: str, table_id: int, properties: Dict) -> None:
        system_biz_id = get_system_biz_id(properties)
        system_tgt_srv_id = get_system_tgt_srv_id(properties)
        owner_srv_id = get_owner_srv_id(properties)
        system_id = get_system_id(properties)
        system_name = get_system_name(properties)
        biz_id = get_biz_id(properties)
        biz_name = get_biz_name(properties)

        try:
            self.duckdb_conn.execute("""
                    INSERT OR REPLACE INTO ais0102 
                    (prj_id, file_id, sql_id, table_id, obj_id, func_id, query_type, sql_obj_type, table_urn, system_biz_id,system_tgt_srv_id,owner_srv_id,system_id,system_name,biz_id,biz_name )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?)
                """, (
                prj_id, file_id, sql_id, table_id, obj_id, func_id, query_type, sql_obj_type, table_urn, system_biz_id,
                system_tgt_srv_id, owner_srv_id, system_id, system_name, biz_id, biz_name))
            self.logger.debug(f"Inserted/Updated record in ais0102 for table_urn: {table_urn}")
        except duckdb.Error as e:
            self.logger.error(f"Error inserting into ais0102: {e}")

    def process_column_lineage(self, downstream: str, upstream_urn: str, fine_grained_lineages: List[Dict],
                               query_custom_keys: Dict, downstream_table_id: int, upstream_table_id: int,
                               downstream_properties: Dict, upstream_properties: Dict) -> None:
        if not fine_grained_lineages:
            self.create_virtual_column_lineage(downstream, upstream_urn, query_custom_keys,
                                               downstream_table_id, upstream_table_id,
                                               downstream_properties, upstream_properties)
        else:
            for lineage in fine_grained_lineages:
                upstreams = lineage.get('upstreams', [])
                downstreams = lineage.get('downstreams', [])
                transform_operation = lineage.get('transformOperation', '')

                if len(downstreams) == 1 and len(upstreams) >= 1:
                    downstream_col = downstreams[0]
                    for upstream_col in upstreams:
                        self.insert_ais0103(upstream_col, downstream_col, query_custom_keys,
                                            transform_operation, upstream_table_id, downstream_table_id,
                                            upstream_properties, downstream_properties)

                elif len(upstreams) == 1:
                    upstream_col = upstreams[0]
                    for downstream_col in downstreams:
                        self.insert_ais0103(upstream_col, downstream_col, query_custom_keys,
                                            transform_operation, upstream_table_id, downstream_table_id,
                                            upstream_properties, downstream_properties)

                else:
                    for upstream_col in upstreams:
                        for downstream_col in downstreams:
                            self.insert_ais0103(upstream_col, downstream_col, query_custom_keys,
                                                transform_operation, upstream_table_id, downstream_table_id,
                                                upstream_properties, downstream_properties)

    def insert_ais0103(self, upstream_col: str, downstream_col: str, query_custom_keys: Dict,
                       transform_operation: str, upstream_table_id: int, downstream_table_id: int,
                       upstream_properties: Dict, downstream_properties: Dict) -> None:
        upstream_col_name = upstream_col.split(',')[-1].strip(')').split('(')[-1]
        downstream_col_name = downstream_col.split(',')[-1].strip(')').split('(')[-1]

        upstream_col_id = self.get_column_id(upstream_table_id, upstream_col_name)
        downstream_col_id = self.get_column_id(downstream_table_id, downstream_col_name)

        prj_id = query_custom_keys.get('prj_id', '')
        file_id = int(query_custom_keys.get('file_id', 0))
        sql_id = int(query_custom_keys.get('sql_id', 0))

        try:
            self.duckdb_conn.execute("""
                    INSERT OR REPLACE INTO ais0103 
                    (prj_id, file_id, sql_id, table_id, col_id, obj_id, func_id, column_urn)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (prj_id, file_id, sql_id, upstream_table_id, upstream_col_id, 0, 0, upstream_col))

            self.duckdb_conn.execute("""
                    INSERT OR REPLACE INTO ais0103 
                    (prj_id, file_id, sql_id, table_id, col_id, obj_id, func_id, column_urn, transform_operation)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                prj_id, file_id, sql_id, downstream_table_id, downstream_col_id, 0, 0, downstream_col,
                transform_operation))

            self.logger.debug(f"Inserted/Updated records in ais0103 for columns: {upstream_col} -> {downstream_col}")
        except duckdb.Error as e:
            self.logger.error(f"Error inserting into ais0103: {e}")

    def create_virtual_column_lineage(self, downstream: str, upstream_urn: str, query_custom_keys: Dict,
                                      downstream_table_id: int, upstream_table_id: int,
                                      downstream_properties: Dict, upstream_properties: Dict) -> None:
        prj_id = query_custom_keys.get('prj_id', '')
        file_id = int(query_custom_keys.get('file_id', 0))
        sql_id = int(query_custom_keys.get('sql_id', 0))

        upstream_virtual_col = f"urn:li:schemaField:({upstream_urn},*)"
        downstream_virtual_col = f"urn:li:schemaField:({downstream},*)"

        upstream_virtual_col_id = self.get_column_id(upstream_table_id, "*")
        downstream_virtual_col_id = self.get_column_id(downstream_table_id, "*")

        try:
            self.duckdb_conn.execute("""
                    INSERT OR REPLACE INTO ais0103 
                    (prj_id, file_id, sql_id, table_id, col_id, obj_id, func_id, column_urn)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (prj_id, file_id, sql_id, upstream_table_id, upstream_virtual_col_id, 0, 0, upstream_virtual_col))

            self.duckdb_conn.execute("""
                    INSERT OR REPLACE INTO ais0103 
                    (prj_id, file_id, sql_id, table_id, col_id, obj_id, func_id, column_urn)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                prj_id, file_id, sql_id, downstream_table_id, downstream_virtual_col_id, 0, 0, downstream_virtual_col))

            self.logger.debug(f"Created virtual column lineage: {upstream_virtual_col} -> {downstream_virtual_col}")
        except duckdb.Error as e:
            self.logger.error(f"Error creating virtual column lineage: {e}")

    async def transfer_to_postgresql(self):
        self.logger.info("Starting asynchronous batch transfer to PostgreSQL")

        try:

            # Transfer ais0102
            await self.transfer_ais0102()

            # Transfer ais0112
            await self.transfer_table('ais0112')

            # Transfer ais0113
            await self.transfer_table('ais0113')

            # Delete existing records from ais0080 and ais0081
            await self.delete_existing_records()

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
                    prj_id,                     # prj_id
                    float(file_id),             # file_id
                    float(sql_id),              # sql_id
                    float(table_id),            # table_id
                    float(obj_id),              # obj_id
                    float(func_id),             # func_id
                    table_name,                 # table_name
                    caps_table_name,            # caps_table_name
                    owner_name,                 # owner_name
                    pg_query_type,              # query_type
                    None,                       # query_line_no
                    sql_obj_type,               # sql_obj_type
                    None,                       # inlineview_yn
                    None,                       # dblink_name
                    None,                       # table_alias_name
                    None,                       # inlineview_src
                    sql_state,                  # sql_state
                    None,                       # column_no
                    None,                       # table_depth
                    None,                       # table_order_no
                    None,                       # rel_table_id
                    None,                       # rel_flow_id
                    None,                       # dbc_mapping_yn
                    None,                       # teradata_sql_id
                    unique_owner_name,          # unique_owner_name
                    unique_owner_tgt_srv_id,    # unique_owner_tgt_srv_id
                    None,                       # sql_name
                    None,                       # system_biz_id
                    None                        # fl_tbl_uid
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
        self.logger.info("Deleting existing records from ais0080 and ais0081")

        delete_queries = [
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
                    self.logger.info(f"Inserted {len(batch)} records into {table_name}")
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
                    self.logger.info(f"Inserted {len(batch)} records into {table_name}")
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
