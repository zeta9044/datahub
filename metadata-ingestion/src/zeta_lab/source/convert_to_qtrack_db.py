import time
from typing import List, Dict, Iterable

import duckdb
import psycopg2
import psycopg2.extras
import requests
import logging
from psycopg2 import pool

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata._urns.urn_defs import SchemaFieldUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn
from zeta_lab.batch.ais0102_batch import AIS0102BatchProcessor
from zeta_lab.batch.ais0113_batch import AIS0113BatchProcessor
from zeta_lab.batch.batch_processor import BatchConfig
from zeta_lab.batch.postgres_transfer import PartitionedTransferManager
from zeta_lab.batch.table_populator import TablePopulatorConfig, OptimizedTablePopulator
from zeta_lab.utilities.qtrack_db import create_duckdb_tables, check_postgres_tables_exist
from zeta_lab.utilities.tool import create_default_dataset_properties

# Set up logging
logging.basicConfig(
    level=logging.INFO,  # 로그 레벨 설정 (DEBUG, INFO, WARNING, ERROR, CRITICAL 중 선택)
    format="%(asctime)s - %(levelname)s - %(message)s",  # 포맷 설정
)
logger = logging.getLogger(__name__)

class ConvertQtrackSource(Source):
    def __init__(self, config: dict, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        self.system_biz_id = self.config['system_biz_id']
        self.duckdb_conn = duckdb.connect(self.config["duckdb_path"])
        logger.info(f"Connected to DuckDB at {self.config['duckdb_path']}")
        self.pg_pool = self.get_postgres_pool()
        logger.info("Initialized PostgreSQL connection pool")
        self.batch_size = self.config.get("batch_size", 100)
        self.max_workers = self.config.get("max_workers", 5)
        logger.info(f"Batch size: {self.batch_size}, Max workers: {self.max_workers}")
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
        logger.info("Initializing databases")
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
            logger.error(f"Error extracting column name from URN {column_urn}: {e}")
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
                logger.error(f"Error fetching properties for {urn}: {e}")
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
        start_time = time.time()
        self.prefetch_dataset_properties(results)
        duration = time.time() - start_time
        minutes, seconds = divmod(duration, 60)
        logger.info(f"Completed pre-patch of {len(results)} downstream_urn from gms server for {minutes} min {seconds:.2f} sec.")

        start_time = time.time()
        for row in results:
            downstream_urn = row[0]
            metadata = eval(row[1])  # Assuming metadata is stored as a string representation of a dict
            self.process_lineage(downstream_urn, metadata)

        duration = time.time() - start_time
        minutes, seconds = divmod(duration, 60)
        logger.info(f"Processed {len(results)} lineage records for {minutes} min {seconds:.2f} sec.")

        # Populate derived tables using table populator
        config = TablePopulatorConfig(
            threads=4,
            logger=logger
        )
        populator = OptimizedTablePopulator(self.duckdb_conn, config)
        populator.populate_tables()

        # Transfer data to PostgreSQL
        logger.info("Starting transfer to PostgreSQL")
        self.transfer_to_postgresql()

        return []

    def _prepare_lineage_data(self, downstream_urn: str, metadata: Dict) -> Dict:
        """
        리니지 처리를 위한 데이터 준비

        Args:
            downstream_urn: 다운스트림 URN
            metadata: 리니지 메타데이터

        Returns:
            Dict containing prepared data for processing
        """
        # 기본 구조 초기화
        downstream_info = {
            'urn': downstream_urn,
            'properties': self.get_dataset_properties(downstream_urn) or create_default_dataset_properties(self.system_biz_id[0], downstream_urn),
            'table_id': self.get_table_id(downstream_urn)
        }

        prepared_data = {
            'downstream': downstream_info,
            'upstreams': [],
            'column_mappings': []
        }

        # 업스트림 데이터 준비
        for upstream in metadata.get('upstreams', []):
            upstream_urn = upstream['dataset']
            upstream_table_id = self.get_table_id(upstream_urn)
            upstream_data = {
                'urn': upstream_urn,
                'properties': self.get_dataset_properties(upstream_urn) or create_default_dataset_properties(self.system_biz_id[0], upstream_urn),
                'table_id': upstream_table_id,
                'query_custom_keys': upstream.get('query_custom_keys', {})
            }
            prepared_data['upstreams'].append(upstream_data)

            # 가상 매핑이 필요할 경우를 대비해 업스트림 정보 저장
            if not metadata.get('fineGrainedLineages'):
                mapping = {
                    'upstream': upstream_data,
                    'upstream_col': {
                        'name': '*',
                        'table_id': upstream_table_id,
                        'col_id': self.get_column_id(upstream_table_id, '*')
                    },
                    'downstream_col': {
                        'name': '*',
                        'table_id': downstream_info['table_id'],
                        'col_id': self.get_column_id(downstream_info['table_id'], '*')
                    },
                    'transform_operation': '',
                    'col_order_no': self.get_next_column_order(upstream_urn),
                    'call_col_order_no': self.get_next_column_order(downstream_urn)
                }
                prepared_data['column_mappings'].append(mapping)

        # 컬럼 레벨 리니지 처리
        fine_grained_lineages = metadata.get('fineGrainedLineages', [])
        if fine_grained_lineages:
            prepared_data['column_mappings'] = []  # 기존 가상 매핑 제거

            for lineage in fine_grained_lineages:
                upstream_cols = lineage.get('upstreams', [])
                downstream_cols = lineage.get('downstreams', [])
                transform_operation = lineage.get('transformOperation', '')

                for upstream_col_urn in upstream_cols:
                    for downstream_col_urn in downstream_cols:
                        # Extract column names from URNs
                        upstream_col_name = self.extract_column_name(upstream_col_urn)
                        downstream_col_name = self.extract_column_name(downstream_col_urn)

                        # Extract table URNs from column URNs
                        upstream_table_urn = upstream_col_urn[
                                             upstream_col_urn.find("(") + 1:upstream_col_urn.rfind(",")]

                        # Find matching upstream data
                        upstream_data = next(
                            (up for up in prepared_data['upstreams'] if up['urn'] == upstream_table_urn),
                            None
                        )

                        if upstream_data:
                            mapping = {
                                'upstream': upstream_data,
                                'upstream_col': {
                                    'name': upstream_col_name,
                                    'table_id': upstream_data['table_id'],
                                    'col_id': self.get_column_id(upstream_data['table_id'], upstream_col_name)
                                },
                                'downstream_col': {
                                    'name': downstream_col_name,
                                    'table_id': downstream_info['table_id'],
                                    'col_id': self.get_column_id(downstream_info['table_id'], downstream_col_name)
                                },
                                'transform_operation': transform_operation,
                                'col_order_no': self.get_next_column_order(upstream_table_urn),
                                'call_col_order_no': self.get_next_column_order(downstream_urn)
                            }
                            prepared_data['column_mappings'].append(mapping)

        return prepared_data

    def process_lineage(self, downstream_urn: str, metadata: Dict) -> None:
        """
        리니지 정보를 처리하고 배치 방식으로 데이터베이스에 저장

        Args:
            downstream_urn: 다운스트림 URN
            metadata: 리니지 메타데이터
        """
        # 배치 프로세서 초기화
        config = BatchConfig(
            chunk_size=5000,
            batch_size=10000,
            logger=logger
        )

        ais0102_processor = AIS0102BatchProcessor(self.duckdb_conn, config)
        ais0113_processor = AIS0113BatchProcessor(self.duckdb_conn, config)

        try:
            # 데이터 준비
            prepared_data = self._prepare_lineage_data(downstream_urn, metadata)

            # AIS0102 처리 (테이블 레벨 리니지)
            for upstream in prepared_data['upstreams']:
                ais0102_processor.add_item({
                    'urn': upstream['urn'],
                    'table_id': upstream['table_id'],
                    'query_custom_keys': upstream['query_custom_keys'],
                    'properties': upstream['properties']
                })

            # 다운스트림 테이블도 AIS0102에 추가
            ais0102_processor.add_item({
                'urn': prepared_data['downstream']['urn'],
                'table_id': prepared_data['downstream']['table_id'],
                'query_custom_keys': metadata.get('upstreams', [{}])[0].get('query_custom_keys', {}),
                'properties': prepared_data['downstream']['properties']
            })

            # AIS0113 처리 (컬럼 레벨 리니지)
            for mapping in prepared_data['column_mappings']:
                ais0113_processor.add_item({
                    'upstream': mapping['upstream'],
                    'downstream': prepared_data['downstream'],
                    'upstream_col': mapping['upstream_col'],
                    'downstream_col': mapping['downstream_col'],
                    'transform_operation': mapping.get('transform_operation', ''),
                    'col_order_no': mapping.get('col_order_no', 0),
                    'call_col_order_no': mapping.get('call_col_order_no', 0)
                })

            # 남은 배치 처리
            ais0102_processor.finalize()
            ais0113_processor.finalize()

            # 최종 처리 결과 로깅
            logger.debug(f"Successfully processed lineage for {downstream_urn}")
            logger.debug(f"Processed {len(prepared_data['upstreams'])} upstream tables")
            logger.debug(f"Processed {len(prepared_data['column_mappings'])} column mappings")

        except Exception as e:
            logger.error(f"Error processing lineage for {downstream_urn}: {e}")
            raise

    def transfer_to_postgresql(self):
        """DuckDB에서 PostgreSQL로 데이터 전송"""
        logger.info("Starting transfer to PostgreSQL")

        try:
            # Initialize transfer manager
            transfer_config = {
                'duckdb_conn': self.duckdb_conn,
                'pg_pool': self.pg_pool,
                'logger': logging,
                'max_workers': self.max_workers,
                'batch_size': 10000,  # 한 번에 읽어올 레코드 수
                'pg_batch_count': 5  # PostgreSQL에 한 번에 보낼 배치 수
            }

            transfer_manager = PartitionedTransferManager(transfer_config)

            # Process tables in order
            table_process_order = ['ais0102', 'ais0103', 'ais0112', 'ais0113', 'ais0080', 'ais0081']

            for table_name in table_process_order:
                logger.info(f"Starting transfer for {table_name}")
                transfer_manager.transfer_table(table_name)
                logger.info(f"Completed transfer for {table_name}")

        except Exception as e:
            logger.error(f"Error during transfer to PostgreSQL: {e}")
            raise

        logger.info("Completed all transfers to PostgreSQL")

    def get_postgres_pool(self):
        logger.info("Creating PostgreSQL connection pool")
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
            logger.info("Successfully created PostgreSQL connection pool")
            return pool
        except Exception as e:
            logger.error(f"Failed to create PostgreSQL connection pool: {e}")
            raise

    def get_report(self):
        logger.info("Generating report")
        return self.report

    def close(self):
        logger.info("Closing connections")
        try:
            self.duckdb_conn.close()
            logger.info("Closed DuckDB connection")
        except Exception as e:
            logger.error(f"Error closing DuckDB connection: {e}")

        try:
            self.pg_pool.closeall()
            logger.info("Closed all PostgreSQL connections")
        except Exception as e:
            logger.error(f"Error closing PostgreSQL connections: {e}")