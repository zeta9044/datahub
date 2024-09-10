import json
import duckdb
import psycopg2
from psycopg2 import pool
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import DatasetPropertiesClass
import requests
import logging
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Iterable
import sys

from zeta_lab.utilities.tool import NameUtil, get_owner_srv_id, get_system_biz_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConvertQtrackSource(Source):
    def __init__(self, config: dict, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        self.duckdb_conn = duckdb.connect(self.config["duckdb_path"])
        self.pg_pool = self.get_postgres_pool()
        self.batch_size = self.config.get("batch_size", 1000)
        self.max_workers = self.config.get("max_workers", 5)
        self.session = None
        self.initialize_databases()

    @classmethod
    def create(cls, config_dict, ctx):
        return cls(config_dict, ctx)

    def initialize_databases(self):
        self.create_table_if_not_exists_duckdb()
        self.check_postgres_table_exists()

    def create_table_if_not_exists_duckdb(self):
        self.duckdb_conn.execute("""
        CREATE TABLE IF NOT EXISTS ais0112 (
            prj_id VARCHAR(5),
            file_id BIGINT,
            sql_id BIGINT,
            table_id BIGINT,
            call_prj_id VARCHAR(5),
            call_file_id BIGINT,
            call_sql_id BIGINT,
            call_table_id BIGINT,
            obj_id BIGINT,
            func_id BIGINT,
            owner_name VARCHAR(80),
            table_name VARCHAR(1000),
            caps_table_name VARCHAR(1000),
            sql_obj_type VARCHAR(3),
            call_obj_id BIGINT,
            call_func_id BIGINT,
            call_owner_name VARCHAR(80),
            call_table_name VARCHAR(1000),
            call_caps_table_name VARCHAR(1000),
            call_sql_obj_type VARCHAR(3),
            unique_owner_name VARCHAR(80),
            call_unique_owner_name VARCHAR(80),
            unique_owner_tgt_srv_id VARCHAR(100),
            call_unique_owner_tgt_srv_id VARCHAR(100),
            cond_mapping_bit BIGINT,
            data_maker BIGINT,
            mapping_kind VARCHAR(10),
            system_biz_id VARCHAR(80),
            call_system_biz_id VARCHAR(80)
        )
        """)

    def check_postgres_table_exists(self):
        pg_config = self.config['target_config']
        with self.pg_pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE  table_schema = '{pg_config['username']}'
                    AND    table_name   = 'ais0112'
                );
                """)
                exists = cur.fetchone()[0]
                if not exists:
                    logger.error("Error: The table 'ais0112' does not exist in the PostgreSQL database.")
                    self.close()
                    sys.exit(1)
            self.pg_pool.putconn(conn)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.process_lineage_data())
        return []  # 여기서는 실제 WorkUnit을 반환하지 않습니다.

    async def process_lineage_data(self):
        # SSL 검증을 비활성화하고 HTTP를 사용하는 ClientSession 생성
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as self.session:
            query = """
            SELECT urn, metadata FROM metadata_aspect_v2
            WHERE aspect_name = 'upstreamLineage' AND version = 0
            """
            results = self.duckdb_conn.execute(query).fetchall()

            tasks = []
            for i in range(0, len(results), self.batch_size):
                batch = results[i:i + self.batch_size]
                task = asyncio.create_task(self.process_batch(batch))
                tasks.append(task)

            await asyncio.gather(*tasks)

            await self.transfer_to_postgres()

    async def process_batch(self, batch: List[tuple]):
        lineage_data = []
        for row in batch:
            downstream = row[0]
            metadata = json.loads(row[1])
            data = await self.process_lineage(downstream, metadata)
            lineage_data.extend(data)

        if lineage_data:
            self.insert_to_duckdb(lineage_data)

    async def process_lineage(self, downstream: str, metadata: Dict) -> List[Dict]:
        downstream_urn = DatasetUrn.from_string(downstream)
        downstream_props = await self.get_dataset_properties(downstream)

        lineage_data = []
        for upstream in metadata.get("upstreams", []):
            upstream_urn = DatasetUrn.from_string(upstream["dataset"])
            upstream_props = await self.get_dataset_properties(upstream["dataset"])

            prj_id = upstream['query_custom_keys'].get("prj_id", "")
            file_id = upstream['query_custom_keys'].get("file_id", 0)
            sql_id = upstream['query_custom_keys'].get("sql_id", 0)

            lineage_data.append({
                "prj_id": prj_id,
                "file_id": file_id,
                "sql_id": sql_id,
                "table_id": 0,
                "obj_id": 0,
                "func_id": 0,
                "owner_name": NameUtil.get_schema(upstream_urn.get_dataset_name()),
                "table_name": NameUtil.get_table_name(upstream_urn.get_dataset_name()),
                "caps_table_name": NameUtil.get_table_name(upstream_urn.get_dataset_name()).upper(),
                "sql_obj_type": "",
                "unique_owner_name": NameUtil.get_unique_owner_name(upstream_urn.get_dataset_name()),
                "unique_owner_tgt_srv_id": NameUtil.get_unique_owner_tgt_srv_id(upstream_urn.get_dataset_name()),
                "system_biz_id": get_system_biz_id(upstream_props),
                "call_prj_id": prj_id,
                "call_file_id": file_id,
                "call_sql_id": sql_id,
                "call_table_id": 0,
                "call_obj_id": 0,
                "call_func_id": 0,
                "call_owner_name": NameUtil.get_schema(downstream_urn.get_dataset_name()),
                "call_table_name": NameUtil.get_table_name(downstream_urn.get_dataset_name()),
                "call_caps_table_name": NameUtil.get_table_name(downstream_urn.get_dataset_name()).upper(),
                "call_sql_obj_type": "",
                "call_unique_owner_name": NameUtil.get_unique_owner_name(downstream_urn.get_dataset_name()),
                "call_unique_owner_tgt_srv_id": NameUtil.get_unique_owner_tgt_srv_id(downstream_urn.get_dataset_name()),
                "call_system_biz_id": get_system_biz_id(downstream_props),
                "cond_mapping_bit": 0,
                "data_maker": 0,
                "mapping_kind": ""
            })

        return lineage_data

    def insert_to_duckdb(self, data: List[Dict]):
        query = """
        INSERT INTO ais0112 (
            prj_id, file_id, sql_id, table_id, obj_id, func_id, owner_name, table_name, caps_table_name, sql_obj_type,
            unique_owner_name, unique_owner_tgt_srv_id, system_biz_id,
            call_prj_id, call_file_id, call_sql_id, call_table_id, call_obj_id, call_func_id, call_owner_name,
            call_table_name, call_caps_table_name, call_sql_obj_type, call_unique_owner_name,
            call_unique_owner_tgt_srv_id, call_system_biz_id, cond_mapping_bit, data_maker, mapping_kind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.duckdb_conn.executemany(query, [tuple(item.values()) for item in data])

    async def transfer_to_postgres(self):
        duckdb_data = self.duckdb_conn.execute("SELECT * FROM ais0112").fetchall()

        conn = self.pg_pool.getconn()
        try:
            with conn.cursor() as cur:
                for i in range(0, len(duckdb_data), self.batch_size):
                    batch = duckdb_data[i:i + self.batch_size]
                    cur.executemany("""
                        INSERT INTO ais0112 (
                            prj_id, file_id, sql_id, table_id, obj_id, func_id, owner_name, table_name, caps_table_name, 
                            sql_obj_type, unique_owner_name, unique_owner_tgt_srv_id, system_biz_id,
                            call_prj_id, call_file_id, call_sql_id, call_table_id, call_obj_id, call_func_id, call_owner_name,
                            call_table_name, call_caps_table_name, call_sql_obj_type, call_unique_owner_name,
                            call_unique_owner_tgt_srv_id, call_system_biz_id, cond_mapping_bit, data_maker, mapping_kind
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, batch)
                conn.commit()
        finally:
            self.pg_pool.putconn(conn)

    async def get_dataset_properties(self, dataset_urn: str) -> Dict:
        url = f"{self.config['datahub_api']['server']}/aspects/{DatasetUrn.url_encode(dataset_urn)}?aspect=datasetProperties&version=0"
        max_retries = 3
        retry_delay = 1  # seconds

        for attempt in range(max_retries):
            try:
                async with self.session.get(url, timeout=self.config['datahub_api']['timeout_sec']) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.error(f"Failed to get dataset properties for {dataset_urn}: HTTP {response.status}")
                        if response.status == 500:
                            logger.error("Server error occurred. This might be due to the dataset not existing or other server-side issues.")
                        return {}
            except aiohttp.ClientConnectorError as e:
                logger.error(f"Connection error when trying to reach {url}: {e}")
            except aiohttp.ClientError as e:
                logger.error(f"Client error when trying to reach {url}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error when trying to reach {url}: {e}")

            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"Max retries reached. Unable to get dataset properties for {dataset_urn}")
                return {}

    def get_postgres_pool(self):
        pg_config = self.config['target_config']
        return psycopg2.pool.SimpleConnectionPool(
            1, 20,
            host=pg_config['host_port'].split(':')[0],
            port=pg_config['host_port'].split(':')[1],
            database=pg_config['database'],
            user=pg_config['username'],
            password=pg_config['password']
        )

    def get_report(self):
        return self.report

    def close(self):
        self.duckdb_conn.close()
        self.pg_pool.closeall()


# Add this to the source_registry
from datahub.ingestion.source.source_registry import source_registry

source_registry.register("convert_qtrack", ConvertQtrackSource)
logger.info("Registered convert_qtrack source")
