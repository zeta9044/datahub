import json
from decimal import DecimalException, Decimal, InvalidOperation

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
from typing import List, Dict, Iterable, Any
import sys

from zeta_lab.utilities.tool import NameUtil, get_owner_srv_id, get_system_biz_id

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ConvertQtrackSource(Source):
    def __init__(self, config: dict, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        logger.info("Initializing ConvertQtrackSource")
        self.duckdb_conn = duckdb.connect(self.config["duckdb_path"])
        logger.info(f"Connected to DuckDB at {self.config['duckdb_path']}")
        self.pg_pool = self.get_postgres_pool()
        logger.info("Initialized PostgreSQL connection pool")
        self.batch_size = self.config.get("batch_size", 1000)
        self.max_workers = self.config.get("max_workers", 5)
        logger.info(f"Batch size: {self.batch_size}, Max workers: {self.max_workers}")
        self.session = None
        self.initialize_databases()

    @classmethod
    def create(cls, config_dict, ctx):
        logger.info("Creating ConvertQtrackSource instance")
        return cls(config_dict, ctx)

    def initialize_databases(self):
        logger.info("Initializing databases")
        self.create_table_if_not_exists_duckdb()
        self.check_postgres_table_exists()

    def create_table_if_not_exists_duckdb(self):
        logger.info("Creating table 'ais0112' in DuckDB if it doesn't exist")
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
            call_system_biz_id VARCHAR(80),
            primary key(prj_id, file_id, sql_id, table_id, call_prj_id, call_file_id, call_sql_id, call_table_id)
        )
        """)
        logger.info("Table 'ais0112' created or already exists in DuckDB")

    def check_postgres_table_exists(self):
        logger.info("Checking if 'ais0112' table exists in PostgreSQL")
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
                else:
                    logger.info("Table 'ais0112' exists in PostgreSQL")
            self.pg_pool.putconn(conn)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        logger.info("Starting get_workunits process")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.process_lineage_data())
        logger.info("Finished get_workunits process")
        return []

    async def process_lineage_data(self):
        logger.info("Processing lineage data")
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as self.session:
            query = """
            SELECT urn, metadata FROM metadata_aspect_v2
            WHERE aspect_name = 'upstreamLineage' AND version = 0
            """
            logger.info("Executing DuckDB query to fetch lineage data")
            results = self.duckdb_conn.execute(query).fetchall()
            logger.info(f"Fetched {len(results)} rows of lineage data")

            tasks = []
            for i in range(0, len(results), self.batch_size):
                batch = results[i:i + self.batch_size]
                task = asyncio.create_task(self.process_batch(batch))
                tasks.append(task)

            logger.info(f"Created {len(tasks)} tasks for processing batches")
            await asyncio.gather(*tasks)

            logger.info("Starting transfer to PostgreSQL")
            await self.transfer_to_postgres()
            logger.info("Finished transfer to PostgreSQL")

    async def process_batch(self, batch: List[tuple]):
        logger.info(f"Processing batch of {len(batch)} items")
        lineage_data = []
        for row in batch:
            downstream = row[0]
            metadata = json.loads(row[1])
            data = await self.process_lineage(downstream, metadata)
            lineage_data.extend(data)

        if lineage_data:
            logger.info(f"Inserting {len(lineage_data)} items into DuckDB")
            self.insert_to_duckdb(lineage_data)
        else:
            logger.warning("No lineage data to insert")

    async def process_lineage(self, downstream: str, metadata: Dict) -> List[Dict]:
        logger.info(f"Processing lineage for downstream: {downstream}")
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
                "owner_name": NameUtil.get_schema(upstream_urn.get_dataset_name()).upper(),
                "table_name": NameUtil.get_table_name(upstream_urn.get_dataset_name()),
                "caps_table_name": NameUtil.get_table_name(upstream_urn.get_dataset_name()).upper(),
                "sql_obj_type": "",
                "unique_owner_name": NameUtil.get_unique_owner_name(upstream_urn.get_dataset_name()).upper(),
                "unique_owner_tgt_srv_id": NameUtil.get_unique_owner_tgt_srv_id(upstream_urn.get_dataset_name()).upper(),
                "system_biz_id": get_system_biz_id(upstream_props),
                "call_prj_id": prj_id,
                "call_file_id": file_id,
                "call_sql_id": sql_id,
                "call_table_id": 0,
                "call_obj_id": 0,
                "call_func_id": 0,
                "call_owner_name": NameUtil.get_schema(downstream_urn.get_dataset_name()).upper(),
                "call_table_name": NameUtil.get_table_name(downstream_urn.get_dataset_name()).upper(),
                "call_caps_table_name": NameUtil.get_table_name(downstream_urn.get_dataset_name()).upper(),
                "call_sql_obj_type": "",
                "call_unique_owner_name": NameUtil.get_unique_owner_name(downstream_urn.get_dataset_name()).upper(),
                "call_unique_owner_tgt_srv_id": NameUtil.get_unique_owner_tgt_srv_id(downstream_urn.get_dataset_name()).upper(),
                "call_system_biz_id": get_system_biz_id(downstream_props),
                "cond_mapping_bit": 0,
                "data_maker": 0,
                "mapping_kind": ""
            })

        logger.info(f"Processed {len(lineage_data)} lineage relationships")
        return lineage_data

    def insert_to_duckdb(self, data: List[Dict]):
        logger.info(f"Inserting {len(data)} rows into DuckDB")

        insert_query = f"""
        INSERT OR IGNORE INTO ais0112 (
            prj_id, file_id, sql_id, table_id, obj_id, func_id, owner_name, table_name, caps_table_name, sql_obj_type,
            unique_owner_name, unique_owner_tgt_srv_id, system_biz_id,
            call_prj_id, call_file_id, call_sql_id, call_table_id, call_obj_id, call_func_id, call_owner_name,
            call_table_name, call_caps_table_name, call_sql_obj_type, call_unique_owner_name,
            call_unique_owner_tgt_srv_id, call_system_biz_id, cond_mapping_bit, data_maker, mapping_kind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.duckdb_conn.executemany(insert_query, [tuple(item.values()) for item in data])
        cursor = self.duckdb_conn.execute("SELECT COUNT(*) FROM ais0112")
        inserted_rows = cursor.fetchone()[0]
        logger.info(f"Inserted {inserted_rows} rows into DuckDB")

    def validate_and_convert_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        schema = {
            'prj_id': ('varchar', 5),
            'file_id': ('numeric', 22),
            'sql_id': ('numeric', 22),
            'table_id': ('numeric', 22),
            'call_prj_id': ('varchar', 5),
            'call_file_id': ('numeric', 22),
            'call_sql_id': ('numeric', 22),
            'call_table_id': ('numeric', 22),
            'obj_id': ('numeric', 22),
            'func_id': ('numeric', 22),
            'owner_name': ('varchar', 80),
            'table_name': ('varchar', 1000),
            'caps_table_name': ('varchar', 1000),
            'sql_obj_type': ('varchar', 3),
            'call_obj_id': ('numeric', 22),
            'call_func_id': ('numeric', 22),
            'call_owner_name': ('varchar', 80),
            'call_table_name': ('varchar', 1000),
            'call_caps_table_name': ('varchar', 1000),
            'call_sql_obj_type': ('varchar', 3),
            'unique_owner_name': ('varchar', 80),
            'call_unique_owner_name': ('varchar', 80),
            'unique_owner_tgt_srv_id': ('varchar', 100),
            'call_unique_owner_tgt_srv_id': ('varchar', 100),
            'cond_mapping_bit': ('numeric', 22),
            'data_maker': ('numeric', 22),
            'mapping_kind': ('varchar', 10),
            'system_biz_id': ('varchar', 80),
            'call_system_biz_id': ('varchar', 80)
        }

        validated_data = {}
        for key, (data_type, max_length) in schema.items():
            value = data.get(key)
            logger.debug(f"Processing field {key}: original value = {value}")

            if value is None:
                validated_data[key] = None
            elif data_type == 'numeric':
                try:
                    # numeric 타입의 경우 Decimal로 변환
                    validated_data[key] = Decimal(str(value))
                    logger.debug(f"Converted {key} to Decimal: {validated_data[key]}")
                except (ValueError, TypeError, InvalidOperation):
                    logger.warning(f"Invalid numeric value for {key}: {value}. Keeping original value.")
                    validated_data[key] = value
            elif data_type == 'varchar':
                # varchar 타입의 경우 문자열로 변환하고 길이 제한
                validated_data[key] = str(value)[:max_length] if value is not None else None
                logger.debug(f"Converted {key} to varchar: {validated_data[key]}")
            else:
                validated_data[key] = value

        logger.debug(f"Validated data: {validated_data}")
        return validated_data

    async def transfer_to_postgres(self):
        logger.info("Starting transfer from DuckDB to PostgreSQL")
        duckdb_data = self.duckdb_conn.execute("SELECT * FROM ais0112").fetchall()
        logger.info(f"Fetched {len(duckdb_data)} rows from DuckDB")

        conn = self.pg_pool.getconn()
        try:
            with conn.cursor() as cur:
                for i in range(0, len(duckdb_data), self.batch_size):
                    batch = duckdb_data[i:i + self.batch_size]
                    column_names = [desc[0] for desc in self.duckdb_conn.execute("SELECT * FROM ais0112").description]
                    validated_batch = [self.validate_and_convert_data(dict(zip(column_names, row))) for row in batch]

                    logger.info(f"Inserting batch of {len(validated_batch)} rows into PostgreSQL")
                    for row in validated_batch:
                        try:
                            cur.execute("""
                                INSERT INTO dlusr.ais0112 (
                                    prj_id, file_id, sql_id, table_id, obj_id, func_id, owner_name, table_name, caps_table_name, 
                                    sql_obj_type, unique_owner_name, unique_owner_tgt_srv_id, system_biz_id,
                                    call_prj_id, call_file_id, call_sql_id, call_table_id, call_obj_id, call_func_id, call_owner_name,
                                    call_table_name, call_caps_table_name, call_sql_obj_type, call_unique_owner_name,
                                    call_unique_owner_tgt_srv_id, call_system_biz_id, cond_mapping_bit, data_maker, mapping_kind
                                )
                                VALUES (
                                    %(prj_id)s, %(file_id)s, %(sql_id)s, %(table_id)s, %(obj_id)s, %(func_id)s, %(owner_name)s, 
                                    %(table_name)s, %(caps_table_name)s, %(sql_obj_type)s, %(unique_owner_name)s, 
                                    %(unique_owner_tgt_srv_id)s, %(system_biz_id)s, %(call_prj_id)s, %(call_file_id)s, 
                                    %(call_sql_id)s, %(call_table_id)s, %(call_obj_id)s, %(call_func_id)s, %(call_owner_name)s, 
                                    %(call_table_name)s, %(call_caps_table_name)s, %(call_sql_obj_type)s, %(call_unique_owner_name)s, 
                                    %(call_unique_owner_tgt_srv_id)s, %(call_system_biz_id)s, %(cond_mapping_bit)s, %(data_maker)s, 
                                    %(mapping_kind)s
                                )
                                ON CONFLICT (prj_id, file_id, sql_id, table_id, call_prj_id, call_file_id, call_sql_id, call_table_id) 
                                DO NOTHING
                            """, row)
                            logger.debug(f"Successfully inserted row: {row}")
                        except Exception as e:
                            logger.error(f"Error inserting row: {row}")
                            logger.error(f"Error details: {e}")
                            # Continue with the next row
                            continue
                conn.commit()
                logger.info("Committed changes to PostgreSQL")
        except Exception as e:
            logger.error(f"Error during transfer to PostgreSQL: {e}")
            conn.rollback()
        finally:
            self.pg_pool.putconn(conn)
        logger.info("Finished transfer to PostgreSQL")

    async def get_dataset_properties(self, dataset_urn: str) -> Dict:
        url = f"{self.config['datahub_api']['server']}/aspects/{DatasetUrn.url_encode(dataset_urn)}?aspect=datasetProperties&version=0"
        max_retries = 3
        retry_delay = 1  # seconds

        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching dataset properties for {dataset_urn}, attempt {attempt + 1}/{max_retries}")
                async with self.session.get(url, timeout=self.config['datahub_api']['timeout_sec']) as response:
                    if response.status == 200:
                        logger.info(f"Successfully fetched dataset properties for {dataset_urn}")
                        return await response.json()
                    else:
                        logger.error(f"Failed to get dataset properties for {dataset_urn}: HTTP {response.status}")
                        if response.status == 500:
                            logger.error(
                                "Server error occurred. This might be due to the dataset not existing or other server-side issues.")
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
        logger.info("Creating PostgreSQL connection pool")
        pg_config = self.config['target_config']
        try:
            pool = psycopg2.pool.SimpleConnectionPool(
                1, 20,
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


# Add this to the source_registry
from datahub.ingestion.source.source_registry import source_registry

source_registry.register("convert_qtrack", ConvertQtrackSource)
logger.info("Registered convert_qtrack source")
