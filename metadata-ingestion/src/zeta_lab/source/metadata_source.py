import logging
import time
from typing import Dict, Iterable, Any, List
import hashlib

import duckdb
from sqlalchemy import create_engine, text
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.emitter.mce_builder import make_dataset_urn, make_data_platform_urn
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    AuditStampClass,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter
from pydantic import validator

logger = logging.getLogger(__name__)

class MetadataSourceConfig(ConfigModel):
    pg_dsn: str
    duckdb_path: str
    platform: str = "postgres"
    gms_server: str
    batch_size: int = 10000

    @validator("duckdb_path")
    def duckdb_path_must_be_valid(cls, v):
        if not v.endswith('.db'):
            raise ValueError("DuckDB path must end with .db")
        return v

    @validator("batch_size")
    def batch_size_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("Batch size must be positive")
        return v

class MetadataSource(Source):
    def __init__(self, config: MetadataSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        self.emitter = DatahubRestEmitter(self.config.gms_server)

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "MetadataSource":
        config = MetadataSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataChangeEvent]:
        # Step 1: Transfer metadata from PostgreSQL to DuckDB
        self._transfer_metadata_to_duckdb()

        # Step 2: Process metadata from DuckDB and emit to GMS
        conn = duckdb.connect(self.config.duckdb_path, read_only=True)
        try:
            query = """
                SELECT
                    platform,
                    schema_name,
                    field_path,
                    native_data_type,
                    tgt_srv_id,
                    owner_srv_id,
                    system_id,
                    system_name, 
                    biz_id,
                    biz_name,
                    system_biz_id 
                FROM
                    main.metadata_origin
            """
            df = conn.execute(query).fetchdf()

            for schema_name, group in df.groupby('schema_name'):
                mce = self._create_metadata_change_event(schema_name, group)
                yield mce

        except Exception as e:
            self.report.report_failure("metadata_processing", f"Failed to process metadata: {e}")
        finally:
            conn.close()

    def _transfer_metadata_to_duckdb(self):
        try:
            # Transfer metadata
            self._transfer_metadata()

            # Transfer meta instance
            self._transfer_meta_instance()

            # Create metadata_origin table
            self._create_metadata_origin()

        except Exception as e:
            self.report.report_failure("metadata_transfer", f"Failed to transfer metadata: {e}")

    def _transfer_meta_instance(self):
        pg_engine = create_engine(self.config.pg_dsn)
        with pg_engine.connect() as pg_conn:
            select_stmt = """
                SELECT DISTINCT
                       t4.job_id AS job_id,
                       t1.system_tgt_srv_id AS tgt_srv_id,
                       (SELECT LOWER(code_name) 
                        FROM ais2100 
                        WHERE lcode = 'DATABASE_TYPE' 
                          AND mcode = 'DB_TYPE' 
                          AND scode = t3.db_type_cd) AS platform,
                       t2.owner_srv_id AS owner_srv_id,
                       t2.metapop_system_id AS system_id,
                       t2.metapop_system_name AS system_name,
                       t2.metapop_biz_id AS biz_id,
                       t2.metapop_biz_name AS biz_name,
                       t2.metapop_system_id || '_' || t2.metapop_biz_id AS system_biz_id
                FROM ais1024 t4
                JOIN ais1022 t2 ON t4.owner_srv_id = t2.owner_srv_id
                JOIN ais1003 t1 ON t4.job_id = t1.job_id
                JOIN ais1021 t3 ON t1.system_tgt_srv_id = t3.tgt_srv_id 
                               AND t3.srv_dtl_id = t2.srv_dtl_id 
                WHERE t4.aval_ed_dt = '99991231235959'
                  AND t2.aval_ed_dt = '99991231235959'
                  AND t3.aval_ed_dt = '99991231235959'
                  AND t1.map_type = '02'
            """
            result = pg_conn.execute(text(select_stmt))
            rows = result.fetchall()

        conn = duckdb.connect(self.config.duckdb_path)
        conn.execute('''
        CREATE TABLE IF NOT EXISTS meta_instance (
            job_id TEXT,
            tgt_srv_id TEXT,
            platform TEXT,
            owner_srv_id TEXT,
            system_id TEXT,
            system_name TEXT,
            biz_id TEXT,
            biz_name TEXT,
            system_biz_id TEXT,
            PRIMARY KEY (job_id, tgt_srv_id, owner_srv_id)
        )
        ''')

        # Batch insert
        batch_size = self.config.batch_size
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            conn.executemany('''
            INSERT OR REPLACE INTO meta_instance VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', batch)

        conn.close()

    def _transfer_metadata(self):
        pg_engine = create_engine(self.config.pg_dsn)
        with pg_engine.connect() as pg_conn:
            select_stmt = text("""
                SELECT
                    obj_rmk6 AS owner_srv_id,
                    biz_key1 AS catalog_name,
                    biz_key2 AS schema_name,
                    abbr_name AS table_name,
                    obj_seq AS column_order_no,
                    obj_rmk2 AS column_name,
                    obj_rmk3 AS column_type,
                    obj_rmk4 AS biz_id,
                    obj_rmk5 AS system_biz_id,
                    sys_id AS system_id
                FROM
                    qt_meta_populator
                WHERE
                    class_id = 9001
                GROUP BY
                    obj_rmk6, biz_key1, biz_key2, abbr_name, obj_seq, obj_rmk2, obj_rmk3, obj_rmk4, obj_rmk5, sys_id
                HAVING
                    COUNT(*) = 1
                ORDER BY
                    obj_rmk6, biz_key1, biz_key2, abbr_name, obj_seq
            """)
            result = pg_conn.execute(select_stmt)
            rows = result.fetchall()

        conn = duckdb.connect(self.config.duckdb_path)
        conn.execute('''
        CREATE TABLE IF NOT EXISTS qt_meta_populator (
            owner_srv_id TEXT,
            catalog_name TEXT,
            schema_name TEXT,
            table_name TEXT,
            column_order_no INTEGER,
            column_name TEXT,
            column_type TEXT,
            biz_id TEXT,
            system_biz_id TEXT,
            system_id TEXT,
            PRIMARY KEY (owner_srv_id, catalog_name, schema_name, table_name, column_order_no, column_name, column_type, biz_id, system_biz_id, system_id)
        )
        ''')

        # Batch insert
        batch_size = self.config.batch_size
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            conn.executemany('''
            INSERT OR REPLACE INTO qt_meta_populator VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', batch)

        conn.close()

    def _create_metadata_origin(self):
        conn = duckdb.connect(self.config.duckdb_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS main.metadata_origin AS
            SELECT
                LOWER(mi.platform) AS platform,
                LOWER(mi.owner_srv_id || '.' || qmp.catalog_name || '.' || qmp.schema_name || '.' || qmp.table_name) AS schema_name,
                LOWER(qmp.column_name) AS field_path,
                LOWER(qmp.column_type) AS native_data_type,
                LOWER(mi.tgt_srv_id) AS tgt_srv_id,
                UPPER(mi.owner_srv_id) AS owner_srv_id,
                UPPER(mi.system_id) AS system_id,
                mi.system_name, 
                UPPER(mi.biz_id) AS biz_id,
                mi.biz_name,
                UPPER(mi.system_biz_id) AS system_biz_id 
            FROM
                main.meta_instance mi
            JOIN main.qt_meta_populator qmp ON
                mi.system_biz_id = qmp.system_biz_id;
        """)
        conn.close()

    def _create_metadata_change_event(self, schema_name: str, group: Any) -> MetadataChangeEvent:
        dataset_urn = make_dataset_urn(self.config.platform, schema_name, self.ctx.graph.env)
        first_row = group.iloc[0]

        dataset_properties = DatasetPropertiesClass(
            description="",
            name=schema_name,
            customProperties={
                "tgt_srv_id": first_row['tgt_srv_id'],
                "owner_srv_id": first_row['owner_srv_id'],
                "system_id": first_row['system_id'],
                "system_name": first_row['system_name'],
                "biz_id": first_row['biz_id'],
                "biz_name": first_row['biz_name'],
                "system_biz_id": first_row['system_biz_id']
            }
        )

        schema_metadata = SchemaMetadataClass(
            schemaName=schema_name,
            platform=make_data_platform_urn(self.config.platform),
            version=0,
            fields=[
                SchemaFieldClass(
                    fieldPath=row['field_path'],
                    nativeDataType=row['native_data_type'],
                    type=SchemaFieldDataTypeClass(type=self._infer_type_from_native(row['native_data_type'])),
                    description=None,
                    nullable=True,
                    isPartOfKey=False,
                )
                for _, row in group.iterrows()
            ],
            hash=str(hashlib.md5(schema_name.encode()).hexdigest()),
            platformSchema={"com.linkedin.schema.MySqlDDL": {"tableSchema": ""}},
            lastModified=AuditStampClass(
                time=int(time.time() * 1000),
                actor="urn:li:corpuser:datahub",
            ),
        )

        snapshot = DatasetSnapshotClass(
            urn=dataset_urn,
            aspects=[dataset_properties, schema_metadata]
        )

        return MetadataChangeEvent(proposedSnapshot=snapshot)

    def _infer_type_from_native(self, native_type: str) -> str:
        if 'int' in native_type.lower():
            return 'NUMBER'
        elif 'char' in native_type.lower() or 'text' in native_type.lower():
            return 'STRING'
        elif 'date' in native_type.lower() or 'time' in native_type.lower():
            return 'DATETIME'
        else:
            return 'UNKNOWN'

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass

# Register the source with DataHub's source registry
from datahub.ingestion.source.source_registry import source_registry
source_registry.register("metadata_source", MetadataSource)