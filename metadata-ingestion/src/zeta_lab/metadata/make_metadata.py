import hashlib
import json
import logging
import sys
import time
from logging.handlers import RotatingFileHandler
from typing import Optional

import click
import duckdb
from sqlalchemy import create_engine, select, MetaData, Table, func, text

import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata._schema_classes import MetadataChangeEventClass, DatasetSnapshotClass, DatasetPropertiesClass, \
    SchemaMetadataClass, SchemaFieldClass, SchemaFieldDataTypeClass, AuditStampClass
from datahub.metadata.schema_classes import (
    SystemMetadataClass,
)
from zeta_lab.utilities.tool import format_time, infer_type_from_native

logger = logging.getLogger(__name__)

def transfer_meta_instance(pg_dsn: str, duckdb_path: str) -> Optional[bool]:
    """
    :param pg_dsn: PostgreSQL Data Source Name used to establish connection to the PostgreSQL database.
    :param duckdb_path: File path to the DuckDB database where meta instances will be stored.
    :return: Returns True if the transfer is successful, None otherwise.
    """
    try:
        start_time = time.time()
        pg_engine = create_engine(pg_dsn)
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
                       t2.metapop_owner_srv_id AS platform_instance,
                       t2.metapop_inst_name AS default_db,
                       t2.metapop_schema_name AS default_schema,
                       t2.metapop_system_id AS system_id,
                       t2.metapop_biz_id AS biz_id,
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

        rows_as_tuples = [tuple(row) for row in rows]
        conn = duckdb.connect(duckdb_path)
        conn.execute('''
        CREATE TABLE IF NOT EXISTS meta_instance (
            job_id TEXT,
            tgt_srv_id TEXT,
            platform TEXT,
            platform_instance TEXT,
            default_db TEXT,
            default_schema TEXT,            
            system_id TEXT,
            biz_id TEXT,
            system_biz_id TEXT,
            PRIMARY KEY (job_id, tgt_srv_id, platform_instance)
        )
        ''')

        for row in rows_as_tuples:
            try:
                conn.execute('''
                INSERT OR REPLACE INTO meta_instance VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', row)
            except duckdb.ConstraintException as e:
                if "Constraint Error: Duplicate key" in str(e):
                    logging.warning(f"Duplicate key found, skipping: {row}")
                else:
                    logging.error(f"An error occurred while inserting row {row}: {e}")
                    raise

        end_time = time.time()
        time_taken = format_time(end_time - start_time)
        logging.info(f"Meta instance transfer complete! Time taken: {time_taken} ")
        return True
    except Exception as e:
        logging.error(f"An error occurred during meta instance transfer: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def transfer_metadata(pg_dsn: str, duckdb_path: str) -> Optional[float]:
    """
    :param pg_dsn: The Data Source Name (DSN) for connecting to the PostgreSQL database.
    :param duckdb_path: The file path to the DuckDB database.
    :return: The time taken for the metadata transfer in seconds, or None if an error occurred.
    """
    try:
        pg_engine = create_engine(pg_dsn)
        metadata = MetaData()
        pg_table = Table('qt_meta_populator', metadata, autoload_with=pg_engine)

        with pg_engine.connect() as pg_conn:
            select_stmt = select(
                pg_table.c.obj_rmk6.label('owner_srv_id'),
                pg_table.c.biz_key1.label('catalog_name'),
                pg_table.c.biz_key2.label('schema_name'),
                pg_table.c.abbr_name.label('table_name'),
                pg_table.c.obj_seq.label('column_order_no'),
                pg_table.c.obj_rmk2.label('column_name'),
                pg_table.c.obj_rmk3.label('column_type'),
                pg_table.c.obj_rmk4.label('biz_id'),
                pg_table.c.obj_rmk5.label('system_biz_id'),
                pg_table.c.sys_id.label('system_id')
            ).where(pg_table.c.class_id == 9001).group_by(
                pg_table.c.obj_rmk6,
                pg_table.c.biz_key1,
                pg_table.c.biz_key2,
                pg_table.c.abbr_name,
                pg_table.c.obj_seq,
                pg_table.c.obj_rmk2,
                pg_table.c.obj_rmk3,
                pg_table.c.obj_rmk4,
                pg_table.c.obj_rmk5,
                pg_table.c.sys_id
            ).having(func.count() == 1).order_by(
                pg_table.c.obj_rmk6,
                pg_table.c.biz_key1,
                pg_table.c.biz_key2,
                pg_table.c.abbr_name,
                pg_table.c.obj_seq
            )

            result = pg_conn.execute(select_stmt)
            rows = result.fetchall()

        # SQLAlchemy 결과를 리스트로 변환
        rows = [list(row) for row in rows]

        conn = duckdb.connect(duckdb_path)

        # 테이블이 없으면 생성
        conn.execute('''
        CREATE TABLE IF NOT EXISTS qt_meta_populator(
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

        start_time = time.time()

        # 벌크 삽입을 위한 준비
        insert_query = '''
        INSERT OR REPLACE INTO qt_meta_populator VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''

        try:
            # 수동으로 트랜잭션 시작
            conn.execute("BEGIN TRANSACTION")

            # 10000개씩 나누어 벌크 삽입
            chunk_size = 10000
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                conn.executemany(insert_query, chunk)

            # 트랜잭션 커밋
            conn.execute("COMMIT")
        except Exception as e:
            # 오류 발생 시 롤백
            conn.execute("ROLLBACK")
            raise e

        end_time = time.time()
        time_taken = format_time(end_time - start_time)
        logging.info(f"Metadata transfer complete! Time taken: {time_taken} ")
        return time_taken
    except Exception as e:
        logging.error(f"An error occurred during metadata transfer: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def drop_metadata_origin_table(conn):
    """
    :param conn: The database connection object used to execute the SQL command.
    :return: None. This function does not return any value.
    """
    try:
        conn.execute("DROP TABLE IF EXISTS main.metadata_origin")
        logging.info("Dropped table main.metadata_origin")
    except Exception as e:
        logging.error(f"Error dropping main.metadata_origin: {e}")


def create_metadata_origin(conn):
    """
    Create the metadata_origin table if it does not exist by combining data from
    meta_instance and qt_meta_populator tables with specific transformations and
    schema requirements.

    :param conn: Database connection object used to execute SQL queries
    :return: None
    """
    try:
        start_time = time.time()

        drop_metadata_origin_table(conn)
        create_table_query = """
            CREATE TABLE IF NOT EXISTS main.metadata_origin AS
            SELECT DISTINCT
                LOWER(mi.platform) AS platform,
                LOWER(qmp.owner_srv_id || '.' || qmp.catalog_name || '.' || qmp.schema_name || '.' || qmp.table_name) AS schema_name,
                LOWER(qmp.column_name) AS field_path,
                LOWER(qmp.column_type) AS native_data_type,
                LOWER(mi.tgt_srv_id) AS tgt_srv_id,
                UPPER(qmp.owner_srv_id) AS owner_srv_id,
                UPPER(qmp.system_id) AS system_id,
                UPPER(qmp.biz_id) AS biz_id,
                UPPER(qmp.system_biz_id) AS system_biz_id 
            FROM
                main.meta_instance mi
            JOIN main.qt_meta_populator qmp ON
                mi.system_biz_id = qmp.system_biz_id;
        """
        conn.execute(create_table_query)
        end_time = time.time()
        time_taken = format_time(end_time - start_time)
        logging.info(f"main.metadata_origin create complete! Time taken: {time_taken} ")
    except Exception as e:
        logging.error(f"Error creating main.metadata_origin: {e}")
        raise


def create_metadata_table(conn):
    """
    :param conn: The database connection object
    :return: None
    """
    try:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS main.metadata_aspect_v2(
                urn VARCHAR,
                aspect_name VARCHAR,
                "version" BIGINT,
                metadata JSON,
                system_metadata JSON,
                createdon BIGINT
                PRIMARY KEY (urn, aspect_name, "version")
            );
        """
        conn.execute(create_table_query)
        logging.info("Created table main.metadata_aspect_v2 successfully")
    except Exception as e:
        logging.error(f"Error creating main.metadata_aspect_v2: {e}")
        raise


def send_to_gms(metadata_change_proposals, gms_server):
    """
    :param metadata_change_proposals: A list of dictionaries where each dictionary represents a metadata change proposal containing the 'aspect' and 'entityUrn'.
    :param gms_server: A string representing the GMS server address.
    :return: None
    """
    emitter = DatahubRestEmitter(gms_server)
    for proposal in metadata_change_proposals:
        try:
            aspect = proposal['aspect']

            # Create MetadataChangeEventClass
            mce = MetadataChangeEventClass(
                proposedSnapshot=DatasetSnapshotClass(
                    urn=proposal['entityUrn'],
                    aspects=[aspect]
                )
            )

            emitter.emit_mce(mce)
            logging.debug(f"Successfully sent proposal for {proposal['entityUrn']}")
        except Exception as e:
            logging.error(f"Failed to send proposal for {proposal['entityUrn']}: {e}")


def create_schema_fields(group):
    """
    :param group: A DataFrame containing schema field information where each row represents a field with 'field_path' and 'native_data_type' columns.
    :return: A list of SchemaFieldClass instances constructed from the DataFrame's rows, with inferred data types.
    """
    return [
        SchemaFieldClass(
            fieldPath=row['field_path'],
            nativeDataType=row['native_data_type'],
            type=SchemaFieldDataTypeClass(type=infer_type_from_native(row['native_data_type'])),
            nullable=True,
            recursive=False,
            isPartOfKey=False
        ) for _, row in group.iterrows()
    ]


def create_metadata_from_duckdb(duckdb_path: str, json_output_path: Optional[str] = None,
                                gms_server: Optional[str] = None):
    """
    :param duckdb_path: The file path to the DuckDB database.
    :param json_output_path: Optional path where the metadata JSON file will be saved. If not provided, the data will be sent to the GMS server or stored in the DuckDB.
    :param gms_server: Optional URL of the GMS server where metadata will be sent. If not provided, the data will be stored in a JSON file or stored in the DuckDB.
    :return: Boolean indicating the success or failure of the metadata creation process.
    """
    try:
        start_time = time.time()

        conn = duckdb.connect(database=duckdb_path, read_only=False)

        create_metadata_origin(conn)
        if not json_output_path and not gms_server:
            create_metadata_table(conn)

        query = """
            SELECT
                platform,
                schema_name,
                field_path,
                native_data_type,
                tgt_srv_id,
                owner_srv_id,
                system_id,
                biz_id,
                system_biz_id 
            FROM
                main.metadata_origin
        """
        df = conn.execute(query).fetchdf()

        if not df.empty:
            grouped = df.groupby('schema_name')

            metadata_change_proposals = []
            for schema_name, group in grouped:
                first_row = group.iloc[0]
                dataset_urn = builder.make_dataset_urn(platform=first_row['platform'], name=schema_name, env="PROD")

                # SchemaMetadata aspect 생성
                fields = create_schema_fields(group)
                schema_metadata_dict = {
                    "schemaName": schema_name,
                    "platform": builder.make_data_platform_urn(first_row['platform']),
                    "version": 0,
                    "fields": fields,
                    "platformSchema": {
                        "com.linkedin.schema.MySqlDDL": {
                            "tableSchema": ""
                        }
                    },
                    "hash": hashlib.md5(str(fields).encode()).hexdigest(),
                    "lastModified": AuditStampClass(
                        time=int(time.time() * 1000),
                        actor="urn:li:corpuser:datahub",
                    ),
                }
                schema_metadata = SchemaMetadataClass(**schema_metadata_dict)

                metadata_change_proposals.append({
                    "entityType": "dataset",
                    "entityUrn": dataset_urn,
                    "aspectName": "schemaMetadata",
                    "aspect": schema_metadata
                })

                # DatasetProperties aspect 생성
                dataset_properties = DatasetPropertiesClass(
                    description="",
                    name=schema_name,
                    customProperties={
                        "tgt_srv_id": first_row['tgt_srv_id'],
                        "owner_srv_id": first_row['owner_srv_id'],
                        "system_id": first_row['system_id'],
                        "biz_id": first_row['biz_id'],
                        "system_biz_id": first_row['system_biz_id']
                    }
                )

                metadata_change_proposals.append({
                    "entityType": "dataset",
                    "entityUrn": dataset_urn,
                    "aspectName": "datasetProperties",
                    "aspect": dataset_properties
                })

            if json_output_path:
                # JSON 파일로 저장
                with open(json_output_path, 'w') as f:
                    json.dump(metadata_change_proposals, f, indent=2, default=lambda x: x.to_obj())
                logging.info(f"Metadata JSON file created at {json_output_path}")
            elif gms_server:
                # GMS 서버로 전송
                send_to_gms(metadata_change_proposals, gms_server)
                logging.info(f"Metadata sent to GMS server at {gms_server}")
            else:
                # DuckDB에 저장
                for proposal in metadata_change_proposals:
                    dataset_urn = proposal['entityUrn']
                    aspect_name = proposal['aspectName']
                    metadata_json = json.dumps(proposal['aspect'])

                    current_time_millis = int(time.time() * 1000)
                    system_metadata = SystemMetadataClass(
                        lastObserved=current_time_millis,
                        runId=__name__
                    )
                    system_metadata_json = json.dumps(system_metadata.to_obj())

                    insert_query = """
                    INSERT INTO main.metadata_aspect_v2 (urn, aspect_name, "version", metadata, system_metadata, createdon)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """
                    conn.execute(insert_query, (
                        dataset_urn,
                        aspect_name,
                        0,
                        metadata_json,
                        system_metadata_json,
                        current_time_millis
                    ))

                logging.info(f"Inserted or updated metadata for {len(grouped)} tables into main.metadata_aspect_v2")
                conn.commit()

                end_time = time.time()
                time_taken = format_time(end_time - start_time)
                logging.info(f"create_metadata_from_duckdb complete! Time taken: {time_taken} ")
        else:
            logging.warning("No data found in metadata.")

        return True
    except Exception as e:
        logging.error(f"Error in create_metadata_from_duckdb: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()


@click.command()
@click.option('--pg-dsn', required=True, help='PostgreSQL connection string')
@click.option('--duckdb-path', default='metadata.db', help='Path to DuckDB file')
@click.option('--json-output', default=None, help='Path to output JSON file')
@click.option('--gms-server', default=None, help='GMS server URL')
@click.option('--log-file', default='metadata.log', help='Path to log file')
@click.option('--log-level', default='INFO', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
              help='Logging level')
def main(pg_dsn, duckdb_path, json_output, gms_server, log_file, log_level):
    """
    :param pg_dsn: PostgreSQL connection string
    :param duckdb_path: Path to DuckDB file
    :param json_output: Path to output JSON file
    :param gms_server: GMS server URL
    :return: None
    """

    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logging.info("Starting metadata processing")

    try:
        logging.info("Step 1: Transferring meta instance")
        if transfer_meta_instance(pg_dsn, duckdb_path):
            logging.info("Meta instance transfer completed successfully")
        else:
            logging.error("Meta instance transfer failed")
            return

        logging.info("Step 2: Transferring metadata")
        if transfer_metadata(pg_dsn, duckdb_path):
            logging.info("Metadata transfer completed successfully")
        else:
            logging.error("Metadata transfer failed")
            return

        logging.info("Step 3: Creating metadata")
        if create_metadata_from_duckdb(duckdb_path, json_output, gms_server):
            if json_output:
                logging.info(f"Metadata JSON file created at {json_output}")
            elif gms_server:
                logging.info(f"Metadata sent to GMS server at {gms_server}")
            else:
                logging.info("Metadata creation and insertion into DuckDB completed successfully")
        else:
            logging.error("Metadata creation failed")
            return

        logging.info("All steps completed successfully")
    except Exception as e:
        logging.error(f"An error occurred during the process: {e}")
    finally:
        logging.info("Metadata processing finished")


if __name__ == '__main__':
    main()
