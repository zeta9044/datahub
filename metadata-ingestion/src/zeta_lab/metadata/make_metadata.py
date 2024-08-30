import logging
import click
from sqlalchemy import create_engine, select, MetaData, Table, func, text
import duckdb
import time
import hashlib
import json
from typing import Optional
import datahub.emitter.mce_builder as builder
from datahub.metadata._schema_classes import DatasetPropertiesClass
from datahub.metadata.schema_classes import (
    SchemaMetadataClass,
    SchemaFieldClass,
    AuditStampClass,
    MySqlDDLClass,
    SystemMetadataClass
)
from zeta_lab.utilities.tool import log_execution_time, infer_type_from_native

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@log_execution_time(log_file='execution_time.log')
def transfer_meta_instance(pg_dsn: str, duckdb_path: str) -> Optional[bool]:
    try:
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

        rows_as_tuples = [tuple(row) for row in rows]
        conn = duckdb.connect(duckdb_path)
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

        for row in rows_as_tuples:
            try:
                conn.execute('''
                INSERT INTO meta_instance VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', row)
            except duckdb.ConstraintException as e:
                if "Constraint Error: Duplicate key" in str(e):
                    logger.warning(f"Duplicate key found, skipping: {row}")
                else:
                    logger.error(f"An error occurred while inserting row {row}: {e}")
                    raise

        logger.info("Meta instance transfer completed successfully.")
        return True
    except Exception as e:
        logger.error(f"An error occurred during meta instance transfer: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


@log_execution_time(log_file='execution_time.log')
def transfer_metadata(pg_dsn: str, duckdb_path: str) -> Optional[float]:
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
        INSERT OR IGNORE INTO qt_meta_populator VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''

        try:
            # 수동으로 트랜잭션 시작
            conn.execute("BEGIN TRANSACTION")

            # 10000개씩 나누어 벌크 삽입
            chunk_size = 10000
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i+chunk_size]
                conn.executemany(insert_query, chunk)

            # 트랜잭션 커밋
            conn.execute("COMMIT")
        except Exception as e:
            # 오류 발생 시 롤백
            conn.execute("ROLLBACK")
            raise e

        end_time = time.time()
        time_taken = end_time - start_time
        logger.info(f"Metadata transfer complete! Time taken: {time_taken:.2f} seconds")
        return time_taken
    except Exception as e:
        logger.error(f"An error occurred during metadata transfer: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def drop_metadata_origin_table(conn):
    try:
        conn.execute("DROP TABLE IF EXISTS main.metadata_origin")
        logger.info("Dropped table main.metadata_origin")
    except Exception as e:
        logger.error(f"Error dropping main.metadata_origin: {e}")


def create_metadata_origin(conn):
    try:
        drop_metadata_origin_table(conn)
        create_table_query = """
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
        """
        conn.execute(create_table_query)
        logger.info("Created table main.metadata_origin successfully")
    except Exception as e:
        logger.error(f"Error creating main.metadata_origin: {e}")
        raise


def create_metadata_table(conn):
    try:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS main.metadata_aspect_v2(
                urn VARCHAR,
                aspect_name VARCHAR,
                "version" BIGINT,
                metadata JSON,
                system_metadata JSON,
                createdon BIGINT,
                tgt_srv_id VARCHAR,
                owner_srv_id VARCHAR,
                system_id VARCHAR,
                system_name VARCHAR,
                biz_id VARCHAR,
                biz_name VARCHAR,
                system_biz_id VARCHAR,
                PRIMARY KEY (urn, aspect_name, "version")
            );
        """
        conn.execute(create_table_query)
        logger.info("Created table main.metadata_aspect_v2 successfully")
    except Exception as e:
        logger.error(f"Error creating main.metadata_aspect_v2: {e}")
        raise


@log_execution_time(log_file='execution_time.log')
def create_metadata_from_duckdb(duckdb_path: str, json_output_path: Optional[str] = None):
    try:
        conn = duckdb.connect(database=duckdb_path, read_only=False)

        create_metadata_origin(conn)
        if not json_output_path:
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
                system_name, 
                biz_id,
                biz_name,
                system_biz_id 
            FROM
                main.metadata_origin
        """
        df = conn.execute(query).fetchdf()

        if not df.empty:
            grouped = df.groupby('schema_name')
            metadata_list = []

            for schema_name, group in grouped:
                first_row = group.iloc[0]
                dataset_urn = builder.make_dataset_urn(platform=first_row['platform'], name=schema_name)

                schema_fields = [
                    SchemaFieldClass(
                        fieldPath=row['field_path'],
                        type=infer_type_from_native(row['native_data_type']),
                        nativeDataType=row['native_data_type'],
                        nullable=True,
                        recursive=False,
                        isPartOfKey=False
                    ) for _, row in group.iterrows()
                ]

                group_string = group.to_json(orient='records')
                hash_md5 = hashlib.md5(group_string.encode('utf-8')).hexdigest()

                current_time_millis = int(time.time() * 1000)
                created_on = current_time_millis

                schema_metadata = SchemaMetadataClass(
                    schemaName=schema_name,
                    platform=builder.make_data_platform_urn(first_row['platform']),
                    version=0,
                    hash=hash_md5,
                    platformSchema=MySqlDDLClass(tableSchema=""),
                    fields=schema_fields,
                    lastModified=AuditStampClass(time=current_time_millis, actor="urn:li:corpuser:qtrack")
                )

                custom_properties = {
                    'tgt_srv_id': first_row['tgt_srv_id'],
                    'owner_srv_id': first_row['owner_srv_id'],
                    'system_id': first_row['system_id'],
                    'system_name': first_row['system_name'],
                    'biz_id': first_row['biz_id'],
                    'biz_name': first_row['biz_name'],
                    'system_biz_id': first_row['system_biz_id']
                }

                dataset_properties = DatasetPropertiesClass(
                    customProperties=custom_properties,
                    name=schema_name,
                    description=""
                )

                system_metadata = SystemMetadataClass(
                    lastObserved=current_time_millis,
                    runId=__name__
                )

                metadata_record = {
                    "urn": dataset_urn,
                    "aspects": [
                        {
                            "com.linkedin.schema.SchemaMetadata": schema_metadata.to_obj()
                        },
                        {
                            "com.linkedin.common.DatasetProperties": dataset_properties.to_obj()
                        }
                    ]
                }

                if json_output_path:
                    metadata_list.append(metadata_record)
                else:
                    schema_metadata_json = json.dumps(schema_metadata.to_obj())
                    dataset_properties_json = json.dumps(dataset_properties.to_obj())
                    system_metadata_json = json.dumps(system_metadata.to_obj())

                    insert_query = """
                    INSERT INTO main.metadata_aspect_v2 (urn, aspect_name, "version", metadata, system_metadata, createdon, tgt_srv_id, owner_srv_id, system_id, system_name, biz_id, biz_name, system_biz_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    conn.execute(insert_query, (
                        dataset_urn,
                        'schemaMetadata',
                        0,
                        schema_metadata_json,
                        system_metadata_json,
                        created_on,
                        first_row['tgt_srv_id'],
                        first_row['owner_srv_id'],
                        first_row['system_id'],
                        first_row['system_name'],
                        first_row['biz_id'],
                        first_row['biz_name'],
                        first_row['system_biz_id']
                    ))

                    conn.execute(insert_query, (
                        dataset_urn,
                        'datasetProperties',
                        0,
                        dataset_properties_json,
                        system_metadata_json,
                        created_on,
                        first_row['tgt_srv_id'],
                        first_row['owner_srv_id'],
                        first_row['system_id'],
                        first_row['system_name'],
                        first_row['biz_id'],
                        first_row['biz_name'],
                        first_row['system_biz_id']
                    ))

            if json_output_path:
                with open(json_output_path, 'w') as f:
                    json.dump(metadata_list, f, indent=2)
                logger.info(f"Metadata JSON file created at {json_output_path}")
            else:
                logger.info(f"Inserted or updated metadata for {len(grouped)} tables into main.metadata_aspect_v2")
                conn.commit()
        else:
            logger.warning("No data found in metadata.")

        return True
    except Exception as e:
        logger.error(f"Error in create_metadata_from_duckdb: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

@click.command()
@click.option('--pg-dsn', required=True, help='PostgreSQL connection string')
@click.option('--duckdb-path', default='metadata.db', help='Path to DuckDB file')
@click.option('--json-output', default='metadata.json', help='Path to output JSON file (if not provided, data will be inserted into DuckDB)')
def main(pg_dsn, duckdb_path, json_output):
    logger.info("Starting metadata processing")

    try:
        logger.info("Step 1: Transferring meta instance")
        if transfer_meta_instance(pg_dsn, duckdb_path):
            logger.info("Meta instance transfer completed successfully")
        else:
            logger.error("Meta instance transfer failed")
            return

        logger.info("Step 2: Transferring metadata")
        if transfer_metadata(pg_dsn, duckdb_path):
            logger.info("Metadata transfer completed successfully")
        else:
            logger.error("Metadata transfer failed")
            return

        logger.info("Step 3: Creating metadata")
        if create_metadata_from_duckdb(duckdb_path, json_output if json_output != 'metadata.json' else None):
            if json_output != 'metadata.json':
                logger.info(f"Metadata JSON file created at {json_output}")
            else:
                logger.info("Metadata creation and insertion into DuckDB completed successfully")
        else:
            logger.error("Metadata creation failed")
            return

        logger.info("All steps completed successfully")
    except Exception as e:
        logger.error(f"An error occurred during the process: {e}")
    finally:
        logger.info("Metadata processing finished")

if __name__ == '__main__':
    main()