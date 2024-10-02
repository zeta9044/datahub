import logging
from typing import Any

import duckdb
import psycopg2
from zeta_lab.utilities.tool import get_db_name,get_schema_name

logger = logging.getLogger(__name__)

def create_duckdb_tables(conn: Any):
    """Create necessary tables in DuckDB if they don't exist."""
    create_ais0102(conn)
    create_ais0102_work(conn)
    create_ais0103(conn)
    create_ais0112(conn)
    create_ais0113(conn)
    create_ais0080(conn)
    create_ais0080_work(conn)
    create_ais0081(conn)
    create_ais0081_work(conn)

def create_ais0102(conn: Any):
    logger.info("Creating table 'ais0102'.")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ais0102 (
                prj_id VARCHAR,
                file_id INTEGER,
                sql_id INTEGER,
                table_id INTEGER,
                obj_id INTEGER,
                func_id INTEGER,
                query_type VARCHAR,
                sql_obj_type VARCHAR,
                table_urn VARCHAR,
                system_biz_id VARCHAR,
                system_tgt_srv_id VARCHAR,
                owner_srv_id VARCHAR,
                system_id VARCHAR,
                system_name VARCHAR,
                biz_id VARCHAR,
                biz_name VARCHAR,
                PRIMARY KEY (prj_id, file_id, sql_id, table_id)
        )
    """)
    logger.info("Table 'ais0102' created.")

def create_ais0102_work(conn: Any):
    logger.info("Creating table 'ais0102_work'.")
    conn.execute("""
        DROP TABLE IF EXISTS ais0102_work;
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ais0102_work (
            prj_id VARCHAR,
            table_id INTEGER,
            sql_obj_type VARCHAR,
            table_urn VARCHAR,
            system_biz_id VARCHAR,
            system_tgt_srv_id VARCHAR,
            owner_srv_id VARCHAR,
            system_id VARCHAR,
            system_name VARCHAR,
            biz_id VARCHAR,
            biz_name VARCHAR,
            PRIMARY KEY (
                prj_id, table_id, sql_obj_type, table_urn, 
                system_biz_id, system_tgt_srv_id, owner_srv_id, system_id, system_name, biz_id, biz_name
            )
        )
    """)
    logger.info("Table 'ais0102_work' created.")

def create_ais0103(conn: Any):
    logger.info("Creating table 'ais0103'.")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ais0103 (
                prj_id VARCHAR,
                file_id INTEGER,
                sql_id INTEGER,
                table_id INTEGER,
                col_id INTEGER,
                obj_id INTEGER,
                func_id INTEGER,
                column_urn VARCHAR,
                transform_operation  VARCHAR,
                PRIMARY KEY (prj_id, file_id, sql_id, table_id, col_id)
        )
    """)
    logger.info("Table 'ais0103' created.")

def create_ais0112(conn: Any):
    logger.info("Creating table 'ais0112'.")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ais0112 (
            prj_id VARCHAR(5),
            file_id INTEGER,
            sql_id INTEGER,
            table_id INTEGER,
            call_prj_id VARCHAR(5),
            call_file_id INTEGER,
            call_sql_id INTEGER,
            call_table_id INTEGER,
            obj_id INTEGER,
            func_id INTEGER,
            owner_name VARCHAR(80),
            table_name VARCHAR(1000),
            caps_table_name VARCHAR(1000),
            sql_obj_type VARCHAR(3),
            call_obj_id INTEGER,
            call_func_id INTEGER,
            call_owner_name VARCHAR(80),
            call_table_name VARCHAR(1000),
            call_caps_table_name VARCHAR(1000),
            call_sql_obj_type VARCHAR(3),
            unique_owner_name VARCHAR(80),
            call_unique_owner_name VARCHAR(80),
            unique_owner_tgt_srv_id VARCHAR(100),
            call_unique_owner_tgt_srv_id VARCHAR(100),
            cond_mapping_bit INTEGER,
            data_maker VARCHAR(100),
            mapping_kind VARCHAR(10),
            system_biz_id VARCHAR(80),
            call_system_biz_id VARCHAR(80),
            PRIMARY KEY (prj_id, file_id, sql_id, table_id, call_prj_id, call_file_id, call_sql_id, call_table_id)
        )
    """)
    logger.info("Table 'ais0112' created.")

def create_ais0113(conn: Any):
    logger.info("Creating table 'ais0112'.")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ais0113 (
            prj_id VARCHAR(5),
            file_id INTEGER,
            sql_id INTEGER,
            table_id INTEGER,
            col_id INTEGER,
            call_prj_id VARCHAR(5),
            call_file_id INTEGER,
            call_sql_id INTEGER,
            call_table_id INTEGER,
            call_col_id INTEGER,
            obj_id INTEGER,
            func_id INTEGER,
            owner_name VARCHAR(80),
            table_name VARCHAR(1000),
            caps_table_name VARCHAR(1000),
            sql_obj_type VARCHAR(3),
            col_name VARCHAR(3000),
            caps_col_name VARCHAR(3000),
            col_value_yn CHAR(1),
            col_expr VARCHAR(3000),
            col_name_org VARCHAR(3000),
            caps_col_name_org VARCHAR(3000),
            call_obj_id INTEGER,
            call_func_id INTEGER,
            call_owner_name VARCHAR(80),
            call_table_name VARCHAR(1000),
            call_caps_table_name VARCHAR(1000),
            call_sql_obj_type VARCHAR(3),
            call_col_name VARCHAR(3000),
            call_caps_col_name VARCHAR(3000),
            call_col_value_yn CHAR(1),
            call_col_expr VARCHAR(3000),
            call_col_name_org VARCHAR(3000),
            call_caps_col_name_org VARCHAR(3000),
            unique_owner_name VARCHAR(80),
            call_unique_owner_name VARCHAR(80),
            unique_owner_tgt_srv_id VARCHAR(100),
            call_unique_owner_tgt_srv_id VARCHAR(100),
            cond_mapping INTEGER DEFAULT 2,
            data_maker VARCHAR(100) DEFAULT 'ingest_cli',
            mapping_kind VARCHAR(10),
            col_order_no INTEGER,
            call_col_order_no INTEGER,
            adj_col_order_no INTEGER,
            call_adj_col_order_no INTEGER,
            system_biz_id VARCHAR(80),
            call_system_biz_id VARCHAR(80),
            PRIMARY KEY (prj_id, file_id, sql_id, table_id, col_id, call_prj_id, call_file_id, call_sql_id, call_table_id, call_col_id)
        )
    """)
    logger.info("Table 'ais0113' created.")

def create_ais0080(conn: Any):
    logger.info("Creating table 'ais0080'.")
    conn.execute("""
        DROP TABLE IF EXISTS ais0080;
            """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ais0080 (
            src_prj_id VARCHAR,
            src_owner_name VARCHAR,
            src_caps_table_name VARCHAR,
            src_table_name VARCHAR,
            src_table_name_org VARCHAR,
            src_table_type VARCHAR,
            src_mte_table_id VARCHAR,
            tgt_prj_id VARCHAR,
            tgt_owner_name VARCHAR,
            tgt_caps_table_name VARCHAR,
            tgt_table_name VARCHAR,
            tgt_table_name_org VARCHAR,
            tgt_table_type VARCHAR,
            tgt_mte_table_id VARCHAR,
            src_owner_tgt_srv_id VARCHAR,
            tgt_owner_tgt_srv_id VARCHAR,
            cond_mapping_bit INTEGER,
            mapping_kind VARCHAR,
            src_system_biz_id VARCHAR,
            tgt_system_biz_id VARCHAR,
            src_db_instance_org VARCHAR,
            src_schema_org VARCHAR,
            tgt_db_instance_org VARCHAR,
            tgt_schema_org VARCHAR,
            src_system_id VARCHAR,
            tgt_system_id VARCHAR,
            src_biz_id VARCHAR,
            tgt_biz_id VARCHAR,
            src_system_nm VARCHAR,
            tgt_system_nm VARCHAR,
            src_biz_nm VARCHAR,
            tgt_biz_nm VARCHAR,
            src_system_biz_nm VARCHAR,
            tgt_system_biz_nm VARCHAR,
            PRIMARY KEY (src_prj_id, src_mte_table_id, tgt_prj_id, tgt_mte_table_id)
        )
    """)
    logger.info("Table 'ais0080' created.")

def create_ais0080_work(conn: Any):
    logger.info("Creating table 'ais0080_work'.")
    conn.execute("""
        DROP TABLE IF EXISTS ais0080_work;
            """)
    conn.execute("""
            CREATE TABLE IF NOT EXISTS ais0080_work ( 
                src_prj_id VARCHAR,
                src_owner_name VARCHAR,
                src_caps_table_name VARCHAR,
                src_table_name VARCHAR,
                src_table_type VARCHAR,
                src_mte_table_id VARCHAR,
                src_owner_tgt_srv_id VARCHAR,
                src_system_biz_id VARCHAR,
                tgt_prj_id VARCHAR,
                tgt_owner_name VARCHAR,
                tgt_caps_table_name VARCHAR,
                tgt_table_name VARCHAR,
                tgt_table_type VARCHAR,
                tgt_mte_table_id VARCHAR,
                tgt_owner_tgt_srv_id VARCHAR,
                tgt_system_biz_id VARCHAR,
                cond_mapping_bit INTEGER,
                mapping_kind VARCHAR,                
                PRIMARY KEY (src_prj_id, src_mte_table_id, tgt_prj_id, tgt_mte_table_id)
            )
        """)
    logger.info("Table 'ais0080_work' created.")

def create_ais0081(conn: Any):
    logger.info("Creating table 'ais0081'.")
    conn.execute("""
        DROP TABLE IF EXISTS ais0081;
            """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ais0081 (
            src_prj_id VARCHAR,
            src_owner_name VARCHAR,
            src_caps_table_name VARCHAR,
            src_table_name VARCHAR,
            src_table_name_org VARCHAR,
            src_table_type VARCHAR,
            src_mte_table_id VARCHAR,
            src_caps_col_name VARCHAR,
            src_col_name VARCHAR,
            src_col_value_yn VARCHAR,
            src_mte_col_id INTEGER,
            tgt_prj_id VARCHAR,
            tgt_owner_name VARCHAR,
            tgt_caps_table_name VARCHAR,
            tgt_table_name VARCHAR,
            tgt_table_name_org VARCHAR,
            tgt_table_type VARCHAR,
            tgt_mte_table_id VARCHAR,
            tgt_caps_col_name VARCHAR,
            tgt_col_name VARCHAR,
            tgt_col_value_yn VARCHAR,
            tgt_mte_col_id INTEGER,
            src_owner_tgt_srv_id VARCHAR,
            tgt_owner_tgt_srv_id VARCHAR,
            cond_mapping INTEGER,
            mapping_kind VARCHAR,
            src_system_biz_id VARCHAR,
            tgt_system_biz_id VARCHAR,
            data_maker VARCHAR,
            src_db_instance_org VARCHAR,
            src_schema_org VARCHAR,
            tgt_db_instance_org VARCHAR,
            tgt_schema_org VARCHAR,
            src_system_id VARCHAR,
            tgt_system_id VARCHAR,
            src_biz_id VARCHAR,
            tgt_biz_id VARCHAR,
            src_system_nm VARCHAR,
            tgt_system_nm VARCHAR,
            src_biz_nm VARCHAR,
            tgt_biz_nm VARCHAR,
            src_system_biz_nm VARCHAR,
            tgt_system_biz_nm VARCHAR,
            PRIMARY KEY (src_prj_id, src_mte_table_id, src_mte_col_id, tgt_prj_id, tgt_mte_table_id,tgt_mte_col_id)
        )
    """)
    logger.info("Table 'ais0081' created.")

def create_ais0081_work(conn: Any):
    logger.info("Creating table 'ais0081_work'.")
    conn.execute("""
        DROP TABLE IF EXISTS ais0081_work;
            """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ais0081_work (
            src_prj_id VARCHAR,
            src_owner_name VARCHAR,
            src_caps_table_name VARCHAR,
            src_table_name VARCHAR,
            src_table_type VARCHAR,
            src_mte_table_id VARCHAR,
            src_caps_col_name VARCHAR,
            src_col_name VARCHAR,
            src_col_value_yn VARCHAR,
            src_mte_col_id INTEGER,
            src_owner_tgt_srv_id VARCHAR,
            src_system_biz_id VARCHAR,           
            tgt_prj_id VARCHAR,
            tgt_owner_name VARCHAR,
            tgt_caps_table_name VARCHAR,
            tgt_table_name VARCHAR,
            tgt_table_type VARCHAR,
            tgt_mte_table_id VARCHAR,
            tgt_caps_col_name VARCHAR,
            tgt_col_name VARCHAR,
            tgt_col_value_yn VARCHAR,
            tgt_mte_col_id INTEGER,
            tgt_owner_tgt_srv_id VARCHAR,
            tgt_system_biz_id VARCHAR,
            cond_mapping INTEGER,
            mapping_kind VARCHAR,
            data_maker VARCHAR,
            PRIMARY KEY (src_prj_id, src_mte_table_id, src_mte_col_id, tgt_prj_id, tgt_mte_table_id,tgt_mte_col_id)
        )
    """)
    logger.info("Table 'ais0081_work' created.")


def check_postgres_tables_exist(pg_pool: Any, pg_config: dict):
    """Check if required tables exist in PostgreSQL."""
    with pg_pool.getconn() as conn:
        with conn.cursor() as cur:
            for table in ['ais0112', 'ais0113','ais0080','ais0081']:
                cur.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = '{pg_config['username']}'
                    AND table_name = '{table}'
                );
                """)
                exists = cur.fetchone()[0]
                if not exists:
                    logger.error(f"Error: The table '{table}' does not exist in the PostgreSQL database.")
                else:
                    logger.info(f"Table '{table}' exists in PostgreSQL")
        pg_pool.putconn(conn)

def populate_ais0080(conn: Any):
    logger.info("Populating ais0080 from ais0112")
    try:
        conn.execute("""
            INSERT OR REPLACE INTO ais0102_work (
                prj_id, table_id, sql_obj_type, table_urn, 
                system_biz_id, system_tgt_srv_id, owner_srv_id, system_id, system_name, biz_id, biz_name
            )
            SELECT DISTINCT
                prj_id, table_id, sql_obj_type, table_urn, 
                system_biz_id, system_tgt_srv_id, owner_srv_id, system_id, system_name, biz_id, biz_name
            FROM ais0102
        """)
        conn.execute("""
            INSERT OR REPLACE INTO ais0080_work (
                src_prj_id, src_owner_name, src_caps_table_name, src_table_name,src_table_type, src_mte_table_id,
                src_owner_tgt_srv_id, src_system_biz_id,
                tgt_prj_id, tgt_owner_name, tgt_caps_table_name, tgt_table_name,tgt_table_type,tgt_mte_table_id,
                tgt_owner_tgt_srv_id, tgt_system_biz_id,
                cond_mapping_bit, mapping_kind
            )
            SELECT DISTINCT
                prj_id, owner_name, caps_table_name, table_name, sql_obj_type, cast(table_id as VARCHAR), 
                unique_owner_tgt_srv_id, system_biz_id,
                call_prj_id, call_owner_name, call_caps_table_name, call_table_name, call_sql_obj_type, cast(call_table_id as VARCHAR),
                call_unique_owner_tgt_srv_id, call_system_biz_id,
                cond_mapping_bit, mapping_kind
            FROM ais0112
        """)

        # SQL 쿼리
        sql_query = """
        SELECT
            w80.src_prj_id, w80.src_owner_name, w80.src_caps_table_name, w80.src_table_name, w80.src_table_name AS src_table_name_org, w80.src_table_type, w80.src_mte_table_id, 
            w80.tgt_prj_id, w80.tgt_owner_name, w80.tgt_caps_table_name, w80.tgt_table_name, w80.tgt_table_name AS tgt_table_name_org, w80.tgt_table_type, w80.tgt_mte_table_id, 
            w80.src_owner_tgt_srv_id, w80.tgt_owner_tgt_srv_id, w80.cond_mapping_bit, w80.mapping_kind, w80.src_system_biz_id, w80.tgt_system_biz_id,
            src.table_urn AS src_table_urn, tgt.table_urn AS tgt_table_urn,
            src.system_id AS src_system_id, tgt.system_id AS tgt_system_id, src.biz_id AS src_biz_id, tgt.biz_id AS tgt_biz_id,
            src.system_name AS src_system_nm, tgt.system_name AS tgt_system_nm, src.biz_name AS src_biz_nm, tgt.biz_name AS tgt_biz_nm
        FROM
            ais0080_work w80
        LEFT JOIN
            ais0102_work src ON w80.src_prj_id = src.prj_id AND w80.src_mte_table_id = src.table_id
        LEFT JOIN
            ais0102_work tgt ON w80.tgt_prj_id = tgt.prj_id AND w80.tgt_mte_table_id = tgt.table_id
        """

        # 쿼리 실행 및 데이터 가져오기
        df = conn.execute(sql_query).df()

        # Python 함수 적용
        df['src_db_instance_org'] = df['src_table_urn'].apply(get_db_name)
        df['src_schema_org'] = df['src_table_urn'].apply(get_schema_name)
        df['tgt_db_instance_org'] = df['tgt_table_urn'].apply(get_db_name)
        df['tgt_schema_org'] = df['tgt_table_urn'].apply(get_schema_name)

        # src_system_biz_nm과 tgt_system_biz_nm 계산
        df['src_system_biz_nm'] = df['src_system_nm'] + '_' + df['src_biz_nm']
        df['tgt_system_biz_nm'] = df['tgt_system_nm'] + '_' + df['tgt_biz_nm']

        # ais0080 테이블의 컬럼 순서에 맞게 데이터 프레임 재구성
        columns_order = [
            'src_prj_id', 'src_owner_name', 'src_caps_table_name', 'src_table_name', 'src_table_name_org', 'src_table_type', 'src_mte_table_id',
            'tgt_prj_id', 'tgt_owner_name', 'tgt_caps_table_name', 'tgt_table_name', 'tgt_table_name_org', 'tgt_table_type', 'tgt_mte_table_id',
            'src_owner_tgt_srv_id', 'tgt_owner_tgt_srv_id', 'cond_mapping_bit', 'mapping_kind', 'src_system_biz_id', 'tgt_system_biz_id',
            'src_db_instance_org', 'src_schema_org', 'tgt_db_instance_org', 'tgt_schema_org',
            'src_system_id', 'tgt_system_id', 'src_biz_id', 'tgt_biz_id',
            'src_system_nm', 'tgt_system_nm', 'src_biz_nm', 'tgt_biz_nm', 'src_system_biz_nm', 'tgt_system_biz_nm'
        ]

        df_insert = df[columns_order]

        # 결과를 ais0080 테이블에 삽입
        batch_size = 1000  # 이 값을 조정하여 성능을 최적화할 수 있습니다

        for i in range(0, len(df_insert), batch_size):
            batch = df_insert.iloc[i:i+batch_size]
            conn.execute("INSERT INTO ais0080 SELECT * FROM batch")
            conn.commit()

        print(f"Data insertion completed successfully. {len(df_insert)} rows inserted.")

    except duckdb.Error as e:
        logger.error(f"Error populating ais0080: {e}")


def populate_ais0081(conn: Any):
    logger.info("Populating ais0081 from ais0113")
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ais0081_work AS 
            SELECT
                src_prj_id, src_owner_name, src_caps_table_name, src_table_name,src_table_type, src_mte_table_id,
                src_caps_col_name, src_col_name, src_col_value_yn,src_mte_col_id,
                src_owner_tgt_srv_id, src_system_biz_id,
                tgt_prj_id, tgt_owner_name, tgt_caps_table_name, tgt_table_name,tgt_table_type, tgt_mte_table_id, 
                tgt_caps_col_name, tgt_col_name, tgt_col_value_yn,tgt_mte_col_id,
                tgt_owner_tgt_srv_id, tgt_system_biz_id,
                cond_mapping, mapping_kind, data_maker
            FROM ais0081
        """)
        conn.execute("""
            INSERT OR REPLACE INTO ais0081_work (
                src_prj_id, src_owner_name, src_caps_table_name, src_table_name,src_table_type, src_mte_table_id,
                src_caps_col_name, src_col_name, src_col_value_yn,src_mte_col_id,
                src_owner_tgt_srv_id, src_system_biz_id,
                tgt_prj_id, tgt_owner_name, tgt_caps_table_name, tgt_table_name,tgt_table_type, tgt_mte_table_id, 
                tgt_caps_col_name, tgt_col_name, tgt_col_value_yn,tgt_mte_col_id,
                tgt_owner_tgt_srv_id, tgt_system_biz_id,
                cond_mapping, mapping_kind, data_maker
            )
            SELECT DISTINCT
                prj_id, owner_name, caps_table_name, table_name,sql_obj_type, cast(table_id as VARCHAR),
                caps_col_name, col_name, col_value_yn,col_id,
                unique_owner_tgt_srv_id, system_biz_id,
                call_prj_id, call_owner_name, call_caps_table_name, call_table_name,call_sql_obj_type, cast(call_table_id as VARCHAR),
                call_caps_col_name, call_col_name, call_col_value_yn,call_col_id,
                call_unique_owner_tgt_srv_id, call_system_biz_id,
                cond_mapping, mapping_kind, data_maker
            FROM ais0113
        """)

        # SQL 쿼리
        sql_query = """
        SELECT
            w81.src_prj_id, w81.src_owner_name, w81.src_caps_table_name, w81.src_table_name, w81.src_table_name AS src_table_name_org, w81.src_table_type, w81.src_mte_table_id,
            w81.src_caps_col_name, w81.src_col_name, w81.src_col_value_yn, w81.src_mte_col_id, 
            w81.tgt_prj_id, w81.tgt_owner_name, w81.tgt_caps_table_name, w81.tgt_table_name, w81.tgt_table_name AS tgt_table_name_org, w81.tgt_table_type, w81.tgt_mte_table_id,
            w81.tgt_caps_col_name, w81.tgt_col_name, w81.tgt_col_value_yn, w81.tgt_mte_col_id, 
            w81.src_owner_tgt_srv_id, w81.tgt_owner_tgt_srv_id, w81.cond_mapping, w81.mapping_kind, w81.src_system_biz_id, w81.tgt_system_biz_id, w81.data_maker,
            src.table_urn AS src_table_urn, tgt.table_urn AS tgt_table_urn,
            src.system_id AS src_system_id, tgt.system_id AS tgt_system_id, src.biz_id AS src_biz_id, tgt.biz_id AS tgt_biz_id,
            src.system_name AS src_system_nm, tgt.system_name AS tgt_system_nm, src.biz_name AS src_biz_nm, tgt.biz_name AS tgt_biz_nm
        FROM
            ais0081_work w81
        LEFT JOIN
            ais0102_work src ON w81.src_prj_id = src.prj_id AND w81.src_mte_table_id = src.table_id
        LEFT JOIN
            ais0102_work tgt ON w81.tgt_prj_id = tgt.prj_id AND w81.tgt_mte_table_id = tgt.table_id
        """

        # 쿼리 실행 및 데이터 가져오기
        df = conn.execute(sql_query).df()

        # Python 함수 적용
        df['src_db_instance_org'] = df['src_table_urn'].apply(get_db_name)
        df['src_schema_org'] = df['src_table_urn'].apply(get_schema_name)
        df['tgt_db_instance_org'] = df['tgt_table_urn'].apply(get_db_name)
        df['tgt_schema_org'] = df['tgt_table_urn'].apply(get_schema_name)

        # src_system_biz_nm과 tgt_system_biz_nm 계산
        df['src_system_biz_nm'] = df['src_system_nm'] + '_' + df['src_biz_nm']
        df['tgt_system_biz_nm'] = df['tgt_system_nm'] + '_' + df['tgt_biz_nm']

        # ais0081 테이블의 컬럼 순서에 맞게 데이터 프레임 재구성
        columns_order = [
            'src_prj_id', 'src_owner_name', 'src_caps_table_name', 'src_table_name', 'src_table_name_org', 'src_table_type', 'src_mte_table_id',
            'src_caps_col_name', 'src_col_name', 'src_col_value_yn', 'src_mte_col_id',
            'tgt_prj_id', 'tgt_owner_name', 'tgt_caps_table_name', 'tgt_table_name', 'tgt_table_name_org', 'tgt_table_type', 'tgt_mte_table_id',
            'tgt_caps_col_name', 'tgt_col_name', 'tgt_col_value_yn', 'tgt_mte_col_id',
            'src_owner_tgt_srv_id', 'tgt_owner_tgt_srv_id', 'cond_mapping', 'mapping_kind', 'src_system_biz_id', 'tgt_system_biz_id', 'data_maker',
            'src_db_instance_org', 'src_schema_org', 'tgt_db_instance_org', 'tgt_schema_org',
            'src_system_id', 'tgt_system_id', 'src_biz_id', 'tgt_biz_id',
            'src_system_nm', 'tgt_system_nm', 'src_biz_nm', 'tgt_biz_nm', 'src_system_biz_nm', 'tgt_system_biz_nm'
        ]

        df_insert = df[columns_order]

        # 결과를 ais0081 테이블에 삽입
        batch_size = 1000  # 이 값을 조정하여 성능을 최적화할 수 있습니다

        for i in range(0, len(df_insert), batch_size):
            batch = df_insert.iloc[i:i+batch_size]
            conn.execute("INSERT INTO ais0081 SELECT * FROM batch")
            conn.commit()

        print(f"Data insertion completed successfully. {len(df_insert)} rows inserted into ais0081.")

    except duckdb.Error as e:
        logger.error(f"Error populating ais0081: {e}")
