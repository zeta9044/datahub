import logging
from typing import Any

import duckdb
import psycopg2

logger = logging.getLogger(__name__)

def create_duckdb_tables(conn: Any):
    """Create necessary tables in DuckDB if they don't exist."""
    create_ais0102(conn)
    create_ais0103(conn)
    create_ais0112(conn)
    create_ais0113(conn)
    create_ais0080(conn)
    create_ais0081(conn)

def create_ais0102(conn: Any):
    logger.info("Creating table 'ais0102' in DuckDB if it doesn't exist")
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
            tgt_system_biz_id VARCHAR,
            owner_srv_id VARCHAR,
            system_id VARCHAR,
            system_name VARCHAR,
            biz_id VARCHAR,
            biz_name VARCHAR,
            PRIMARY KEY (prj_id, file_id, sql_id, table_id)
    )
    """)
    logger.info("Table 'ais0102' created or already exists in DuckDB")

def create_ais0103(conn: Any):
    logger.info("Creating table 'ais0103' in DuckDB if it doesn't exist")
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
    logger.info("Table 'ais0103' created or already exists in DuckDB")

def create_ais0112(conn: Any):
    logger.info("Creating table 'ais0112' in DuckDB if it doesn't exist")
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
    logger.info("Table 'ais0112' created or already exists in DuckDB")

def create_ais0113(conn: Any):
    logger.info("Creating table 'ais0113' in DuckDB if it doesn't exist")
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
        cond_mapping INTEGER,
        data_maker VARCHAR(100),
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
    logger.info("Table 'ais0113' created or already exists in DuckDB")

def create_ais0080(conn: Any):
    logger.info("Creating table 'ais0080' in DuckDB if it doesn't exist")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS ais0080 (
        src_prj_id VARCHAR,
        src_owner_name VARCHAR,
        src_caps_table_name VARCHAR,
        src_table_name VARCHAR,
        src_table_name_org VARCHAR,
        src_table_type VARCHAR,
        src_mte_table_id VARCHAR,
        src_system_tgt_srv_id VARCHAR,
        tgt_prj_id VARCHAR,
        tgt_owner_name VARCHAR,
        tgt_caps_table_name VARCHAR,
        tgt_table_name VARCHAR,
        tgt_table_name_org VARCHAR,
        tgt_table_type VARCHAR,
        tgt_mte_table_id VARCHAR,
        tgt_system_tgt_srv_id VARCHAR,
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
        tgt_system_biz_nm VARCHAR
    )
    """)
    logger.info("Table 'ais0080' created or already exists in DuckDB")

def create_ais0081(conn: Any):
    logger.info("Creating table 'ais0081' in DuckDB if it doesn't exist")
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
        src_system_tgt_srv_id VARCHAR,
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
        tgt_system_tgt_srv_id VARCHAR,
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
        tgt_system_biz_nm VARCHAR
    )
    """)
    logger.info("Table 'ais0081' created or already exists in DuckDB")

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
