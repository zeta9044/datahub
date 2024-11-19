import logging
from typing import Optional
import duckdb
from dataclasses import dataclass

@dataclass
class TablePopulatorConfig:
    """테이블 데이터 적재 설정"""
    threads: int = 4  # Default number of threads
    logger: Optional[logging.Logger] = None

class OptimizedTablePopulator:
    def __init__(self, conn: duckdb.DuckDBPyConnection, config: TablePopulatorConfig):
        self.conn = conn
        self.config = config
        self.logger = config.logger or logging.getLogger(__name__)

        # DuckDB 실제 적용 가능한 설정
        self._configure_duckdb()

    def _configure_duckdb(self):
        """실제 적용 가능한 DuckDB 설정"""
        try:
            # threads 설정 적용
            self.conn.execute(f"SET threads TO {self.config.threads}")

            # 메모리 관련 실제 적용 가능한 설정
            self.conn.execute("SET temp_directory='/tmp'")

            self.logger.info("Applied DuckDB configuration")
        except Exception as e:
            self.logger.error(f"Error configuring DuckDB: {e}")
            raise

    def populate_tables(self):
        """순차적으로 테이블 데이터 적재"""
        try:
            # 1. AIS0113에서 직접 파생되는 테이블들
            self._populate_ais0103_from_ais0113()
            self._populate_ais0112_from_ais0113()

            # 2. AIS0081 처리 (AIS0080의 소스가 됨)
            self._populate_ais0081_from_ais0113()

            # 3. AIS0081에서 AIS0080 처리
            self._populate_ais0080_from_ais0081()

            self.logger.info("Completed populating all tables")

        except Exception as e:
            self.logger.error(f"Error in populate_tables: {e}")
            raise

    def _populate_table(self, table_name: str, query: str):
        """단일 테이블 데이터 적재"""
        try:
            # 임시 테이블 생성
            temp_table = f"temp_{table_name}"

            # 기존 임시 테이블이 있다면 삭제
            self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

            # 새로운 임시 테이블 생성
            self.conn.execute(f"CREATE TEMP TABLE {temp_table} AS {query}")

            # 데이터 건수 확인
            count = self.conn.execute(f"SELECT COUNT(*) FROM {temp_table}").fetchone()[0]

            # 기존 데이터 삭제 및 새 데이터 삽입
            self.conn.execute(f"DELETE FROM {table_name}")
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table}")

            # 임시 테이블 정리
            self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

            self.logger.info(f"Populated {table_name} with {count} rows")

        except Exception as e:
            self.logger.error(f"Error populating {table_name}: {e}")
            # 임시 테이블 정리 시도
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            except:
                pass
            raise

    def _populate_ais0103_from_ais0113(self):
        """AIS0103 테이블 적재 - 정확한 컬럼 순서로 매핑"""
        query = """
                SELECT DISTINCT
                    t.prj_id::VARCHAR,                -- 1
                    t.file_id::INTEGER,               -- 2
                    t.sql_id::INTEGER,                -- 3
                    t.table_id::INTEGER,              -- 4
                    t.col_id::INTEGER,                -- 5
                    NULL::INTEGER as obj_id,          -- 6
                    NULL::INTEGER as func_id,         -- 7
                    NULL::VARCHAR as col_name,        -- 8
                    NULL::VARCHAR as col_use_type,    -- 9
                    NULL::VARCHAR as col_alias_yn,    -- 10
                    NULL::VARCHAR as col_name_src,    -- 11
                    NULL::VARCHAR as table_alias_name,-- 12
                    NULL::INTEGER as query_line_no,   -- 13
                    NULL::INTEGER as column_no,       -- 14
                    NULL::VARCHAR as col_alias_name,  -- 15
                    NULL::VARCHAR as col_expr,        -- 16
                    NULL::VARCHAR as sql_state,       -- 17
                    NULL::INTEGER as col_order_no,    -- 18
                    NULL::VARCHAR as col_value_yn,    -- 19
                    NULL::INTEGER as rel_flow_id,     -- 20
                    NULL::INTEGER as rel_table_id,    -- 21
                    NULL::VARCHAR as col_scalar_yn,   -- 22
                    NULL::VARCHAR as fmt_col_type,    -- 23
                    NULL::INTEGER as fmt_begin_pos,   -- 24
                    NULL::INTEGER as fmt_end_pos,     -- 25
                    NULL::VARCHAR as fmt_type,        -- 26
                    NULL::INTEGER as adjust_col_order_no, -- 27
                    t.caps_col_name::VARCHAR,         -- 28
                    NULL::INTEGER as fl_tbl_uid       -- 29
                FROM (
                    SELECT 
                        prj_id,
                        file_id,
                        sql_id,
                        table_id,
                        col_id,
                        caps_col_name
                    FROM ais0113
                    UNION ALL
                    SELECT 
                        call_prj_id,
                        call_file_id,
                        call_sql_id,
                        call_table_id,
                        call_col_id,
                        call_caps_col_name
                    FROM ais0113
                ) t
            """
        self._populate_table('ais0103', query)

    def _populate_ais0112_from_ais0113(self):
        """AIS0112 테이블 적재"""
        query = """
            SELECT DISTINCT
                prj_id, file_id, sql_id, table_id, call_prj_id,
                call_file_id, call_sql_id, call_table_id, obj_id, func_id,
                owner_name, table_name, caps_table_name, sql_obj_type, call_obj_id,
                call_func_id, call_owner_name, call_table_name, call_caps_table_name, 
                call_sql_obj_type, unique_owner_name, call_unique_owner_name, 
                unique_owner_tgt_srv_id, call_unique_owner_tgt_srv_id, 2 as cond_mapping_bit,
                data_maker, mapping_kind, system_biz_id, call_system_biz_id
            FROM ais0113
        """
        self._populate_table('ais0112', query)

    def _populate_ais0081_from_ais0113(self):
        """AIS0081 테이블 적재 (AIS0080의 소스)"""
        query = """
            SELECT DISTINCT
                prj_id as src_prj_id,
                owner_name as src_owner_name,
                caps_table_name as src_caps_table_name,
                table_name as src_table_name,
                caps_table_name as src_table_name_org,
                sql_obj_type as src_table_type,
                CAST(file_id as VARCHAR) as src_mte_table_id,
                CASE WHEN caps_col_name = '*' THEN '[*+*]' ELSE caps_col_name END as src_caps_col_name,
                CASE WHEN col_name = '*' THEN '[*+*]' ELSE col_name END as src_col_name,
                col_value_yn as src_col_value_yn,
                col_id as src_mte_col_id,
                call_prj_id as tgt_prj_id,
                call_owner_name as tgt_owner_name,
                call_caps_table_name as tgt_caps_table_name,
                call_table_name as tgt_table_name,
                call_caps_table_name as tgt_table_name_org,
                call_sql_obj_type as tgt_table_type,
                CAST(call_file_id as VARCHAR) as tgt_mte_table_id,
                CASE WHEN call_caps_col_name = '*' THEN '[*+*]' ELSE call_caps_col_name END as tgt_caps_col_name,
                CASE WHEN call_col_name = '*' THEN '[*+*]' ELSE call_col_name END as tgt_col_name,
                call_col_value_yn as tgt_col_value_yn,
                call_col_id as tgt_mte_col_id,
                unique_owner_tgt_srv_id as src_owner_tgt_srv_id,
                call_unique_owner_tgt_srv_id as tgt_owner_tgt_srv_id,
                cond_mapping,
                mapping_kind,
                data_maker,
                system_biz_id as src_system_biz_id,
                call_system_biz_id as tgt_system_biz_id,
                CASE 
                    WHEN unique_owner_name IS NULL THEN '[owner_undefined]'
                    ELSE split_part(unique_owner_name, '.', 1)
                END as src_db_instance_org,
                CASE 
                    WHEN unique_owner_name IS NULL THEN '[owner_undefined]'
                    ELSE COALESCE(NULLIF(split_part(unique_owner_name, '.', 2), ''), '[owner_undefined]')
                END as src_schema_org,
                CASE 
                    WHEN call_unique_owner_name IS NULL THEN '[owner_undefined]'
                    ELSE split_part(call_unique_owner_name, '.', 1)
                END as tgt_db_instance_org,
                CASE 
                    WHEN call_unique_owner_name IS NULL THEN '[owner_undefined]'
                    ELSE COALESCE(NULLIF(split_part(call_unique_owner_name, '.', 2), ''), '[owner_undefined]')
                END as tgt_schema_org,
                split_part(system_biz_id, '_', 1) as src_system_id,
                CASE 
                    WHEN system_biz_id LIKE '[owner%_undefined' THEN 'undefined'
                    ELSE split_part(system_biz_id, '_', 2)
                END as src_biz_id,
                split_part(call_system_biz_id, '_', 1) as tgt_system_id,
                CASE 
                    WHEN call_system_biz_id LIKE 'owner%_undefined' THEN 'undefined'
                    ELSE split_part(call_system_biz_id, '_', 2)
                END as tgt_biz_id
            FROM ais0113
        """
        self._populate_table('ais0081', query)

    def _populate_ais0080_from_ais0081(self):
        """AIS0080 테이블을 AIS0081에서 적재"""
        query = """
            SELECT DISTINCT
                src_prj_id, src_owner_name, src_caps_table_name, src_table_name, 
                src_table_name_org, src_table_type, src_mte_table_id,
                tgt_prj_id, tgt_owner_name, tgt_caps_table_name, tgt_table_name, 
                tgt_table_name_org, tgt_table_type, tgt_mte_table_id,
                src_owner_tgt_srv_id, tgt_owner_tgt_srv_id,
                2 as cond_mapping_bit, mapping_kind,
                src_system_biz_id, tgt_system_biz_id,
                src_db_instance_org, src_schema_org, tgt_db_instance_org, tgt_schema_org,
                src_system_id, src_biz_id, tgt_system_id, tgt_biz_id
            FROM ais0081
        """
        self._populate_table('ais0080', query)