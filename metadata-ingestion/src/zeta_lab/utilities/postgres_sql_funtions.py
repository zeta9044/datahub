import duckdb
import psycopg2
from duckdb.duckdb.typing import BIGINT


def setup_postgresql_functions(duckdb_conn, pg_pool):
    def pg_nextval(seq_name):
        conn = pg_pool.getconn()
        try:
            # 연결 풀에서 연결 가져오기
            if conn:
                print("연결 성공, 쿼리 실행")
                with conn.cursor() as cur:
                    cur.execute(f"SELECT nextval('{seq_name}')")
                    return cur.fetchone()[0]
        finally:
            # 연결 반환
            if conn:
                pg_pool.putconn(conn)

    def pg_ap_common_fn_system_tgtsrvid(prj_id):
        conn  = pg_pool.getconn()
        try:
            # 연결 풀에서 연결 가져오기
            conn = pg_pool.getconn()
            if conn:
                print("연결 성공, 쿼리 실행")
                with conn.cursor() as cur:
                    cur.execute(f"SELECT ap_common_fn_system_tgtsrvid('{prj_id}')")
                    return cur.fetchone()[0]
        finally:
            # 연결 반환
            if conn:
                pg_pool.putconn(conn)
    # DuckDB에 PostgreSQL 함수 등록
    try:
        duckdb_conn.create_function("pg_nextval", pg_nextval, ['VARCHAR'], 'BIGINT')
        duckdb_conn.create_function("pg_ap_common_fn_system_tgtsrvid", pg_ap_common_fn_system_tgtsrvid, ['VARCHAR'], 'VARCHAR')
    except Exception as e:
        print(f"함수 생성 중 에러 발생: {e}")
