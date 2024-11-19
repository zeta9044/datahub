import os
import duckdb
from typing import NamedTuple, Optional

class MetaColumns(NamedTuple):
    PLATFORM: int = 0
    PLATFORM_INSTANCE: int = 1
    DEFAULT_DB: int = 2
    DEFAULT_SCHEMA: int = 3
    SYSTEM_BIZ_ID: int = 4

META_COLS = MetaColumns()

def get_meta_instance(db_path: str, prj_id: str,  select_columns: Optional[tuple[int, ...]] = None) -> tuple:
    """
    DuckDB에서 meta_instance 테이블의 지정된 컬럼을 조회합니다.

    Args:
        db_path (str): DuckDB 데이터베이스 파일 경로
        prj_id (str): 조회할 job_id 값
        select_columns (tuple): 조회할 컬럼 인덱스의 튜플
                              예: (META_COLS['PLATFORM'], META_COLS['DEFAULT_DB'])
                              None일 경우 모든 컬럼 반환

    Returns:
        tuple: 요청된 컬럼들의 값
    """
    if not os.path.exists(db_path):
        raise ValueError("metadata.db file does not exist.")

    try:
        conn = duckdb.connect(db_path, read_only=True)

        query = """ 
            SELECT lower(platform), lower(platform_instance), lower(default_db), lower(default_schema), system_biz_id  
            FROM main.meta_instance  
            WHERE prj_id = ? 
            LIMIT 1 
        """
        result = conn.execute(query, [prj_id]).fetchone()
        conn.close()

        if result is None:
            raise ValueError("collect job is not setting. please,complete setting.")

            # 특정 컬럼만 반환
        if select_columns is not None:
            return tuple(result[i] for i in select_columns)

        return result

    except ValueError:
        raise
    except Exception as e:
        raise Exception(f"Error querying meta_instance: {str(e)}")