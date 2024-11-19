import os
import duckdb
from typing import NamedTuple, Union

class MetaColumns(NamedTuple):
    PLATFORM: int = 0
    PLATFORM_INSTANCE: int = 1
    DEFAULT_DB: int = 2
    DEFAULT_SCHEMA: int = 3
    SYSTEM_BIZ_ID: int = 4

META_COLS = MetaColumns()

def get_meta_instance(db_path: str, prj_id: str,  select_columns: Union[tuple[int, ...], int] = None) -> tuple:
    """
    :param db_path: The file path to the metadata database.
    :param prj_id: The project ID to filter the meta_instance records.
    :param select_columns: Optional tuple of integers specifying which columns to return from the result. If not provided, all columns are returned.
    :return: A tuple containing the requested columns from the meta_instance table. If select_columns is specified, only the columns at the specified indices are returned. Raises ValueError if the prj_id does not exist or if the db_path is invalid. Raises a generic Exception for other errors encountered during the query execution.
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

        # select_columns가 단일 정수인 경우 처리
        if isinstance(select_columns, int):
            return (result[select_columns],)  # 단일 값을 튜플로 반환
        # select_columns가 튜플인 경우 처리
        elif select_columns is not None:
            return tuple(result[i] for i in select_columns)
        return result
    except ValueError:
        raise
    except Exception as e:
        raise Exception(f"Error querying meta_instance: {str(e)}")