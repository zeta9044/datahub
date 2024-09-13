import logging
from typing import List, Dict, Any
import duckdb

logger = logging.getLogger(__name__)

def insert_ais0102(conn: duckdb.DuckDBPyConnection, data: List[Dict[str, Any]]):
    logger.info(f"Inserting {len(data)} rows into ais0102")

    # First, get all existing URNs and their table_ids
    existing_urns = conn.execute("""
        SELECT unique_owner_name || '.' || caps_table_name as urn, table_id
        FROM ais0102
    """).fetchall()
    urn_to_table_id = {urn: table_id for urn, table_id in existing_urns}

    # Find the maximum existing table_id
    max_table_id = max(urn_to_table_id.values()) if urn_to_table_id else 0

    for item in data:
        # Create a unique URN for the table
        urn = f"{item['unique_owner_name']}.{item['caps_table_name']}"

        if urn in urn_to_table_id:
            item['table_id'] = urn_to_table_id[urn]
        else:
            # Assign a new table_id
            max_table_id += 1
            item['table_id'] = max_table_id
            urn_to_table_id[urn] = max_table_id

        # Insert or update the record
        conn.execute("""
            INSERT OR REPLACE INTO ais0102 (
                prj_id, file_id, sql_id, table_id, obj_id, func_id, table_name, caps_table_name,
                owner_name, query_type, sql_obj_type, unique_owner_name, unique_owner_tgt_srv_id, system_biz_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            item['prj_id'], item['file_id'], item['sql_id'], item['table_id'],
            item['obj_id'], item['func_id'], item['table_name'], item['caps_table_name'],
            item['owner_name'], item.get('query_type', ''), item['sql_obj_type'],
            item['unique_owner_name'], item['unique_owner_tgt_srv_id'], item['system_biz_id']
        ])

    conn.commit()
    cursor = conn.execute("SELECT COUNT(*) FROM ais0102")
    inserted_rows = cursor.fetchone()[0]
    logger.info(f"Inserted/Updated {inserted_rows} rows in ais0102")

def insert_ais0103(conn: duckdb.DuckDBPyConnection, data: List[Dict[str, Any]]):
    logger.info(f"Inserting {len(data)} rows into ais0103")
    for item in data:
        # Get table information from ais0102
        table_info = conn.execute("""
            SELECT table_id FROM ais0102
            WHERE prj_id = ? AND file_id = ? AND sql_id = ? AND unique_owner_name = ? AND caps_table_name = ?
        """, [item['prj_id'], item['file_id'], item['sql_id'], item['unique_owner_name'], item['caps_table_name']]).fetchone()

        if not table_info:
            logger.warning(f"Table not found in ais0102: {item['prj_id']}, {item['file_id']}, {item['sql_id']}, {item['unique_owner_name']}, {item['caps_table_name']}")
            continue

        item['table_id'] = table_info[0]

        # Check if the column already exists
        existing = conn.execute("""
            SELECT col_id FROM ais0103
            WHERE prj_id = ? AND file_id = ? AND sql_id = ? AND table_id = ? AND col_name = ?
        """, [item['prj_id'], item['file_id'], item['sql_id'], item['table_id'], item['col_name']]).fetchone()

        if existing:
            item['col_id'] = existing[0]
        else:
            # Get the next available col_id
            next_id = conn.execute("""
                SELECT COALESCE(MAX(col_id), 0) + 1 FROM ais0103
                WHERE prj_id = ? AND file_id = ? AND sql_id = ? AND table_id = ?
            """, [item['prj_id'], item['file_id'], item['sql_id'], item['table_id']]).fetchone()[0]

            item['col_id'] = next_id

        # Insert or update the record
        conn.execute("""
            INSERT OR REPLACE INTO ais0103 (
                prj_id, file_id, sql_id, table_id, col_id, obj_id, func_id,
                col_name, col_expr, caps_col_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            item['prj_id'], item['file_id'], item['sql_id'], item['table_id'],
            item['col_id'], item['obj_id'], item['func_id'], item['col_name'], item['col_expr'],
            item['caps_col_name']
        ])

    conn.commit()
    cursor = conn.execute("SELECT COUNT(*) FROM ais0103")
    inserted_rows = cursor.fetchone()[0]
    logger.info(f"Inserted/Updated {inserted_rows} rows in ais0103")

def insert_ais0112(conn: duckdb.DuckDBPyConnection, data: List[Dict[str, Any]]):
    logger.info(f"Inserting {len(data)} rows into ais0112")
    
    insert_query = """
    INSERT OR REPLACE INTO ais0112 (
        prj_id, file_id, sql_id, table_id, obj_id, func_id, owner_name, table_name, caps_table_name, sql_obj_type,
        unique_owner_name, unique_owner_tgt_srv_id, system_biz_id,
        call_prj_id, call_file_id, call_sql_id, call_table_id, call_obj_id, call_func_id, call_owner_name,
        call_table_name, call_caps_table_name, call_sql_obj_type, call_unique_owner_name,
        call_unique_owner_tgt_srv_id, call_system_biz_id, cond_mapping_bit, data_maker, mapping_kind
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    conn.executemany(insert_query, [tuple(item.values()) for item in data])
    
    conn.commit()
    cursor = conn.execute("SELECT COUNT(*) FROM ais0112")
    inserted_rows = cursor.fetchone()[0]
    logger.info(f"Inserted/Updated {inserted_rows} rows into ais0112")

def insert_ais0113(conn: duckdb.DuckDBPyConnection, data: List[Dict[str, Any]]):
    logger.info(f"Inserting {len(data)} rows into ais0113")
    
    insert_query = """
    INSERT OR REPLACE INTO ais0113 (
        prj_id, file_id, sql_id, table_id, col_id, call_prj_id, call_file_id, call_sql_id, call_table_id, call_col_id,
        obj_id, func_id, owner_name, table_name, caps_table_name, sql_obj_type, col_name, caps_col_name, col_value_yn,
        col_expr, col_name_org, caps_col_name_org, call_obj_id, call_func_id, call_owner_name, call_table_name,
        call_caps_table_name, call_sql_obj_type, call_col_name, call_caps_col_name, call_col_value_yn, call_col_expr,
        call_col_name_org, call_caps_col_name_org, unique_owner_name, call_unique_owner_name, unique_owner_tgt_srv_id,
        call_unique_owner_tgt_srv_id, cond_mapping, data_maker, mapping_kind, col_order_no, call_col_order_no,
        adj_col_order_no, call_adj_col_order_no, system_biz_id, call_system_biz_id
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Then, execute the insert query
    conn.executemany(insert_query, [tuple(item.values()) for item in data])
    
    conn.commit()
    cursor = conn.execute("SELECT COUNT(*) FROM ais0113")
    inserted_rows = cursor.fetchone()[0]
    logger.info(f"Inserted/Updated {inserted_rows} rows into ais0113")
