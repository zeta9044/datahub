import sqlglot
from sqlglot import exp, parse_one
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

def parse_sql_to_flow(query_data):
    query = query_data['query_text']

    # 쿼리를 라인 단위로 분리하여 저장
    query_lines = query.splitlines()

    # Parse the query
    try:
        parsed = parse_one(query)
    except Exception as e:
        print(f"Error parsing query: {e}")
        return []

    flow_entries = []
    column_counter = 0  # Column 타입을 위한 카운터

    def get_node_line_no(node):
        # 노드의 텍스트 표현을 가져와서 해당 라인을 찾음
        node_text = str(node)
        for i, line in enumerate(query_lines, 1):  # 1부터 시작하는 라인 번호
            if node_text.strip() in line:
                return i
        return 0

    def process_node(node, depth=0, parent_flow_id=None):
        nonlocal flow_entries, column_counter
        flow_id = len(flow_entries) + 1

        # Column 타입인 경우 column_counter 증가
        if isinstance(node, exp.Column):
            column_counter += 1
            column_no = column_counter
        else:
            column_no = 0

        # Create entry for current node
        entry = {
            'prj_id': query_data['prj_id'],
            'file_id': int(query_data['file_id']),
            'sql_id': int(query_data['sql_id']),
            'flow_id': flow_id,
            'obj_id': int(query_data['obj_id']),
            'func_id': int(query_data['func_id']),
            'flow_src': type(node).__name__,
            'line_no': get_node_line_no(node),
            'column_no': column_no,
            'flow_depth': depth,
            'rel_flow_id': parent_flow_id if parent_flow_id else -1,
            'sub_sql_id': -1,
            'sql_grp': 0
        }
        flow_entries.append(entry)

        # Process child nodes
        for child in node.args.values():
            if isinstance(child, exp.Expression):
                process_node(child, depth + 1, flow_id)
            elif isinstance(child, list):
                for item in child:
                    if isinstance(item, exp.Expression):
                        process_node(item, depth + 1, flow_id)

        return flow_entries

    # Process the parsed query
    flow_entries = process_node(parsed)

    return flow_entries

# Example usage
query_data = {
    'prj_id': '21',
    'file_id': '2',
    'obj_id': '0',
    'func_id': '0',
    'sql_id': '1',
    'query_text': """INSERT INTO A.B.Table_h
SELECT
  CAST(t1.Col1 AS VARCHAR) AS Col_l,
  CAST(CASE WHEN t1.shopid = 1018 THEN 'INDIE' ELSE 'PACKAGE_VR' END AS VARCHAR) AS Col_m,
  CAST(COALESCE(t2.Col_s, 0) * t4.krw_erate_val AS DECIMAL(38, 0)) AS Col_n,
  CAST((
    COALESCE(t2.Col_s, 0) - COALESCE(t2.dc_amt, 0) - COALESCE(t2.Col_h, 0)
  ) * t4.krw_erate_val AS DECIMAL(38, 0)) AS Col_o,
  TO_TIMESTAMP(t1.Col_r) AS Col_p,
  TO_DATE(SUBSTRING(T1.LOAD_DT, 1, 10)) AS Col_q
FROM A.B.Table_a AS t1
LEFT OUTER JOIN A.B.Table_c AS t2
  ON (
    t1.Col1 = t2.Col1
  )
LEFT OUTER JOIN C.D.Table_i AS t3
  ON (
    t2.shopitemid = t3.shop_item_id
  )
LEFT OUTER JOIN A.B.Table_f AS t4
  ON (
    t1.currencycode = t4.currency_cd
    AND SUBSTRING(CAST(t1.Col_r AS VARCHAR), 1, 10) = t4.kr_dt
  )
WHERE
  NOT t2.itemname IS NULL AND NOT Col_s IS NULL""",
    'query_type': 'UNKNOWN'
}

# Parse the query and get flow entries
flow_entries = parse_sql_to_flow(query_data)

# Convert to DataFrame for easy viewing
df = pd.DataFrame(flow_entries)
print("\nFlow entries:")
print(df[['flow_src', 'line_no', 'column_no', 'flow_depth']])

# Database connection parameters
db_params = {
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres',
    'user': 'dlusr',
    'password': 'dlusr'
}

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Prepare the insert query
    insert_query = '''
        INSERT INTO ais0109 (
            prj_id, file_id, sql_id, flow_id, obj_id, func_id,
            flow_src, line_no, column_no, flow_depth,
            rel_flow_id, sub_sql_id, sql_grp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    # Prepare the data for batch insert
    insert_data = [
        (
            entry['prj_id'], entry['file_id'], entry['sql_id'],
            entry['flow_id'], entry['obj_id'], entry['func_id'],
            entry['flow_src'], entry['line_no'], entry['column_no'],
            entry['flow_depth'], entry['rel_flow_id'], entry['sub_sql_id'],
            entry['sql_grp']
        )
        for entry in flow_entries
    ]

    # Execute batch insert
    execute_batch(cursor, insert_query, insert_data)

    # Commit the transaction
    conn.commit()
    print(f"Successfully inserted {len(flow_entries)} records")

except psycopg2.Error as e:
    print(f"Database error: {e}")
    if conn:
        conn.rollback()

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()