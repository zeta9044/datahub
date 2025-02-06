from sqlglot import maybe_parse, exp
import sqlglot.lineage
import sqlglot.optimizer

sql = """
INSERT INTO pdi_dw.st_cy_ge
SELECT i.ss_dd AS ss_dd
     , i.ca AS cty_dc
     , i.cc AS cr_dc
     , i.d AS cct_td
     , CASE LEFT(i.devic_type, 1)
           WHEN 'a' THEN '03'
           WHEN 'i' THEN '02'
           ELSE '99'
       END AS eqpmt_se_cd
     , SUM(CASE WHEN RIGHT(i.devic_type, 1) = 'u' THEN i.devic_val ELSE 0 END) AS dld_tcn
     , SUM(CASE WHEN RIGHT(i.devic_type, 1) = 'r' THEN i.devic_val ELSE 0 END) AS sales_amt
     , DATEADD(HOUR, 9, SYSDATE()) AS ld_dm     -- UTC -> KST
FROM clawr_ssssr.st_smy_ios
UNPIVOT (devic_val FOR devic_type IN (au, ar, iu, ir)) AS i
    left outer join dip_dw.twc_sst_agegrp_cd as b
        on b.agegrp_abrv_nm = right(devic_type, 2)
WHERE i.ss_dd = ''
GROUP BY i.ss_dd
       , i.ca
       , i.cc
       , i.d
       , CASE LEFT(i.devic_type, 1)
             WHEN 'a' THEN '03'
             WHEN 'i' THEN '02'
             ELSE '99'
         END;
"""

# Parse and optimize the SQL first
parsed_sql = maybe_parse(sql, dialect="snowflake", error_level=sqlglot.ErrorLevel.IMMEDIATE)
parsed_sql = sqlglot.optimizer.optimizer.optimize(
    parsed_sql,
    dialect="snowflake",
    validate_qualify_columns=False,
    rules=(
        sqlglot.optimizer.optimizer.qualify,
        sqlglot.optimizer.optimizer.pushdown_projections,
        sqlglot.optimizer.optimizer.unnest_subqueries,
        sqlglot.optimizer.optimizer.merge_subqueries,
        sqlglot.optimizer.optimizer.eliminate_ctes,
        sqlglot.optimizer.optimizer.quote_identifiers,
    ),
)
optimized_sql = sqlglot.optimizer.optimize(
    parsed_sql,
    dialect="snowflake",
    qualify_columns=True,
    validate_qualify_columns=False,
    identify=True,
    rules=(
        sqlglot.optimizer.optimizer.qualify,
        sqlglot.optimizer.optimizer.pushdown_projections,
        sqlglot.optimizer.optimizer.unnest_subqueries,
        sqlglot.optimizer.optimizer.merge_subqueries,
        sqlglot.optimizer.optimizer.eliminate_ctes,
        sqlglot.optimizer.optimizer.quote_identifiers,
    ),
)

select_stmt = optimized_sql.find(exp.Select)
root_scope = sqlglot.optimizer.build_scope(select_stmt)

# Build table mapping (including UNPIVOT)
table_info = {}
for table in optimized_sql.find_all(exp.Table):
    full_name = f"{table.db}.{table.name}" if table.db else table.name

    # Store the main table info
    table_info[table.alias or table.name] = {
        'full_name': full_name,
        'schema': table.db,
        'table': table.name,
        'alias': table.alias
    }

    # Check if this table is involved in UNPIVOT
    parent = table.parent
    while parent:
        if isinstance(parent, exp.Unnest):
            if hasattr(parent, 'alias'):
                table_info[parent.alias] = {
                    'full_name': full_name,
                    'schema': table.db,
                    'table': table.name,
                    'alias': parent.alias,
                    'note': 'from UNPIVOT'
                }
        parent = parent.parent

print("="*80)
print("Optimized SQL:")
print(optimized_sql.sql())
print("\n" + "="*80)

print("Table Mappings:")
print("="*80)
for alias, info in table_info.items():
    print(f"Alias: {alias} -> {info['full_name']}")

print("\n" + "="*80)
print("Column Lineage Analysis:")
print("="*80)

def extract_column_refs(expression):
    column_refs = []
    for node in expression.walk():
        if isinstance(node, exp.Column):
            column_refs.append(node)
    return column_refs

for expr in select_stmt.expressions:
    try:
        print(f"\nAnalyzing column: {expr.alias_or_name}")
        print(f"Expression: {expr.sql()}")
        print("Source columns:")

        column_refs = extract_column_refs(expr)

        if column_refs:
            for col in column_refs:
                table_alias = col.table
                if table_alias in table_info:
                    table_data = table_info[table_alias]
                    print(f"  Original Table: {table_data['full_name']} (Alias: {table_alias})")
                    if 'note' in table_data:
                        print(f"  Note: {table_data['note']}")
                else:
                    print(f"  Table alias '{table_alias}' not found in mapping")
                print(f"  Column: {col.name}")

                try:
                    lineage = sqlglot.lineage.lineage(
                        expr,
                        optimized_sql,
                        dialect="snowflake",
                        scope=root_scope,
                        trim_selects=False
                    )
                    if hasattr(lineage, 'tables'):
                        print(f"    Source tables: {lineage.tables}")
                except Exception as e:
                    print(f"    Lineage info not available: {str(e)}")
        else:
            print("  No direct column references (computed field)")

    except Exception as e:
        print(f"  Error: {str(e)}")

    print("-" * 40)