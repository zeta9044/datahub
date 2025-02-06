from sqlglot import parse_one, exp
import sqlglot.lineage
import sqlglot.optimizer

sql = """
select 
    a.search_dt as search_dt,
    a.app_id as app_id,
    a.country as cntry_cd,
    case left(replace(a.user_age, 'normalized_demographics_', ''), 1)
        when 'm' then '01'
        when 'f' then '02'
        else '99'
    end as sex_cd,
    nvl(b.agegrp_cd, '81') as agegrp_cd,
    a.user_cnt as stats_val,
    a.confidence as rlablty_val,
    a.date as start_dt,
    a.end_date as end_dt,
    dateadd(hour, 9, sysdate()) as load_dtm     -- utc -> kst
from 
    crawl_sensortower.sst_app_analysis_demographics_app
    unpivot (
        user_cnt for user_age in (
            normalized_demographics_female_18,
            normalized_demographics_female_25,
            normalized_demographics_female_35,
            normalized_demographics_female_45,
            normalized_demographics_female_55,
            normalized_demographics_male_18,
            normalized_demographics_male_25,
            normalized_demographics_male_35,
            normalized_demographics_male_45,
            normalized_demographics_male_55,
            female,
            male
        )
    ) as a
    left outer join dip_dw.twc_sst_agegrp_cd as b
        on b.agegrp_abrv_nm = right(user_age, 2)
where 
    a.search_dt = '2025-01-20'
    and b.agegrp_cd is not null;
"""

# SQL parsing
parsed_sql = parse_one(sql, dialect="snowflake")
select_stmt = parsed_sql.find(exp.Select)
root_scope = sqlglot.optimizer.build_scope(select_stmt)

# Collect table columns
table_columns = {}
for column in parsed_sql.find_all(exp.Column):
    if column.table:
        if column.table not in table_columns:
            table_columns[column.table] = set()
        table_columns[column.table].add(column.alias_or_name)

print("="*80)
print("Table Structure Analysis:")
print("="*80)

# First, print table structure
for table in parsed_sql.find_all(exp.Table):
    print(f"\nTable: {table.name} (Alias: {table.alias})")
    if table.alias in table_columns:
        print("Columns:", ", ".join(sorted(table_columns[table.alias])))
    else:
        print(f"Looking for columns with alias '{table.alias}'...")
        matching_columns = [col.alias_or_name for col in parsed_sql.find_all(exp.Column)
                            if col.table == table.alias]
        if matching_columns:
            print("Found columns:", ", ".join(sorted(matching_columns)))
        else:
            print("No columns found")

print("\n" + "="*80)
print("Column Lineage Analysis:")
print("="*80)

for expr in select_stmt.expressions:
    try:
        print(f"\nAnalyzing column: {expr.alias_or_name}")
        print(f"Expression: {expr.sql()}")
        print("Source information:")

        # Extract all column references from the expression
        column_refs = []
        for node in expr.walk():
            if isinstance(node, exp.Column):
                column_refs.append(node)

        if column_refs:
            for col in column_refs:
                table_alias = col.table
                # Get all columns for this table
                table_cols = table_columns.get(table_alias, set())

                # Find the matching table object
                table_obj = next((t for t in parsed_sql.find_all(exp.Table) if t.alias == table_alias), None)

                print(f"  Table: {table_obj.name if table_obj else 'Unknown'} (Alias: {table_alias})")
                print(f"  Column: {col.name}")
                print(f"  Available columns in this table: {', '.join(sorted(table_cols))}")

        else:
            print("  No direct column references (computed field)")

    except Exception as e:
        print(f"  Error: {str(e)}")

    print("-" * 40)