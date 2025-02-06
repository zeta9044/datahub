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

parsed_sql = parse_one(sql, dialect="snowflake")
select_stmt = parsed_sql.find(exp.Select)
root_scope = sqlglot.optimizer.build_scope(select_stmt)

# Build table mapping
table_info = {}
for table in parsed_sql.find_all(exp.Table):
    full_name = f"{table.db}.{table.name}" if table.db else table.name
    table_info[table.alias or table.name] = {
        'full_name': full_name,
        'schema': table.db,
        'table': table.name,
        'alias': table.alias
    }

print("="*80)
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
                    print(f"  Column: {col.name}")

                try:
                    lineage = sqlglot.lineage.lineage(
                        expr,
                        parsed_sql,
                        dialect="snowflake",
                        scope=root_scope,
                        trim_selects=False
                    )
                    if hasattr(lineage, 'tables'):
                        print(f"    Source tables: {lineage.tables}")
                    if hasattr(lineage, 'columns'):
                        print(f"    Source columns: {[col.sql() for col in lineage.columns]}")
                except Exception as e:
                    print(f"    Lineage info not available: {str(e)}")
        else:
            print("  No direct column references (computed field)")

    except Exception as e:
        print(f"  Error: {str(e)}")

    print("-" * 40)