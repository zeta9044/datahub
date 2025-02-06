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

# Get Select statement and build scope
select_stmt = parsed_sql.find(exp.Select)
root_scope = sqlglot.optimizer.build_scope(select_stmt)

print("="*80)
print("Column Lineage Analysis:")
print("="*80)

# Helper function to extract column references
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

        # Extract all column references from the expression
        column_refs = extract_column_refs(expr)

        if column_refs:
            for col in column_refs:
                print(f"  Table: {col.table or 'N/A'}, Column: {col.name}")
                # Try to get lineage for each source column
                try:
                    lineage = sqlglot.lineage.lineage(
                        col,
                        parsed_sql,
                        dialect="snowflake",
                        scope=root_scope,
                        trim_selects=False
                    )
                    if hasattr(lineage, 'tables'):
                        print(f"    Source tables: {lineage.tables}")
                except Exception as e:
                    print(f"    Lineage error: {str(e)}")
        else:
            print("  No direct column references (possibly a computed field)")

    except Exception as e:
        print(f"  Error: {str(e)}")

    print("-" * 40)