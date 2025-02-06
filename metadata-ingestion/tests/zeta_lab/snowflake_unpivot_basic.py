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

# SQL 파싱
parsed_sql = parse_one(sql, dialect="snowflake")

# Select 문의 컬럼들 추출
select_stmt = parsed_sql.find(exp.Select)
if not select_stmt:
    raise ValueError("No SELECT statement found")

# scope 생성
root_scope = sqlglot.optimizer.build_scope(select_stmt)

print("="*80)
print("Column Lineage Analysis:")
print("="*80)

for expr in select_stmt.expressions:
    try:
        print(f"\nAnalyzing column: {expr.alias_or_name}")
        print(f"Expression: {expr.sql()}")

        # 원본 expression을 직접 사용
        lineage_node = sqlglot.lineage.lineage(
            expr,  # alias가 아닌 expression 자체를 전달
            parsed_sql,
            dialect="snowflake",
            scope=root_scope,
            trim_selects=False
        )

        print("Lineage found:")
        if hasattr(lineage_node, 'tables'):
            print("  Tables:", lineage_node.tables)
        if hasattr(lineage_node, 'columns'):
            print("  Columns:", [col.sql() for col in lineage_node.columns])  # SQL 표현식으로 출력
        if hasattr(lineage_node, 'expressions'):
            print("  Expressions:", [expr.sql() for expr in lineage_node.expressions])

    except sqlglot.errors.SqlglotError as e:
        print(f"  Error analyzing lineage: {str(e)}")
    except Exception as e:
        print(f"  Unexpected error: {str(e)}")

    print("-" * 40)