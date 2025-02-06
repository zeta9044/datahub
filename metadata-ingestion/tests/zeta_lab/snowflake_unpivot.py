from sqlglot import parse_one, exp

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

print("="* 80)
# 테이블별로 컬럼을 수집하기 위한 딕셔너리 생성
table_columns = {}

# 모든 컬럼을 순회하면서 테이블별로 분류
for column in parse_one(sql,dialect="snowflake").find_all(exp.Column):
    if column.table:  # 테이블 정보가 있는 경우만
        if column.table not in table_columns:
            table_columns[column.table] = set()
        table_columns[column.table].add(column.alias_or_name)

# 테이블과 해당 컬럼들 출력
for table in parse_one(sql,dialect="snowflake").find_all(exp.Table):
    print(f"\nTable: {table.name} (Alias: {table.alias})")
    if table.alias in table_columns:
        print("Columns:", ", ".join(sorted(table_columns[table.alias])))
    else:
        print(f"Looking for columns with alias '{table.alias}'...")
        # table.alias가 있는 모든 컬럼 출력
        matching_columns = [col.alias_or_name for col in parse_one(sql,dialect="snowflake").find_all(exp.Column)
                            if col.table == table.alias]
        if matching_columns:
            print("Found columns:", ", ".join(sorted(matching_columns)))
        else:
            print("No columns found")