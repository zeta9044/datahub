import sqlglot

query = """
DELETE FROM A.B.Table_a
 WHERE col_d = ':p1' ;

COPY INTO A.B.Table_a
     FROM (
            SELECT $1:col_a              :: NUMBER
                , $1:date                    :: VARCHAR
                , $1:col_b                  :: NUMBER(38,5)
                , REPLACE(SPLIT_PART(metadata$filename, '/', 4), 'search_dt=', '') AS col_d
                , DATEADD(HOUR, 9, SYSDATE()) AS col_c
              FROM @A.B.Table_a_STAGE/search_dt=:p2 
)
ON_ERROR = ABORT_STATEMENT
FORCE=TRUE ;
DROP TABLE IF EXISTS A.B.Table_a;
CREATE TABLE A.B.Table_a AS
SELECT *
FROM (
    SELECT T1.Col1, ROW_NUMBER() OVER (PARTITION BY T1.Col1 ORDER BY T1.S_DT DESC) AS RN, T1.MEMO, T2.BAN_NAME, T1.S_DT, T1.E_DT
    FROM C.D.Table_b T1
    LEFT JOIN C.D.Table_i T2
    ON (T1.BLOCK_REASON_CD = T2.BAN_CD)
    AND T2.LANG = 'ko'
) A
WHERE RN = 1;
DROP TABLE IF EXISTS A.B.Table_c;
CREATE TABLE A.B.Table_c AS
SELECT Col1, SUBSTRING(MAX(LOGIN_DT), 1, 10) AS LST_LOGIN_DT
FROM C.D.Table_d
GROUP BY Col1;
TRUNCATE TABLE IF EXISTS A.B.Table_e;
INSERT INTO A.B.Table_e
SELECT T1.Col1 
    , DATEDIFF(day, SUBSTRING(T1.REG_DT, 1, 10)::DATE, DATEADD(DAY, -1, CONVERT_TIMEZONE('Asia/Seoul', CURRENT_TIMESTAMP))::DATE) AS Col2 
    , CASE WHEN SUBSTRING(T3.E_DT, 1, 10) = '2099-12-31' OR SUBSTRING(T3.E_DT, 1, 10) = '2999-12-31' THEN 'Y' ELSE 'N' END AS Col3 
    , CASE WHEN T3.Col1 IS NOT NULL THEN 'Y' ELSE 'N' END AS Col4 
    , DATEADD(HOUR, 9, SYSDATE()) AS LOAD_DTM
FROM C.D.Table_f T1
LEFT JOIN C.D.Table_g T2
ON (T1.Col1 = T2.Col1)
LEFT JOIN A.B.Table_a T3
ON (T1.Col1 = T3.Col1)
LEFT JOIN A.B.Table_c T4
ON (T1.Col1 = T4.Col1)
LEFT JOIN (
  SELECT Col1, MIN(SUBSTRING(AGREEMENT_DT, 1, 10)) AS AGREEMENT_DT
  FROM C.D.Table_h
  GROUP BY Col1
) T5
ON (T1.Col1 = T5.Col1);
DROP TABLE IF EXISTS A.B.Table_a;
DROP TABLE IF EXISTS A.B.Table_c; 
"""
for q in query.split(";"):
    try:
        parsed = sqlglot.parse_one(q, read="snowflake")
        print("\n\nparsed:\n"+str(parsed))

        # 테이블 관련 노드 검색
        tables = parsed.find_all(sqlglot.exp.Table)

        # 방법 1: table.sql() 사용
        for table in tables:
            print(f"table:{table}")  # 전체 테이블 경로 출력
    except Exception as e:
        print(f"\nquery:\n {q}",q)
        print(f"Error: {e}")
