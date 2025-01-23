from dataclasses import dataclass
from typing import List, Dict

from sqlglot import exp, maybe_parse
from sqlglot.expressions import (
    Expression, Select, Table, Join, Pivot, Column, Identifier,
    TableAlias, Boolean, Tuple as TupleExp, Subquery
)

_table_counter = 0
_unpivot_counter = 0


def _get_next_table_num() -> int:
    global _table_counter
    _table_counter += 1
    return _table_counter


def _get_next_unpivot_num() -> int:
    global _unpivot_counter
    _unpivot_counter += 1
    return _unpivot_counter


@dataclass
class UnpivotInfo:
    """UNPIVOT 연산과 관련된 정보를 저장하는 데이터 클래스"""
    original_table: Table
    original_alias: str  # T0, T1 등으로 자동 생성
    unpivot_alias: str  # UV1, UV2 등으로 자동 생성

    # (val1, val2) 와 같이 다중 value 컬럼이 있을 수 있으므로 Expression 리스트로 둔다.
    # 하나만 있으면 length=1 리스트로 처리.
    value_expressions: List[Expression]

    # FOR 뒤에 오는 'c' 같은 이름 (Alias)
    name_column: str

    # UNPIVOT에서 IN (...) 부분에 들어가는 표현들을 담는다.
    # 다중 컬럼( (c1, c2), (c3, c4) ) 등을 고려해, List[List[Expression]] 형태로 둔다.
    # 단일 컬럼 UNPIVOT이라면, 각 내부 리스트가 길이 1이 됨.
    pivot_column_sets: List[List[Expression]]

    nested_unpivots: List['UnpivotInfo']


def convert_unpivot_to_lateral(ast: Expression) -> Expression:
    """SQL AST를 받아 UNPIVOT을 LATERAL JOIN으로 변환"""
    global _table_counter, _unpivot_counter
    _table_counter = 0  # 초기화
    _unpivot_counter = 0

    input_ast = ast.copy()

    # UNPIVOT 구문 체크
    has_unpivot = False

    def check_unpivot_exists(node: Expression) -> None:
        nonlocal has_unpivot
        if isinstance(node, Select):
            if node.args.get("from"):
                from_table = node.args["from"].args["this"]
                if from_table.args.get("pivots"):
                    unpivot_exprs = [p for p in from_table.args["pivots"] if p.args.get("unpivot")]
                    if unpivot_exprs:
                        has_unpivot = True
        for child in node.iter_expressions():
            check_unpivot_exists(child)

    check_unpivot_exists(input_ast)

    if not has_unpivot:
        return input_ast

    return _transform_from_clause(input_ast)


def _transform_from_clause(node: Expression) -> Expression:
    """FROM 절 변환 및 변환된 결과에 대한 컬럼 참조 업데이트"""
    if isinstance(node, Select):
        if node.args.get("from"):
            from_table = node.args["from"].args["this"]
            if from_table.args.get("pivots"):
                unpivot_exprs = [p for p in from_table.args["pivots"] if p.args.get("unpivot")]
                if unpivot_exprs:
                    unpivot_info = None
                    for up_expr in unpivot_exprs:
                        unpivot_info = _collect_unpivot_info(from_table, up_expr)
                        new_from = _convert_unpivot_node(from_table, unpivot_info)
                        from_table = new_from

                    # FROM 테이블 갱신
                    node.args["from"].args["this"] = from_table

                    # SELECT 절 및 서브 트리 전체 컬럼 참조 업데이트
                    # (기존에는 SELECT expressions만 업데이트했지만,
                    #  이제는 WHERE, GROUP BY, ORDER BY 등도 모두 수정)
                    _update_column_aliases_in_subtree(node, unpivot_info)

    # 재귀 처리
    for child in node.iter_expressions():
        _transform_from_clause(child)

    return node


def _get_table_alias(table: Table, generate_if_missing: bool = True) -> str:
    """테이블의 alias를 반환. 없으면 선택적으로 생성"""
    if table.args.get("alias") and isinstance(table.args["alias"], TableAlias):
        return table.args["alias"].args["this"].name
    if not generate_if_missing:
        return ""
    return f"T{_get_next_table_num()}"


def _get_unpivot_alias(pivot: Pivot, generate_if_missing: bool = True) -> str:
    """UNPIVOT 결과의 alias를 반환. 없으면 선택적으로 생성"""
    if pivot.args.get("alias") and isinstance(pivot.args["alias"], TableAlias):
        return pivot.args["alias"].args["this"].name
    if not generate_if_missing:
        return ""
    return f"UV{_get_next_unpivot_num()}"


def _collect_unpivot_info(node: Table, pivot: Pivot) -> UnpivotInfo:
    """UNPIVOT 정보 수집"""
    # 기존 alias 사용 또는 새로 생성
    original_alias = _get_table_alias(node)
    unpivot_alias = _get_unpivot_alias(pivot)

    value_expressions = pivot.args["expressions"]
    name_column = pivot.args["field"].args["this"].name
    pivot_exps = pivot.args["field"].args["expressions"]

    pivot_column_sets: List[List[Expression]] = []
    for expr in pivot_exps:
        if isinstance(expr, TupleExp):
            pivot_column_sets.append(list(expr.expressions))
        else:
            pivot_column_sets.append([expr])

    nested_unpivots = []
    if pivot.args.get("nested_pivot"):
        nested_up_expr = pivot.args["nested_pivot"].args.get("pivots")
        if nested_up_expr:
            nested_unpivot_exprs = [p for p in nested_up_expr if p.args.get("unpivot")]
            for nested_p in nested_unpivot_exprs:
                nested_unpivots.append(_collect_unpivot_info(pivot.args["nested_pivot"], nested_p))

    return UnpivotInfo(
        original_table=node,
        original_alias=original_alias,
        unpivot_alias=unpivot_alias,
        value_expressions=value_expressions,
        name_column=name_column,
        pivot_column_sets=pivot_column_sets,
        nested_unpivots=nested_unpivots
    )


def _update_column_aliases_in_subtree(node: Select, unpivot_info: UnpivotInfo) -> None:
    """
    SELECT 노드 전체(SELECT 절, WHERE, GROUP BY, ORDER BY, HAVING 등)에 걸쳐
    컬럼 참조를 일괄 변환한다.

    UNPIVOT으로 생성되는 컬럼(value와 name 컬럼)은 그대로 두고,
    나머지 컬럼들의 테이블 별칭을 원본 테이블 별칭으로 변경한다.
    """
    unpivot_columns = _get_unpivot_columns(unpivot_info)

    def _recursive_column_update(expr: Expression) -> Expression:
        """재귀적으로 모든 노드의 컬럼 참조를 업데이트하는 내부 함수"""
        # 리스트 타입 처리
        if isinstance(expr, list):
            return [_recursive_column_update(item) if isinstance(item, Expression) else item for item in expr]

        # 표현식 처리
        if isinstance(expr, Expression):
            # 컬럼 타입일 때 별칭 변경
            if isinstance(expr, Column):
                table_name = expr.args.get("table")
                if table_name and isinstance(table_name, Identifier):
                    table_name = table_name.name

                # UNPIVOT 별칭을 참조하는 컬럼이면서
                if table_name == unpivot_info.unpivot_alias:
                    col_name = expr.args["this"].name
                    # UNPIVOT으로 생성된 컬럼(value나 name)이 아닌 경우에만
                    if col_name not in unpivot_columns:
                        # 원본 테이블 별칭으로 변경
                        expr.set("table", Identifier(this=unpivot_info.original_alias))

            # 자식 노드들도 재귀적으로 업데이트
            for k, v in expr.args.items():
                if isinstance(v, (Expression, list)):
                    expr.set(k, _recursive_column_update(v))

        return expr

    # 노드의 모든 하위 식 업데이트
    update_keys = ['expressions', 'this', 'where', 'group', 'having', 'order', 'window']
    for key in update_keys:
        # 키가 존재하고 값이 None이 아닌 경우에만 처리
        if node.args.get(key) is not None:
            updated_expr = _recursive_column_update(node.args[key])
            node.set(key, updated_expr)

def _get_unpivot_columns(unpivot_info: UnpivotInfo) -> set[str]:
    """UNPIVOT으로 생성되는 컬럼들(value와 name)의 이름을 반환"""
    columns = {unpivot_info.name_column}  # name 컬럼

    # value 컬럼들 추가
    for expr in unpivot_info.value_expressions:
        alias_name = getattr(expr, "alias", None)
        if not alias_name and isinstance(expr, Column) and expr.args.get("this"):
            alias_name = expr.args["this"].name
        if not alias_name:
            alias_name = expr.sql().replace(" ", "_")
        columns.add(alias_name)

    return columns


def _create_column_mappings(unpivot_info: UnpivotInfo) -> Dict[str, Expression]:
    """
    unpivot_info에 따라:
      - value_columns, name_column 등을 UV별칭에서 가져와서
        실제로 Column(table=..., this=...) 형태로 매핑
    """
    mappings = {}

    # 다중 value 컬럼일 수 있으므로, value_expressions에 들어 있는 Expression의 alias 이름 추출
    # 하지만 UNPIVOT 변환 후 SELECT 구문에서 "val1", "val2" 등 alias를 어떻게 붙이는지에 따라 달라질 수 있음.
    # 여기서는 단순히 expression의 `alias` (혹은 식별자)로 간주하고, 그대로 매핑한다고 가정.
    # 실제 로직에선 더 복잡한 처리가 필요할 수 있음.

    # name_column은 무조건 하나라고 가정
    mappings[unpivot_info.name_column] = Column(
        this=Identifier(this=unpivot_info.name_column),
        table=Identifier(this=unpivot_info.unpivot_alias)
    )

    # value_expressions 각각은 'val1', 'val2' 등 이름을 가지고 있을 수도 있고,
    # 없을 수도 있음. 보통은 UNPIVOT(val1, val2) 문법에서 val1, val2가 이름이 되지만,
    # sqlglot 파싱 결과에 따라 달라질 수 있음.
    # 원 코드에서는 단순히 [col.name for col in pivot.args["expressions"]] 형태였으나
    # expression이 함수/식일 경우 name이 없을 수 있으므로, 여기서는 임시로 "COL_{i}"로 매핑하거나,
    # 혹은 Expression SQL 문자열을 키로 삼는 방법도 있음.

    # 원본 코드의 로직(문자열 이름으로 매핑)과 최대한 비슷하게 가정:
    for expr in unpivot_info.value_expressions:
        # 식별자 이름 추출(Identifier) or 임시 처리
        alias_name = getattr(expr, "alias", None)
        if not alias_name and isinstance(expr, Column) and expr.args.get("this"):
            alias_name = expr.args["this"].name
        if not alias_name:
            # 만약 이름이 전혀 없으면, 임시로 expr.sql() 등을 써서 식별자를 만든다.
            alias_name = expr.sql().replace(" ", "_")

        mappings[alias_name] = Column(
            this=Identifier(this=alias_name),
            table=Identifier(this=unpivot_info.unpivot_alias)
        )

    return mappings


def _convert_unpivot_node(node: Table, unpivot_info: UnpivotInfo) -> Expression:
    """UNPIVOT을 LATERAL JOIN + UNION ALL 서브쿼리로 변환"""
    # 원본 테이블 (T0 스타일 별칭)
    original_table = Table(
        this=node.args["this"],
        db=node.args.get("db"),
        catalog=node.args.get("catalog"),
        alias=TableAlias(this=Identifier(this=unpivot_info.original_alias))
    )

    # pivot_column_sets: ex) [[c1, c2], [c3, c4]]
    # value_expressions: ex) [val1, val2]
    # name_column: ex) "c"
    union_parts = []
    for idx, pivot_set in enumerate(unpivot_info.pivot_column_sets):
        # pivot_set는 expression들의 리스트.
        # 만약 단일컬럼 UNPIVOT이면 [ Column(this=Identifier(c1)) ] 식이 됨.
        # 다중컬럼이면 [ Column(this=Identifier(c1)), Column(this=Identifier(c2)) ] 식

        # SELECT 문으로 구성
        select_exprs = []

        # 첫 번째 컬럼: name_column
        # - Snowflake 문법에서 col IN (c1, c2)처럼 c1,c2 문자열 자체를 행으로 펼치는 경우,
        #   "c1", "c2" 같은 리터럴을 넣어서 name_column에 매핑해주어야 하는데
        #   실제로 "c1" 등은 pivot_set에서 직접 추출할 수 없다(식일 수도 있기 때문).
        # - 여기서는 단순화: pivot_set를 문자열로 만들어 Literal로 alias를 준다거나,
        #   혹은 그냥 idx를 쓰는 식으로 처리한다.
        #   혹은 pivot_set가 단일 Identifier면 그 이름을 string literal로 놓을 수도 있음.

        pivot_label = None
        # 만약 pivot_set가 [Column(this=Identifier("c1+1"))] 같은 건 표현식이니,
        # 그걸 문자열로 만들어서 name_column에 넣어야 할 수도 있음.
        # 여기서는 단순히 pivot_set[0].sql()를 string literal로 박는 예시:
        pivot_label = pivot_set[0].sql(dialect="snowflake")
        if len(pivot_set) > 1:
            # 여러 개면 뒤쪽 expression들도 합쳐보거나, 별도 처리를 할 수도 있음.
            pivot_label += "_" + "_".join(e.sql(dialect="snowflake") for e in pivot_set[1:])

        select_exprs.append(
            exp.Alias(
                this=exp.Literal.string(pivot_label),
                alias=exp.Identifier(this=unpivot_info.name_column)
            )
        )

        # value_expressions와 pivot_set가 1:1 매핑된다고 가정
        # 예: (val1, val2) -> (c1, c2)
        # expression 개수 불일치(예: val1, val2 vs c1, c2, c3) 시엔 추가 로직 필요
        if len(pivot_set) != len(unpivot_info.value_expressions):
            # 간단히 둘 중 작은 쪽에 맞추거나, 오류 처리
            msg = f"[WARN] pivot_set({len(pivot_set)}) vs value_expressions({len(unpivot_info.value_expressions)}) size mismatch."
            # 실제 상황에 맞는 처리 필요
            # 여기서는 작은 쪽 개수에만 맞춰서 진행
            zipped = zip(unpivot_info.value_expressions, pivot_set)
        else:
            zipped = zip(unpivot_info.value_expressions, pivot_set)

        for val_expr, pivot_expr in zipped:
            # val_expr는 UNPIVOT(...)에서 선언한 값 컬럼(Expression)
            # pivot_expr는 원본 테이블에서 꺼낼 실제 Expression
            # SELECT T0.(pivot_expr) AS val_expr_alias
            alias_name = getattr(val_expr, "alias", None)
            if not alias_name and isinstance(val_expr, Column) and val_expr.args.get("this"):
                alias_name = val_expr.args["this"].name
            if not alias_name:
                alias_name = val_expr.sql(dialect="snowflake")

            # pivot_expr는 단순 Identifier(c1)일 수도 있고, 함수일 수도 있음.
            # 여기서는 original_alias(T0) 붙여서 T0.c1 형태로 만들어준다.
            pivot_expr_with_alias = pivot_expr.copy()
            if isinstance(pivot_expr_with_alias, Column):
                pivot_expr_with_alias.set("table", Identifier(this=unpivot_info.original_alias))
            else:
                # pivot_expr가 함수/식이라면, 테이블 별칭만 앞에 붙일 수 있는 형태인지
                # 혹은 c1+1 같은 경우 c1에만 별칭 넣어야 하는지 등 추가 변환 필요
                # 여기서는 단순하게 pivot_expr 자체를 사용하되, c1이 Column이면 table alias를 붙이는 식으로 재귀 변환
                pivot_expr_with_alias = _apply_table_alias_to_expr(pivot_expr_with_alias, unpivot_info.original_alias)

            select_exprs.append(
                exp.Alias(
                    this=pivot_expr_with_alias,
                    alias=exp.Identifier(this=alias_name)
                )
            )

        union_parts.append(Select(expressions=select_exprs))

    # UNION ALL로 묶기
    union_query = union_parts[0]
    for part in union_parts[1:]:
        union_query = exp.Union(
            this=union_query,
            expression=part,
            distinct=False
        )

    # LATERAL 서브쿼리
    subquery = Subquery(
        this=union_query,
        alias=exp.Identifier(this=unpivot_info.unpivot_alias)
    )
    lateral = exp.Lateral(this=subquery)
    lateral_join = Join(
        this=lateral,
        side="LEFT",
        on=Boolean(this=True)
    )

    # 중첩 UNPIVOT 처리
    for nested_info in unpivot_info.nested_unpivots:
        nested_join = _convert_unpivot_node(nested_info.original_table, nested_info)
        lateral_join.args.setdefault("joins", []).append(nested_join)

    # 원본 테이블에 LATERAL JOIN 추가
    original_table.args["joins"] = original_table.args.get("joins", []) + [lateral_join]

    # 기존 pivots 제거
    if "pivots" in original_table.args:
        del original_table.args["pivots"]

    return original_table


def _apply_table_alias_to_expr(expression: Expression, alias_name: str) -> Expression:
    """
    Expression(함수, 연산, CASE 등) 내부에 Column이 있으면,
    해당 Column에 table=Identifier(this=alias_name)을 설정한다.

    UNPIVOT 대상 expression이 단순 col이 아닐 경우를 처리하기 위함.
    """

    def apply_alias(expr: Expression) -> Expression:
        if isinstance(expr, Column):
            if not expr.args.get("table"):
                expr.set("table", Identifier(this=alias_name))
        return expr

    return expression.transform(apply_alias)