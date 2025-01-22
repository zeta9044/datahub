from typing import List, Optional

from sqlglot import exp, maybe_parse
from sqlglot.expressions import Expression, Select, Table, Join, Pivot, Column, Identifier, TableAlias, Boolean


class UnpivotInfo:
    """
    Represents information related to an unpivot operation in a database query.

    This class is used to encapsulate details about the original table, its alias,
    and the alias used for the unpivoted data, which are required during unpivot
    operations.

    :param original_table: The original table involved in the unpivot operation.
    :type original_table: Table
    :param original_alias: The optional alias name used for the original table.
    :type original_alias: Optional[str]
    :param unpivot_alias: The alias name assigned to the unpivoted data.
    :type unpivot_alias: str
    """

    def __init__(self,
                 original_table: Table,
                 original_alias: Optional[str],
                 unpivot_alias: str):
        self.original_table = original_table
        self.original_alias = original_alias
        self.unpivot_alias = unpivot_alias


def convert_unpivot_to_lateral(ast: Expression) -> Expression:
    """
    :param ast: The input Abstract Syntax Tree (AST) represented as an Expression object.
    :return: A transformed Expression object where the unpivot is converted to a lateral join by modifying the from clause.
    """
    return _transform_from_clause(ast.copy())


def _has_unpivot(node: Expression) -> bool:
    """
    :param node: An instance of the Expression class representing a node in the expression tree.
    :return: A boolean indicating whether the given node contains an "unpivot" argument within its "pivots".
    """
    return bool(node.args.get("pivots") and
                any(p.args.get("unpivot") for p in node.args["pivots"]))


def _get_table_alias(table: Table) -> Optional[str]:
    """
    :param table: Table object from which the alias will be extracted. It should have an attribute 'args' containing alias information, and may have 'alias_or_name' as a fallback.
    :return: The extracted alias name as a string if present, or the alias or name of the table as a fallback. Returns None if neither is available.
    """
    if table.args.get("alias") and isinstance(table.args["alias"], TableAlias):
        return table.args["alias"].args["this"].name
    return table.alias_or_name


def _transform_from_clause(node: Expression) -> Expression:
    """
    Transforms the "from" clause in a SQL expression node. If the given node is a Select and contains
    an "unpivot" operation in its "from" clause, it collects unpivot information, converts the unpivot node,
    and updates column aliases in the select clause accordingly. The transformation is also recursively
    applied to child nodes.

    :param node: The SQL expression node to be transformed, typically of type Expression.
    :return: The transformed SQL expression node with the modified "from" clause and updated child nodes.
    """
    if isinstance(node, Select):
        if node.args.get("from"):
            from_table = node.args["from"].args["this"]
            if _has_unpivot(from_table):
                # Unpivot 정보 수집
                unpivot_info = _collect_unpivot_info(from_table)

                # From 절 변환
                new_from = _convert_unpivot_node(from_table, unpivot_info)
                node.args["from"].args["this"] = new_from

                # Select 절의 컬럼 별칭 업데이트
                _update_select_column_aliases(node, unpivot_info)

    # 재귀적으로 자식 노드들도 변환
    for child in node.iter_expressions():
        _transform_from_clause(child)

    return node


def _collect_unpivot_info(node: Table) -> UnpivotInfo:
    """
    :param node: Table node object containing pivot and unpivot information.
    :return: An instance of UnpivotInfo containing details such as the node, original alias, and unpivot alias.
    """
    original_alias = _get_table_alias(node)

    # Unpivot 별칭 찾기
    pivot = next(p for p in node.args["pivots"] if p.args.get("unpivot"))
    unpivot_alias = pivot.args["alias"].args["this"].name if pivot.args.get("alias") else None

    if not unpivot_alias:
        raise ValueError("Unpivot alias is required")

    return UnpivotInfo(node, original_alias, unpivot_alias)


def _update_select_column_aliases(node: Select, unpivot_info: UnpivotInfo):
    """
    :param node: The `Select` node containing the query information.
    :param unpivot_info: The `UnpivotInfo` object containing information about the unpivot operation.
    :return: None. This function updates the aliases of select and where clause columns in the `node` based on `unpivot_info`.
    """
    for expr in node.args.get("expressions", []):
        if isinstance(expr, Column):
            _update_column_alias(expr, unpivot_info)
        elif hasattr(expr, "this") and isinstance(expr.this, Column):
            _update_column_alias(expr.this, unpivot_info)
        # where 절의 컬럼들도 업데이트
        if node.args.get("where"):
            _update_where_column_aliases(node.args["where"], unpivot_info)


def _update_where_column_aliases(where_node: Expression, unpivot_info: UnpivotInfo):
    """
    :param where_node: The current node in the expression tree being evaluated, potentially representing a column or containing a reference to a column.
    :param unpivot_info: Contextual information about the unpivot operation, used to update column aliases accordingly.
    :return: None
    """
    if isinstance(where_node, Column):
        _update_column_alias(where_node, unpivot_info)
    elif hasattr(where_node, "this"):
        if isinstance(where_node.this, Column):
            _update_column_alias(where_node.this, unpivot_info)
        else:
            _update_where_column_aliases(where_node.this, unpivot_info)
    elif hasattr(where_node, "expression"):
        _update_where_column_aliases(where_node.expression, unpivot_info)


def _update_column_alias(column: Column, unpivot_info: UnpivotInfo):
    """
    :param column: The column object to update. It may include information regarding table aliases and column details.
    :param unpivot_info: The UnpivotInfo object containing details about the unpivoting process, including the unpivot alias and the original alias.
    :return: None. This function modifies the column object in place to update its alias if conditions match.
    """
    if column.args.get("table"):
        current_alias = column.args["table"].name
        if current_alias == unpivot_info.unpivot_alias:
            if unpivot_info.original_alias:
                column.args["table"] = Identifier(this=unpivot_info.original_alias)


def _convert_unpivot_node(node: Table, unpivot_info: UnpivotInfo) -> Expression:
    """
    :param node: The original table node containing information necessary for generating the UNPIVOT query. The node should include details about the source table, pivots, and any specified aliases.
    :param unpivot_info: An object containing metadata required for the unpivot operation, such as original table alias, unpivot target alias, and other relevant fields.
    :return: An expression representing the transformed table structure with a LATERAL join to the UNPIVOT subquery included.
    """
    # 원본 테이블 구성 (기존 별칭 유지)
    original_table = Table(
        this=node.args["this"],
        db=node.args.get("db"),
        catalog=node.args.get("catalog")
    )
    if unpivot_info.original_alias:
        original_table.args["alias"] = TableAlias(this=Identifier(this=unpivot_info.original_alias))

    # UNPIVOT 정보 추출
    pivot: Pivot = next(p for p in node.args["pivots"] if p.args.get("unpivot"))

    # UNPIVOT의 value column과 name column 이름 추출
    value_column = pivot.args["expressions"][0]  # USER_CNT
    field = pivot.args["field"]  # IN (...) 부분의 정보
    name_column = field.args["this"]  # USER_AGE

    # UNPIVOT 대상 컬럼들
    unpivot_columns = field.args["expressions"]  # IN 절 안의 컬럼들

    # UNION ALL 서브쿼리 구성
    union_parts: List[Select] = []

    for col in unpivot_columns:
        # 각 UNPIVOT 대상 컬럼에 대한 SELECT 구문 생성
        select_expr = Select(
            expressions=[
                # UNPIVOT의 FOR에 지정된 컬럼 (USER_AGE)
                exp.Alias(
                    this=exp.Literal.string(col.name),
                    alias=exp.Identifier(this=name_column.name)
                ),
                # UNPIVOT의 value column (USER_CNT)
                exp.Alias(
                    this=Column(
                        this=exp.Identifier(this=col.name),
                        table=exp.Identifier(this=original_table.alias)
                    ),
                    alias=exp.Identifier(this=value_column.name)
                )
            ]
        )
        union_parts.append(select_expr)

    # UNION ALL로 모든 SELECT 구문 결합
    union_query = union_parts[0]
    for part in union_parts[1:]:
        union_query = exp.Union(
            this=union_query,
            expression=part,
            distinct=False
        )
    subquery = exp.Subquery(
        this=union_query,
        alias=exp.Identifier(this=unpivot_info.unpivot_alias)
    )
    # LATERAL 표현식 생성
    lateral = exp.Lateral(this=subquery)

    # LATERAL LEFT JOIN 구성
    lateral_join = Join(
        this=lateral,
        method="LEFT",
        alias=TableAlias(this=Identifier(this=unpivot_info.unpivot_alias)),
        on=Boolean(this=True)
    )

    # 원본 테이블에 LATERAL 조인 추가
    original_table.set("joins", [lateral_join])

    return original_table