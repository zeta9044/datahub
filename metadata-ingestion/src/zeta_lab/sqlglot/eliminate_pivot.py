from __future__ import annotations

import typing as t
from sqlglot import expressions as exp
from sqlglot.optimizer.scope import Scope, build_scope

def eliminate_pivot(expression: exp.Expression) -> exp.Expression:
    """
    Rewrite PIVOT/UNPIVOT operations as regular queries.

    When UNPIVOT:
      - Converts to UNION ALL of SELECT statements
    When PIVOT:
      - Leaves as-is (not implemented yet)

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one('''
        ...     SELECT * FROM (SELECT id, q1, q2 FROM sales)
        ...     PIVOT (value FOR quarter IN (q1, q2))
        ... ''')
        >>> eliminate_pivot(expression).sql()
        'SELECT id, "q1" AS quarter, q1 AS value FROM sales UNION ALL SELECT id, "q2" AS quarter, q2 AS value FROM sales'

    Args:
        expression: expression to transform
    Returns:
        transformed expression
    """
    if isinstance(expression, exp.Subquery):
        eliminate_pivot(expression.this)
        return expression

    root = build_scope(expression)
    if not root:
        return expression

    for scope in root.traverse():
        _eliminate(scope)

    return expression


def _eliminate(scope: Scope) -> None:
    """Process a single scope, looking for PIVOT/UNPIVOT operations to eliminate."""
    if not scope.expression:
        return

    def transform(node: exp.Expression) -> t.Optional[exp.Expression]:
        if not isinstance(node, exp.Pivot):
            return None

        if not node.unpivot:  # Only handle UNPIVOT for now
            return None

        if not _can_eliminate_unpivot(node):
            return None

        return _build_union_all(node)

    scope.expression.transform(transform)


def _can_eliminate_unpivot(node: exp.Pivot) -> bool:
    """Check if a PIVOT operation with unpivot=True can be safely eliminated."""
    if not node.unpivot:
        return False

    field = node.args.get("field")  # FOR field IN ...
    expressions = node.args.get("expressions")  # columns to unpivot
    table = node.args.get("this")  # source table

    return all((
        field,
        expressions,
        table,
        isinstance(table, exp.Select)
    ))


def _build_union_all(node: exp.Pivot) -> exp.Expression:
    """Convert an UNPIVOT operation into a UNION ALL of SELECT statements."""
    field = node.args["field"]  # e.g. "quarter"
    expressions = node.args["expressions"]  # columns to unpivot like [q1, q2, q3, q4]
    table = node.args["this"]

    # Get the FROM clause
    from_clause = table.args.get("from")

    # Get columns that aren't being unpivoted
    retain_columns = [
        expr for expr in table.expressions
        if not any(expr.name == col.name for col in expressions)
    ]

    # Build individual SELECT statements
    selects = []
    for col in expressions:
        select = exp.Select(
            expressions=[
                *retain_columns,
                exp.Literal(value=col.alias_or_name, is_string=True).as_(field.name),
                col.as_("value")  # Use generic 'value' if no name specified
            ],
            from_=from_clause
        )
        selects.append(select)

    # Combine with UNION ALL
    if not selects:
        return node

    result = selects[0]
    for select in selects[1:]:
        result = exp.Union(
            this=result,
            expression=select,
            distinct=False
        )

    return result