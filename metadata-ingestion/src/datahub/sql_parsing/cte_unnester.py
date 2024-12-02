from typing import Dict, Tuple

import sqlglot
from sqlglot.expressions import With, Select, CTE, Table, Subquery, Expression


class CTEUnnester:
    def __init__(self):
        self.cte_definitions: Dict[str, Tuple[Expression, str]] = {}  # (expression, original_alias)

    def _collect_ctes(self, expression: Expression) -> None:
        """Recursively collect all CTE definitions with their original aliases."""
        if isinstance(expression, With):
            for exp in expression.expressions:
                if isinstance(exp, CTE):
                    processed_cte = self._replace_cte_references(exp.this)
                    # Store both the CTE definition and its original alias
                    self.cte_definitions[exp.alias] = (processed_cte, exp.alias)

        for key, value in expression.args.items():
            if isinstance(value, Expression):
                self._collect_ctes(value)
            elif isinstance(value, (list, tuple)):
                for item in value:
                    if isinstance(item, Expression):
                        self._collect_ctes(item)

    def _replace_cte_references(self, expression: Expression) -> Expression:
        """Replace CTE references with their definitions recursively."""
        if isinstance(expression, Select) and expression.args.get("with"):
            self._collect_ctes(expression.args["with"])
            expression.args["with"] = None

        if isinstance(expression, Table):
            if expression.name in self.cte_definitions:
                cte_def, original_alias = self.cte_definitions[expression.name]
                # Use the table's alias if provided, otherwise use the original CTE name
                return Subquery(this=cte_def).as_(original_alias)
            return expression

        new_args = {}
        for key, value in expression.args.items():
            if isinstance(value, (list, tuple)):
                new_args[key] = [
                    self._replace_cte_references(item)
                    if isinstance(item, Expression) else item
                    for item in value
                ]
            elif isinstance(value, Expression):
                new_args[key] = self._replace_cte_references(value)
            else:
                new_args[key] = value

        expression.args = new_args
        return expression

    def unnest_ctes(self, sql: str) -> str:
        """Convert a SQL query with CTEs to one without CTEs."""
        self.cte_definitions.clear()
        parsed = sqlglot.parse_one(sql)
        return self.unnest_ctes_from_expression(parsed).sql()

    def unnest_ctes_from_expression(self, expression: Expression) -> Expression:
        """Convert a SQL Expression with CTEs to one without CTEs."""
        self.cte_definitions.clear()

        if isinstance(expression, Select) and expression.args.get("with"):
            self._collect_ctes(expression.args["with"])
            expression.args["with"] = None

        return self._replace_cte_references(expression)

def transform_sql(sql: str) -> str:
    """Transform SQL string by unnesting CTEs."""
    unnester = CTEUnnester()
    return unnester.unnest_ctes(sql)

def transform_expression(expression: Expression) -> Expression:
    """Transform SQL Expression by unnesting CTEs."""
    unnester = CTEUnnester()
    return unnester.unnest_ctes_from_expression(expression)

# Test cases
def run_tests():
    test_cases = [
        # Test case with alias preservation
        """
        WITH temp AS (
            SELECT id, name FROM users WHERE age > 25
        )
        SELECT * FROM temp t1
        """,

        # Test case with multiple CTEs and aliases
        """
        WITH 
        cte1 AS (
            SELECT id, name FROM users WHERE age > 25
        ),
        cte2 AS (
            SELECT id, count(*) as cnt 
            FROM orders 
            GROUP BY id
        )
        SELECT c1.*, c2.cnt 
        FROM cte1 c1
        JOIN cte2 c2 ON c1.id = c2.id
        """
    ]

    for i, test_sql in enumerate(test_cases, 1):
        print(f"\nTest Case {i}:")
        print(f"Original Query: {test_sql}")
        result = transform_sql(test_sql)
        print(f"Conversion Result: {result}")
        print("-" * 50)

if __name__ == "__main__":
    run_tests()