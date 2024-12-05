import typing as t

import sqlglot.lineage
from sqlglot import Schema, exp, maybe_parse
from sqlglot.errors import SqlglotError
from sqlglot.lineage import Node, to_node
from sqlglot.optimizer import Scope, build_scope, qualify
from sqlglot.dialects.dialect import DialectType

# 기존 함수를 백업
original_lineage = sqlglot.lineage.lineage

def patched_lineage(
        column: str | exp.Column,
        sql: str | exp.Expression,
        schema: t.Optional[t.Dict | Schema] = None,
        sources: t.Optional[t.Mapping[str, str | exp.Query]] = None,
        dialect: DialectType = None,
        scope: t.Optional[Scope] = None,
        trim_selects: bool = True,
        **kwargs,
) -> Node:
    expression = maybe_parse(sql, dialect=dialect)
    # column = normalize_identifiers.normalize_identifiers(column, dialect=dialect).name
    assert isinstance(column, str)

    if sources:
        expression = exp.expand(
            expression,
            {k: t.cast(exp.Query, maybe_parse(v, dialect=dialect)) for k, v in sources.items()},
            dialect=dialect,
        )

    if not scope:
        expression = qualify.qualify(
            expression,
            dialect=dialect,
            schema=schema,
            **{"validate_qualify_columns": False, "identify": False, **kwargs},
        )

        scope = build_scope(expression)

    if not scope:
        raise SqlglotError("Cannot build lineage, sql must be SELECT")

    if not any(select.alias_or_name == column for select in scope.expression.selects):
        raise SqlglotError(f"Cannot find column '{column}' in query.")

    return to_node(column, scope, dialect, trim_selects=trim_selects)

# Monkey patch 적용
def apply_patches():
    sqlglot.lineage.lineage = patched_lineage