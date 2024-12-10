import typing as t

import logging
import sqlglot.lineage
from sqlglot import Schema, exp, maybe_parse
from sqlglot.errors import SqlglotError
from sqlglot.lineage import Node, to_node
from sqlglot.optimizer import Scope, build_scope, find_all_in_scope, qualify
from sqlglot.dialects.dialect import DialectType
from sqlglot.optimizer.scope import ScopeType

logger = logging.getLogger("sqlglot")

# 기존 함수를 백업
original_lineage = sqlglot.lineage.lineage
original_to_node = sqlglot.lineage.to_node

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

def patched_to_node(
        column: str | int,
        scope: Scope,
        dialect: DialectType,
        scope_name: t.Optional[str] = None,
        upstream: t.Optional[Node] = None,
        source_name: t.Optional[str] = None,
        reference_node_name: t.Optional[str] = None,
        trim_selects: bool = True,
) -> Node:
    # Find the specific select clause that is the source of the column we want.
    # This can either be a specific, named select or a generic `*` clause.
    select = (
        scope.expression.selects[column]
        if isinstance(column, int)
        else next(
            (select for select in scope.expression.selects if select.alias_or_name == column),
            exp.Star() if scope.expression.is_star else scope.expression,
        )
    )

    if isinstance(scope.expression, exp.Subquery):
        for source in scope.subquery_scopes:
            return to_node(
                column,
                scope=source,
                dialect=dialect,
                upstream=upstream,
                source_name=source_name,
                reference_node_name=reference_node_name,
                trim_selects=trim_selects,
            )
    if isinstance(scope.expression, exp.SetOperation):
        name = type(scope.expression).__name__.upper()
        upstream = upstream or Node(name=name, source=scope.expression, expression=select)

        index = (
            column
            if isinstance(column, int)
            else next(
                (
                    i
                    for i, select in enumerate(scope.expression.selects)
                    if select.alias_or_name == column or select.is_star
                ),
                -1,  # mypy will not allow a None here, but a negative index should never be returned
            )
        )

        if index == -1:
            raise ValueError(f"Could not find {column} in {scope.expression}")

        for s in scope.union_scopes:
            to_node(
                index,
                scope=s,
                dialect=dialect,
                upstream=upstream,
                source_name=source_name,
                reference_node_name=reference_node_name,
                trim_selects=trim_selects,
            )

        return upstream

    if trim_selects and isinstance(scope.expression, exp.Select):
        # For better ergonomics in our node labels, replace the full select with
        # a version that has only the column we care about.
        #   "x", SELECT x, y FROM foo
        #     => "x", SELECT x FROM foo
        source = t.cast(exp.Expression, scope.expression.select(select, append=False))
    else:
        source = scope.expression

    # Create the node for this step in the lineage chain, and attach it to the previous one.
    node = Node(
        name=f"{scope_name}.{column}" if scope_name else str(column),
        source=source,
        expression=select,
        source_name=source_name or "",
        reference_node_name=reference_node_name or "",
    )

    if upstream:
        upstream.downstream.append(node)

    subquery_scopes = {
        id(subquery_scope.expression): subquery_scope for subquery_scope in scope.subquery_scopes
    }

    for subquery in find_all_in_scope(select, exp.UNWRAPPED_QUERIES):
        subquery_scope = subquery_scopes.get(id(subquery))
        if not subquery_scope:
            logger.warning(f"Unknown subquery scope: {subquery.sql(dialect=dialect)}")
            continue

        for name in subquery.named_selects:
            to_node(
                name,
                scope=subquery_scope,
                dialect=dialect,
                upstream=node,
                trim_selects=trim_selects,
            )

    # if the select is a star add all scope sources as downstreams
    if select.is_star:
        for source in scope.sources.values():
            if isinstance(source, Scope):
                source = source.expression
            node.downstream.append(
                Node(name=select.sql(comments=False), source=source, expression=source)
            )

    # Find all columns that went into creating this one to list their lineage nodes.
    source_columns = set(find_all_in_scope(select, exp.Column))

    # If the source is a UDTF find columns used in the UTDF to generate the table
    if isinstance(source, exp.UDTF):
        source_columns |= set(source.find_all(exp.Column))
        derived_tables = [
            source.expression.parent
            for source in scope.sources.values()
            if isinstance(source, Scope) and source.is_derived_table
        ]
    else:
        derived_tables = scope.derived_tables

    source_names = {
        dt.alias: dt.comments[0].split()[1]
        for dt in derived_tables
        if dt.comments and dt.comments[0].startswith("source: ")
    }

    for c in source_columns:
        table = c.table
        source = scope.sources.get(table)

        if isinstance(c, exp.Column) and c.this and c.table:
            # Column의 name을 dot expression의 마지막 부분으로 변경
            new_name = c.to_dot().alias_or_name
            c.set('this', exp.to_identifier(new_name))

        if isinstance(source, Scope):
            reference_node_name = None
            if source.scope_type == ScopeType.DERIVED_TABLE and table not in source_names:
                reference_node_name = table
            elif source.scope_type == ScopeType.CTE:
                selected_node, _ = scope.selected_sources.get(table, (None, None))
                reference_node_name = selected_node.name if selected_node else None
            # The table itself came from a more specific scope. Recurse into that one using the unaliased column name.
            to_node(
                c.name,
                scope=source,
                dialect=dialect,
                scope_name=table,
                upstream=node,
                source_name=source_names.get(table) or source_name,
                reference_node_name=reference_node_name,
                trim_selects=trim_selects,
            )
        else:
            # The source is not a scope - we've reached the end of the line. At this point, if a source is not found
            # it means this column's lineage is unknown. This can happen if the definition of a source used in a query
            # is not passed into the `sources` map.
            source = source or exp.Placeholder()
            node.downstream.append(
                Node(name=c.sql(comments=False), source=source, expression=source)
            )

    return node

# Monkey patch 적용
def apply_patches():
    # 두 가지 패치 모두 적용
    sqlglot.lineage.lineage = patched_lineage
    sqlglot.lineage.to_node = patched_to_node

# 원래 동작으로 복원하는 함수 (필요한 경우 사용)
def restore_original():
    sqlglot.lineage.lineage = original_lineage
    sqlglot.lineage.to_node = original_to_node