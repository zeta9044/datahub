import typing as t
import logging
from dataclasses import dataclass
from sqlglot import Schema, exp, maybe_parse
from sqlglot.errors import SqlglotError
from sqlglot.lineage import Node, get_source_from_udtf
from sqlglot.optimizer import Scope, build_scope, find_all_in_scope, qualify
from sqlglot.dialects.dialect import DialectType
from sqlglot.optimizer.scope import ScopeType

logger = logging.getLogger("sqlglot")

@dataclass
class LineageWrapper:
    """
    A wrapper class to safely manage patches to sqlglot's lineage functionality.
    This class stores references to original functions and provides patched versions.
    """
    def __init__(self):
        import sqlglot.lineage
        self.original_to_node = sqlglot.lineage.to_node
        self.original_lineage = sqlglot.lineage.lineage
        self._sqlglot = sqlglot.lineage

    def find_source_columns(self, select_expression: exp.Expression) -> set[exp.Column]:
        """
        Extract source columns from a select expression.

        Args:
            select_expression: The SQL expression to analyze

        Returns:
            set[exp.Column]: Set of source columns found
        """
        source_columns = set()

        def extract_source_column(expr: exp.Expression) -> t.Optional[exp.Column]:
            if isinstance(expr, exp.Alias):
                expr = expr.this

            if isinstance(expr, exp.Dot):
                if isinstance(expr.this, exp.Column):
                    column = expr.this
                    # Extract original table and column info without comparing UNPIVOT alias
                    return exp.Column(
                        this=exp.Identifier(this=expr.expression.name),
                        table=column.table
                    )
            return expr

        # Extract source column from select expression
        source_column = extract_source_column(select_expression)
        if source_column:
            source_columns.add(source_column)
        else:
            source_columns.update(set(find_all_in_scope(select_expression, exp.Column)))

        return source_columns

    def patch_to_node(
            self,
            column: str | int,
            scope: Scope,
            dialect: DialectType,
            scope_name: t.Optional[str] = None,
            upstream: t.Optional[Node] = None,
            source_name: t.Optional[str] = None,
            reference_node_name: t.Optional[str] = None,
            trim_selects: bool = True,
    ) -> Node:
        """
        Patched version of sqlglot's to_node function with enhanced column tracking.
        """
        try:
            # Find the specific select clause
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
                    return self.patch_to_node(
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
                        -1,
                    )
                )

                if index == -1:
                    raise ValueError(f"Could not find {column} in {scope.expression}")

                for s in scope.union_scopes:
                    self.patch_to_node(
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
                source = t.cast(exp.Expression, scope.expression.select(select, append=False))
            else:
                source = scope.expression

            # Create node for this lineage step
            node = Node(
                name=f"{scope_name}.{column}" if scope_name else str(column),
                source=source,
                expression=select,
                source_name=source_name or "",
                reference_node_name=reference_node_name or "",
            )

            if upstream:
                upstream.downstream.append(node)

            # Handle subqueries
            subquery_scopes = {
                id(subquery_scope.expression): subquery_scope
                for subquery_scope in scope.subquery_scopes
            }

            for subquery in find_all_in_scope(select, exp.UNWRAPPED_QUERIES):
                subquery_scope = subquery_scopes.get(id(subquery))
                if not subquery_scope:
                    logger.warning(f"Unknown subquery scope: {subquery.sql(dialect=dialect)}")
                    continue

                for name in subquery.named_selects:
                    self.patch_to_node(
                        name,
                        scope=subquery_scope,
                        dialect=dialect,
                        upstream=node,
                        trim_selects=trim_selects,
                    )

            # Handle star expressions
            if select.is_star:
                for source in scope.sources.values():
                    if isinstance(source, Scope):
                        source = source.expression
                    node.downstream.append(
                        Node(name=select.sql(comments=False), source=source, expression=source)
                    )

            # Find source columns
            source_columns = self.find_source_columns(select)

            # Handle UDTF sources
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

            # Process source columns
            for c in source_columns:
                # table 속성이 없을 수 있으므로 안전하게 접근
                table = getattr(c, 'table', None)
                if table is None:
                    logger.warning(f"Column {c} has no table attribute, skipping.")
                    continue
                table = get_source_from_udtf(scope, table)
                source = scope.sources.get(table)

                if isinstance(source, Scope):
                    reference_node_name = None
                    if source.scope_type == ScopeType.DERIVED_TABLE and table not in source_names:
                        reference_node_name = table
                    elif source.scope_type == ScopeType.CTE:
                        selected_node, _ = scope.selected_sources.get(table, (None, None))
                        reference_node_name = selected_node.name if selected_node else None

                    self.patch_to_node(
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
                    source = source or exp.Placeholder()
                    node.downstream.append(
                        Node(name=c.sql(comments=False), source=source, expression=source)
                    )

            return node

        except Exception as e:
            logger.error(f"Error in patch_to_node: {e}")
            raise

    def patch_lineage(
            self,
            column: str | exp.Column,
            sql: str | exp.Expression,
            schema: t.Optional[t.Dict | Schema] = None,
            sources: t.Optional[t.Mapping[str, str | exp.Query]] = None,
            dialect: DialectType = None,
            scope: t.Optional[Scope] = None,
            trim_selects: bool = True,
            **kwargs,
    ) -> Node:
        """
        Patched version of sqlglot's lineage function with enhanced error handling
        and proper column normalization.
        """
        try:
            expression = maybe_parse(sql, dialect=dialect)
            # column = normalize_identifiers.normalize_identifiers(column, dialect=dialect).name
            assert isinstance(column, str)

            if sources:
                expression = exp.expand(
                    expression,
                    {k: t.cast(exp.Query, maybe_parse(v, dialect=dialect))
                     for k, v in sources.items()},
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

            return self.patch_to_node(column, scope, dialect, trim_selects=trim_selects)

        except Exception as e:
            logger.error(f"Error in patch_lineage: {e}")
            raise

    def apply(self) -> None:
        """Apply the patches to sqlglot"""
        logger.info("Applying sqlglot lineage patches...")
        self._sqlglot.to_node = self.patch_to_node
        self._sqlglot.lineage = self.patch_lineage
        logger.info("Patches applied successfully")

    def restore(self) -> None:
        """Restore original sqlglot functions"""
        logger.info("Restoring original sqlglot functions...")
        self._sqlglot.to_node = self.original_to_node
        self._sqlglot.lineage = self.original_lineage
        logger.info("Original functions restored")