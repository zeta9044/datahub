import itertools
from typing import cast, Mapping, Optional, Dict
from copy import deepcopy

import sqlglot
from sqlglot import exp, maybe_parse
from sqlglot.errors import SqlglotError, OptimizeError
from sqlglot.lineage import Node
from sqlglot.optimizer import Scope, build_scope, find_all_in_scope, qualify
from sqlglot.optimizer.scope import ScopeType
from sqlglot.optimizer.unnest_subqueries import _replace, _other_operand
from sqlglot.dialects.dialect import DialectType

# Helper functions
def _extract_source_column(expr: exp.Expression) -> Optional[exp.Expression]:
    """Safely extracts source column from expression."""
    if not isinstance(expr, exp.Expression):
        raise TypeError(f"Expected exp.Expression, got {type(expr)}")

    if expr is None:
        return None

    try:
        if isinstance(expr, exp.Alias):
            expr = expr.this

        if isinstance(expr, exp.Dot):
            if not hasattr(expr, "expression") or not hasattr(expr.expression, "name"):
                return expr

            if isinstance(expr.this, exp.Column):
                column = expr.this
                return exp.Column(
                    this=exp.Identifier(this=getattr(expr.expression, "name", None)),
                    table=getattr(column, "table", None),
                )
        return expr

    except AttributeError:
        return expr


def _find_table_source(scope: Scope, table: str):
    """Find the source node of a table."""
    if not isinstance(scope, Scope) or not table:
        return None

    # Check for PIVOT/UNPIVOT
    for name, node in scope.references:
        pivots = node.args.get("pivots")
        if pivots:
            for pivot in pivots:
                if pivot.alias == table:
                    return node

    # Regular table
    return scope.sources.get(table)


class SQLGlotLineagePatcher:

    def __init__(self):
        self.original_to_node = sqlglot.lineage.to_node
        self.original_lineage = sqlglot.lineage.lineage
        self._patched = False

    def enhanced_to_node(
            column: str | int,
            scope: Scope,
            dialect: DialectType,
            scope_name: Optional[str] = None,
            upstream: Optional[Node] = None,
            source_name: Optional[str] = None,
            reference_node_name: Optional[str] = None,
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
                return sqlglot.lineage.to_node(
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
                sqlglot.lineage.to_node(
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
            source = cast(exp.Expression, scope.expression.select(select, append=False))
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
                sqlglot.lineage.logger.warning(f"Unknown subquery scope: {subquery.sql(dialect=dialect)}")
                continue

            for name in subquery.named_selects:
                sqlglot.lineage.to_node(
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

        # Process the select expression to extract its source column information,
        # handling any aliases or nested column references
        select = _extract_source_column(select)

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

        pivots = scope.pivots
        pivot = pivots[0] if len(pivots) == 1 and not pivots[0].unpivot else None
        if pivot:
            # For each aggregation function, the pivot creates a new column for each field in category
            # combined with the aggfunc. So the columns parsed have this order: cat_a_value_sum, cat_a,
            # b_value_sum, b. Because of this step wise manner the aggfunc 'sum(value) as value_sum'
            # belongs to the column indices 0, 2, and the aggfunc 'max(price)' without an alias belongs
            # to the column indices 1, 3. Here, only the columns used in the aggregations are of interest
            # in the lineage, so lookup the pivot column name by index and map that with the columns used
            # in the aggregation.
            #
            # Example: PIVOT (SUM(value) AS value_sum, MAX(price)) FOR category IN ('a' AS cat_a, 'b')
            pivot_columns = pivot.args["columns"]
            pivot_aggs_count = len(pivot.expressions)

            pivot_column_mapping = {}
            for i, agg in enumerate(pivot.expressions):
                agg_cols = list(agg.find_all(exp.Column))
                for col_index in range(i, len(pivot_columns), pivot_aggs_count):
                    pivot_column_mapping[pivot_columns[col_index].name] = agg_cols

        for c in source_columns:
            table = c.table
            source = _find_table_source(scope, table)

            if isinstance(source, Scope):
                reference_node_name = None
                if source.scope_type == ScopeType.DERIVED_TABLE and table not in source_names:
                    reference_node_name = table
                elif source.scope_type == ScopeType.CTE:
                    selected_node, _ = scope.selected_sources.get(table, (None, None))
                    reference_node_name = selected_node.name if selected_node else None

                # The table itself came from a more specific scope. Recurse into that one using the unaliased column name.
                sqlglot.lineage.to_node(
                    c.name,
                    scope=source,
                    dialect=dialect,
                    scope_name=table,
                    upstream=node,
                    source_name=source_names.get(table) or source_name,
                    reference_node_name=reference_node_name,
                    trim_selects=trim_selects,
                )
            elif pivot and pivot.alias_or_name == c.table:
                downstream_columns = []

                column_name = c.name
                if any(column_name == pivot_column.name for pivot_column in pivot_columns):
                    downstream_columns.extend(pivot_column_mapping[column_name])
                else:
                    # The column is not in the pivot, so it must be an implicit column of the
                    # pivoted source -- adapt column to be from the implicit pivoted source.
                    downstream_columns.append(exp.column(c.this, table=pivot.parent.this))

                for downstream_column in downstream_columns:
                    table = downstream_column.table
                    source = scope.sources.get(table)
                    if isinstance(source, Scope):
                        sqlglot.lineage.to_node(
                            downstream_column.name,
                            scope=source,
                            scope_name=table,
                            dialect=dialect,
                            upstream=node,
                            source_name=source_names.get(table) or source_name,
                            reference_node_name=reference_node_name,
                            trim_selects=trim_selects,
                        )
                    else:
                        source = source or exp.Placeholder()
                        node.downstream.append(
                            Node(
                                name=downstream_column.sql(comments=False),
                                source=source,
                                expression=source,
                            )
                        )
            else:
                # The source is not a scope and the column is not in any pivot - we've reached the end
                # of the line. At this point, if a source is not found it means this column's lineage
                # is unknown. This can happen if the definition of a source used in a query is not
                # passed into the `sources` map.
                source = source or exp.Placeholder()
                node.downstream.append(
                    Node(name=c.sql(comments=False), source=source, expression=source)
                )

        return node

    def enhanced_lineage(
            self,
            column: str | exp.Column,
            sql: str | exp.Expression,
            schema: Optional[Dict | sqlglot.Schema] = None,
            sources: Optional[Mapping[str, str | exp.Query]] = None,
            dialect: sqlglot.dialects.dialect.DialectType = None,
            scope: Optional[Scope] = None,
            trim_selects: bool = True,
            **kwargs,
    ) -> Node:

        expression = maybe_parse(sql, dialect=dialect)
        # column = normalize_identifiers.normalize_identifiers(column, dialect=dialect).name
        assert isinstance(column, str)

        if sources:
            expression = exp.expand(
                expression,
                {k: cast(exp.Query, maybe_parse(v, dialect=dialect)) for k, v in sources.items()},
                dialect=dialect,
            )

        if not scope:
            expression = qualify.qualify(
                expression,
                dialect=dialect,
                schema=schema,
                **{"validate_qualify_columns": False, "identify": False, **kwargs},  # type: ignore
            )

            scope = build_scope(expression)

        if not scope:
            raise SqlglotError("Cannot build lineage, sql must be SELECT")

        if not any(select.alias_or_name == column for select in scope.expression.selects):
            raise SqlglotError(f"Cannot find column '{column}' in query.")

        return sqlglot.lineage.to_node(column, scope, dialect, trim_selects=trim_selects)

    def patch(self):
        if self._patched:
            return

        # Store original functions
        self.original_to_node = sqlglot.lineage.to_node
        self.original_lineage = sqlglot.lineage.lineage

        # Replace with enhanced versions
        sqlglot.lineage.to_node = self.enhanced_to_node
        sqlglot.lineage.lineage = self.enhanced_lineage

        self._patched = True

    def unpatch(self):
        if not self._patched:
            return

        sqlglot.lineage.to_node = self.original_to_node
        sqlglot.lineage.lineage = self.original_lineage

        self._patched = False


class SQLGlotDeepcopyPatcher:

    def __init__(self):
        self.original__deepcopy__ = sqlglot.expressions.Expression.__deepcopy__
        self._patched = False

    def enhanced__deepcopy__(self, memo):
        import datahub.utilities.cooperative_timeout
        datahub.utilities.cooperative_timeout.cooperate()

        root = self.__class__()
        stack = [(self, root)]

        while stack:
            node, copy = stack.pop()

            if node.comments is not None:
                copy.comments = deepcopy(node.comments)
            if node._type is not None:
                copy._type = deepcopy(node._type)
            if node._meta is not None:
                copy._meta = deepcopy(node._meta)
            if node._hash is not None:
                copy._hash = node._hash

            for k, vs in node.args.items():
                if hasattr(vs, "parent"):
                    stack.append((vs, vs.__class__()))
                    copy.set(k, stack[-1][-1])
                elif type(vs) is list:
                    copy.args[k] = []

                    for v in vs:
                        if hasattr(v, "parent"):
                            stack.append((v, v.__class__()))
                            copy.append(k, stack[-1][-1])
                        else:
                            copy.append(k, v)
                else:
                    copy.args[k] = vs

        return root

    def patch(self):
        if self._patched:
            return

        # Store original functions
        self.original__deepcopy__ = sqlglot.expressions.Expression.__deepcopy__

        # Replace with enhanced versions
        sqlglot.expressions.Expression.__deepcopy__ = self.enhanced__deepcopy__

        self._patched = True

    def unpatch(self):
        if not self._patched:
            return

        sqlglot.expressions.Expression.__deepcopy__ = self.original__deepcopy__

        self._patched = False


class SQLGlotScopeTraversePatcher:

    def __init__(self):
        self.original_traverse = sqlglot.optimizer.scope.Scope.traverse
        self._patched = False

    def enhanced_traverse(self):
        """
        Traverse the scope tree from this node.

        Yields:
            Scope: scope instances in depth-first-search post-order
        """
        stack = [self]
        seen_scopes = set()
        result = []
        while stack:
            scope = stack.pop()

            # Scopes aren't hashable, so we use id(scope) instead.
            if id(scope) in seen_scopes:
                raise OptimizeError(f"Scope {scope} has a circular scope dependency")
            seen_scopes.add(id(scope))

            result.append(scope)
            stack.extend(
                itertools.chain(
                    scope.cte_scopes,
                    scope.union_scopes,
                    scope.table_scopes,
                    scope.subquery_scopes,
                )
            )

        yield from reversed(result)

    def patch(self):
        if self._patched:
            return

        # Store original functions
        self.original_traverse = sqlglot.optimizer.scope.Scope.traverse

        # Replace with enhanced versions
        sqlglot.optimizer.scope.Scope.traverse = self.enhanced_traverse

        self._patched = True

    def unpatch(self):
        if not self._patched:
            return

        sqlglot.optimizer.scope.Scope.traverse = self.original_traverse

        self._patched = False


class SQLGlotUnnestSubqueryPatcher:

    def __init__(self):
        self.original_decorrelate = sqlglot.optimizer.unnest_subqueries.decorrelate
        self._patched = False

    def enhenced_decorrelate(select, parent_select, external_columns, next_alias_name):
        where = select.args.get("where")

        if not where or where.find(exp.Or) or select.find(exp.Limit, exp.Offset):
            return

        table_alias = next_alias_name()
        keys = []

        # for all external columns in the where statement, find the relevant predicate
        # keys to convert it into a join
        for column in external_columns:
            if column.find_ancestor(exp.Where) is not where:
                return

            predicate = column.find_ancestor(exp.Predicate)

            if not predicate or predicate.find_ancestor(exp.Where) is not where:
                return

            if isinstance(predicate, exp.Binary):
                key = (
                    predicate.right
                    if any(node is column for node in predicate.left.walk())
                    else predicate.left
                )
            else:
                return

            keys.append((key, column, predicate))

        if not any(isinstance(predicate, exp.EQ) for *_, predicate in keys):
            return

        is_subquery_projection = any(
            node is select.parent
            for node in map(lambda s: s.unalias(), parent_select.selects)
            if isinstance(node, exp.Subquery)
        )

        value = select.selects[0]
        key_aliases = {}
        group_by = []

        for key, _, predicate in keys:
            # if we filter on the value of the subquery, it needs to be unique
            if key == value.this:
                key_aliases[key] = value.alias
                group_by.append(key)
            else:
                if key not in key_aliases:
                    key_aliases[key] = next_alias_name()
                # all predicates that are equalities must also be in the unique
                # so that we don't do a many to many join
                if isinstance(predicate, exp.EQ) and key not in group_by:
                    group_by.append(key)

        parent_predicate = select.find_ancestor(exp.Predicate)

        # if the value of the subquery is not an agg or a key, we need to collect it into an array
        # so that it can be grouped. For subquery projections, we use a MAX aggregation instead.
        agg_func = exp.Max if is_subquery_projection else exp.ArrayAgg
        if not value.find(exp.AggFunc) and value.this not in group_by:
            select.select(
                exp.alias_(agg_func(this=value.this), value.alias, quoted=False),
                append=False,
                copy=False,
            )

        # exists queries should not have any selects as it only checks if there are any rows
        # all selects will be added by the optimizer and only used for join keys
        if isinstance(parent_predicate, exp.Exists):
            select.args["expressions"] = []

        for key, alias in key_aliases.items():
            if key in group_by:
                # add all keys to the projections of the subquery
                # so that we can use it as a join key
                if isinstance(parent_predicate, exp.Exists) or key != value.this:
                    select.select(f"{key} AS {alias}", copy=False)
            else:
                select.select(exp.alias_(agg_func(this=key.copy()), alias, quoted=False), copy=False)

        alias = exp.column(value.alias, table_alias)
        other = _other_operand(parent_predicate)
        op_type = type(parent_predicate.parent) if parent_predicate else None

        if isinstance(parent_predicate, exp.Exists):
            alias = exp.column(list(key_aliases.values())[0], table_alias)
            parent_predicate = _replace(parent_predicate, f"NOT {alias} IS NULL")
        elif isinstance(parent_predicate, exp.All):
            assert issubclass(op_type, exp.Binary)
            predicate = op_type(this=other, expression=exp.column("_x"))
            parent_predicate = _replace(
                parent_predicate.parent, f"ARRAY_ALL({alias}, _x -> {predicate})"
            )
        elif isinstance(parent_predicate, exp.Any):
            assert issubclass(op_type, exp.Binary)
            if value.this in group_by:
                predicate = op_type(this=other, expression=alias)
                parent_predicate = _replace(parent_predicate.parent, predicate)
            else:
                predicate = op_type(this=other, expression=exp.column("_x"))
                parent_predicate = _replace(parent_predicate, f"ARRAY_ANY({alias}, _x -> {predicate})")
        elif isinstance(parent_predicate, exp.In):
            if value.this in group_by:
                parent_predicate = _replace(parent_predicate, f"{other} = {alias}")
            else:
                parent_predicate = _replace(
                    parent_predicate,
                    f"ARRAY_ANY({alias}, _x -> _x = {parent_predicate.this})",
                )
        else:
            if is_subquery_projection and select.parent.alias:
                alias = exp.alias_(alias, select.parent.alias)

            # COUNT always returns 0 on empty datasets, so we need take that into consideration here
            # by transforming all counts into 0 and using that as the coalesced value
            if value.find(exp.Count):

                def remove_aggs(node):
                    if isinstance(node, exp.Count):
                        return exp.Literal.number(0)
                    elif isinstance(node, exp.AggFunc):
                        return exp.null()
                    return node

                alias = exp.Coalesce(this=alias, expressions=[value.this.transform(remove_aggs)])

            select.parent.replace(alias)

        for key, column, predicate in keys:
            predicate.replace(exp.true())
            nested = exp.column(key_aliases[key], table_alias)

            if is_subquery_projection:
                key.replace(nested)
                if not isinstance(predicate, exp.EQ):
                    parent_select.where(predicate, copy=False)
                continue

            if key in group_by:
                key.replace(nested)
            elif isinstance(predicate, exp.EQ):
                if parent_predicate:
                    parent_predicate = _replace(
                        parent_predicate,
                        f"({parent_predicate} AND ARRAY_CONTAINS({nested}, {column}))",
                    )
            else:
                key.replace(exp.to_identifier("_x"))
                if parent_predicate:
                    parent_predicate = _replace(
                        parent_predicate,
                        f"({parent_predicate} AND ARRAY_ANY({nested}, _x -> {predicate}))",
                    )

        parent_select.join(
            select.group_by(*group_by, copy=False),
            on=[predicate for *_, predicate in keys if isinstance(predicate, exp.EQ)],
            join_type="LEFT",
            join_alias=table_alias,
            copy=False,
        )

    def patch(self):
        if self._patched:
            return

        # Store original functions
        self.original_decorrelate = sqlglot.optimizer.unnest_subqueries.decorrelate

        # Replace with enhanced versions
        sqlglot.optimizer.unnest_subqueries.decorrelate = self.enhenced_decorrelate

        self._patched = True

    def unpatch(self):
        if not self._patched:
            return

        sqlglot.optimizer.unnest_subqueries.decorrelate = self.original_decorrelate

        self._patched = False


# Create a global instance
_lineage_patcher = SQLGlotLineagePatcher()
_deepcopy_patcher = SQLGlotDeepcopyPatcher()
_scope_traverse_patcher = SQLGlotScopeTraversePatcher()
_unnest_subquery_patcher = SQLGlotUnnestSubqueryPatcher()


def apply_patches():
    """Apply all sqlglot patches"""
    _deepcopy_patcher.patch()
    _scope_traverse_patcher.patch()
    _unnest_subquery_patcher.patch()
    _lineage_patcher.patch()


def remove_patches():
    """Remove all sqlglot patches"""
    _deepcopy_patcher.unpatch()
    _scope_traverse_patcher.unpatch()
    _unnest_subquery_patcher.unpatch()
    _lineage_patcher.unpatch()
