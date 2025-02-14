from sqlglot import exp
from sqlglot.optimizer import build_scope

def find_stars_in_expr(expr: exp.Expression, parent_select: exp.Select) -> list:
    """
    Within the given expr,
    recursively collect exp.Star nodes whose ancestor is parent_select.
    (In other words, star nodes that do not extend into inner subqueries, etc.)
    """

    stars = []
    for star in expr.find_all(exp.Star):
        if star.find_ancestor(exp.Select) == parent_select:
            stars.append(star)
    return stars


def merge_mappings(map1: dict, map2: dict) -> dict:
    """
    Merge two mapping dicts ({source_table: set(columns)})
    and return them in the form of {source_table: sorted_list_of_columns}.
    """

    merged = {}
    for table, cols in map1.items():
        merged.setdefault(table, set()).update(cols)
    for table, cols in map2.items():
        merged.setdefault(table, set()).update(cols)
    return {table: sorted(list(cols)) for table, cols in merged.items()}


def resolve_alias_origin(table_node: exp.Expression, alias: str) -> str:
    """
    For the table_node in the scope:
    if it is a base table (exp.Table), return its name;
    if it is a subquery (or CTE), extract the name from the result of the inner SELECT
    (in the case of a single source table).
    If determination is difficult, return the alias as is.
    """

    if isinstance(table_node, exp.Table):
        return table_node.name
    elif isinstance(table_node, exp.Subquery):
        # Recursively analyze inner SELECT statements.
        sub_mapping = get_expanded_columns_map(table_node.this)
        if len(sub_mapping) == 1:
            return list(sub_mapping.keys())[0]
    return alias


def process_select(select: exp.Select) -> dict:
    """
    For a single SELECT statement, use build_scope to expand stars (*)
    and explicit columns into a source (table, column) mapping.

    Return type: {source_table: set(columns)} (sorting handled later in merge_mappings)
    """

    mapping = {}
    scope = build_scope(select)

    # The tables property returns a list of Tables.
    alias_to_table = {}
    for table_node in scope.tables:
        alias = table_node.alias_or_name  # Retrieve the alias or name of the table.
        origin = resolve_alias_origin(table_node, alias)
        alias_to_table[alias.lower()] = origin

    def add_mapping(orig_table: str, orig_column: str):
        if orig_table:
            mapping.setdefault(orig_table, set()).add(orig_column)

    # Target: All expressions within select.expressions (explicit columns, stars, stars within functions/UDTFs, etc.)
    for expr in select.args.get('expressions', []):
        # 1. Handle direct column references
        if isinstance(expr, exp.Column):
            if expr.table:
                for a, t in alias_to_table.items():
                    if expr.table.lower() == a:
                        add_mapping(t, expr.name)
                        break
            else:
                if len(alias_to_table) == 1:
                    add_mapping(list(alias_to_table.values())[0], expr.name)
        elif isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Column):
            col = expr.this
            if col.table:
                for a, t in alias_to_table.items():
                    if col.table.lower() == a:
                        add_mapping(t, col.name)
                        break
            else:
                if len(alias_to_table) == 1:
                    add_mapping(list(alias_to_table.values())[0], col.name)

        # 2. Directly handle top-level stars
        if isinstance(expr, exp.Star):
            qualifier = expr.this
            if qualifier:
                alias = qualifier.name.lower()
                orig_table = alias_to_table.get(alias)
                if orig_table:
                    # Add column references within this SELECT scope that match the alias.
                    for col in select.find_all(exp.Column):
                        if col.table and col.table.lower() == alias:
                            add_mapping(orig_table, col.name)
            else:
                for col in select.find_all(exp.Column):
                    if col.table:
                        for a, t in alias_to_table.items():
                            if col.table.lower() == a:
                                add_mapping(t, col.name)
                    else:
                        if len(alias_to_table) == 1:
                            add_mapping(list(alias_to_table.values())[0], col.name)

        # 3. Handle stars included in inner expressions, such as functions or UDTFs.
        inner_stars = find_stars_in_expr(expr, select)
        for star in inner_stars:
            qualifier = star.this
            if qualifier:
                alias = qualifier.name.lower()
                orig_table = alias_to_table.get(alias)
                if orig_table:
                    for col in select.find_all(exp.Column):
                        if col.table and col.table.lower() == alias:
                            add_mapping(orig_table, col.name)
            else:
                for col in select.find_all(exp.Column):
                    if col.table:
                        for a, t in alias_to_table.items():
                            if col.table.lower() == a:
                                add_mapping(t, col.name)
                    else:
                        if len(alias_to_table) == 1:
                            add_mapping(list(alias_to_table.values())[0], col.name)

    return mapping


def get_expanded_columns_map(expr: exp.Expression) -> dict:
    """
    If the parsed SQL AST is a SELECT statement, UNION, or composite structure,
    it performs star (*) expansion for each SELECT statement (or each branch of the UNION)
    and finally returns a mapping dict in the form of {source_table: [source_column, ...], ...}.

    - If it is a UNION, the left and right branches are processed recursively.
    - SELECT statements within subqueries or CTEs are also processed recursively.
    """

    mapping = {}

    if isinstance(expr, exp.Union):
        left = expr.args.get("this")
        right = expr.args.get("expression")
        mapping = merge_mappings(get_expanded_columns_map(left), get_expanded_columns_map(right))
    elif isinstance(expr, exp.Select):
        mapping = process_select(expr)
        # Recursion: Process subqueries (or SELECT statements within CTEs) within the current SELECT as well
        for sub_select in expr.find_all(exp.Select):
            if sub_select is not expr:
                sub_mapping = get_expanded_columns_map(sub_select)
                mapping = merge_mappings(mapping, sub_mapping)
    else:
        # Handle all cases where a SELECT is included within other nodes
        for select in expr.find_all(exp.Select):
            mapping = merge_mappings(mapping, get_expanded_columns_map(select))

    return mapping


def get_expanded_table_columns(expr: exp.Expression) -> list[tuple[exp.Table, list[exp.Column]]]:
    """
    Parses and expands the given SQL expression to extract table and column information, including schema and catalog details.

    :param expr: The SQL expression that may contain tables and associated columns.
    :type expr: exp.Expression
    :return: A list of tuples, where each tuple contains a Table object (with schema/catalog metadata) and a list of associated Column objects.
    :rtype: list[tuple[exp.Table, list[exp.Column]]]
    """
    raw_mapping = get_expanded_columns_map(expr)
    result = []

    # Finding table information from the original query
    table_info = {}
    for table in expr.find_all(exp.Table):
        if isinstance(table.this, exp.Identifier):
            qualified_name = table.name
            if qualified_name not in table_info:
                table_info[qualified_name] = {
                    'db': table.text('db'),
                    'catalog': table.text('catalog')
                }

    for table_name, column_names in raw_mapping.items():
        # Create a Table object (including db and schema information)
        table_parts = table_name.split('.')
        if len(table_parts) == 3:  # catalog.db.table
            catalog, db, table_name = table_parts
            table = exp.Table(
                this=exp.Identifier(this=table_name),
                db=exp.Identifier(this=db),
                catalog=exp.Identifier(this=catalog)
            )
        elif len(table_parts) == 2:  # db.table
            db, table_name = table_parts
            table = exp.Table(
                this=exp.Identifier(this=table_name),
                db=exp.Identifier(this=db)
            )
        else:  # table only
            table = exp.Table(this=exp.Identifier(this=table_name))
            # Check additional information in table_info
            if table_name in table_info:
                info = table_info[table_name]
                if info['db']:
                    table.set('db', exp.Identifier(this=info['db']))
                if info['catalog']:
                    table.set('catalog', exp.Identifier(this=info['catalog']))

        # Create a list of Column objects
        columns = [
            exp.Column(
                this=exp.Identifier(this=col_name),
                table=exp.Identifier(this=table_name),
                db=exp.Identifier(this=table.text('db')) if table.text('db') else None,
                catalog=exp.Identifier(this=table.text('catalog')) if table.text('catalog') else None
            )
            for col_name in column_names
        ]

        result.append((table, columns))

    return result


def get_columns_by_table(target_table: exp.Table, infer_table_columns: list[tuple[exp.Table, list[exp.Column]]]) -> \
        list[exp.Column] | None:
    """
    :param target_table: The table for which columns need to be retrieved.
    :param infer_table_columns: A list of tuples, where each tuple contains a table and its corresponding list of columns.
    :return: The list of columns for the specified target_table if found, else None.
    """
    for table, columns in infer_table_columns:
        if table == target_table:
            return columns
    return None
