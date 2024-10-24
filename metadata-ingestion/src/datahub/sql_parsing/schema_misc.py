from typing import Tuple


def clean_stage_name(table_name: str, platform: str) -> str:
    """
    Clean stage name by removing path conditions only for Snowflake platform.
    Example: @A.B.table_a_stage/search_dt='2024-12-25' -> @A.B.table_a_stage (only for Snowflake)

    Args:
    - table_name: The raw table name which may include path conditions.
    - platform: The platform being used (e.g., 'snowflake').

    Returns:
    - A cleaned table name without path conditions if platform is Snowflake, otherwise returns the original table name.
    """
    if platform.lower() == 'snowflake' and '/' in table_name:
        table_name = table_name.split('/')[0]
    return table_name


def normalize_table_name(table_name: str, default_schema: str, default_catalog: str, platform: str) -> Tuple[
    str, str, str]:
    """
    Normalize table name to (catalog, schema, table) format, and clean stage names only if the platform is Snowflake.

    Args:
    - table_name: The raw table name which may include catalog, schema, and table.
    - default_catalog: The default catalog to use if none is provided.
    - default_schema: The default schema to use if none is provided.
    - platform: The platform being used (e.g., 'snowflake').

    Returns:
    - A tuple (catalog, schema, table)
    """
    # Clean stage name only for Snowflake
    table_name = clean_stage_name(table_name, platform)

    # Strip @ only if platform is Snowflake
    if platform.lower() == 'snowflake' and table_name.startswith('@'):
        table_name = table_name.strip('@')

    parts = table_name.split('.')

    if len(parts) == 1:
        return default_catalog, default_schema, parts[0]
    elif len(parts) == 2:
        return default_catalog, parts[0], parts[1]
    elif len(parts) == 3:
        return parts[0], parts[1], parts[2]
    else:
        raise ValueError(f"Invalid table name format: {table_name}")
