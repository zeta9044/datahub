from typing import Tuple,Optional
import re

def is_external_storage_path(path: str) -> bool:
    """
    Check if the given path is an external storage path.

    Args:
    - path: The path to check

    Returns:
    - True if the path matches external storage patterns, False otherwise
    """
    external_storage_patterns = [
        r'^s3://',           # AWS S3
        r'^gs://',           # Google Cloud Storage
        r'^wasb[s]?://',     # Azure Blob Storage
        r'^abfs[s]?://',     # Azure Data Lake Storage Gen2
        r'^adl://',          # Azure Data Lake Storage Gen1
        r'^hdfs://',         # Hadoop Distributed File System
        r'^oss://',          # Alibaba Cloud OSS
        r'^cos://'           # Tencent Cloud COS
    ]

    return any(re.match(pattern, path.lower()) for pattern in external_storage_patterns)

def clean_stage_name(table_name: str, platform: str) -> str:
    """
    Clean stage name by removing path conditions only for Snowflake platform and non-external storage paths.

    Examples:
    - @A.B.table_a_stage/search_dt='2024-12-25' -> @A.B.table_a_stage (Snowflake stage)
    - s3://your-bucket/citibike -> s3://your-bucket/citibike (External storage - unchanged)

    Args:
    - table_name: The raw table name which may include path conditions
    - platform: The platform being used (e.g., 'snowflake')

    Returns:
    - A cleaned table name without path conditions if it's a Snowflake stage,
      otherwise returns the original table name
    """
    if not table_name or platform.lower() != 'snowflake':
        return table_name

    # If it's an external storage path, return as is
    if is_external_storage_path(table_name):
        return table_name

    # Only clean if it's a Snowflake stage (starts with @) and has path conditions
    if table_name.startswith('@') and '/' in table_name:
        return table_name.split('/')[0]

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
