# src/__init__.py
from datahub.sql_parsing.patch_sqlglot import LineageWrapper

# Initialize the wrapper
lineage_wrapper = LineageWrapper()

# Apply patches
lineage_wrapper.apply()