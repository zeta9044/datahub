import json
import logging
from typing import Iterable, List, Dict

import duckdb
from dataclasses import dataclass
from pydantic import Field
from sqlglot import parse_one

from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    Source,
    SourceCapability,
    SourceReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit

logger = logging.getLogger(__name__)

@dataclass
class SqlEntry:
    """
    A data class to represent SQL entries with query and customizable key-value pairs.

    Attributes:
        query (str): SQL query as a string.
        custom_keys (Dict[str, str]): Optional dictionary of custom key-value pairs.

    Methods:
        create(cls, entry_dict: dict) -> "SqlEntry":
            Class method to create an instance of SqlEntry from a dictionary.
            The dictionary must include a "query" key for the SQL query string
            and may optionally include a "custom_keys" key for extended query file customizations.
    """
    query: str
    custom_keys: Dict[str, str]

    @classmethod
    def create(
            cls, entry_dict: dict
    ) -> "SqlEntry":
        return cls(
            query=entry_dict["query"],
            custom_keys=entry_dict.get("custom_keys", {}),  # add custom_keys for extended query_file
        )

class SqlFlowSourceConfig(PlatformInstanceConfigMixin, EnvConfigMixin):
    """Configuration for SQL Flow source"""
    query_file: str = Field(description="Path to query file to ingest")
    platform: str = Field(description="The platform for which to generate data (e.g. snowflake)")
    duckdb_path: str = Field(description="duckdb path")

class SqlFlowSourceReport(SourceReport):
    """Report for SQL Flow processing"""
    num_queries_parsed: int = 0
    num_parsing_failures: int = 0

    def compute_stats(self) -> None:
        """Compute statistics for the processing"""
        super().compute_stats()
        self.parsing_failure_rate = (
            f"{self.num_parsing_failures / self.num_queries_parsed:.4f}"
            if self.num_queries_parsed
            else "0"
        )

@platform_name("SQL Flow")
@config_class(SqlFlowSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.LINEAGE_COARSE, "Extracts SQL query flow")
class SqlFlowSource(Source):
    """
    A source that extracts flow information from SQL queries and stores it in AIS0109 table.
    This source processes SQL queries to analyze their structure and execution flow.
    """
    def __init__(self, ctx: PipelineContext, config: SqlFlowSourceConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SqlFlowSourceReport()
        self.duckdb_conn = duckdb.connect(self.config.duckdb_path)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SqlFlowSource":
        config = SqlFlowSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Process queries and yield workunits"""
        logger.info(f"Processing queries from {self.config.query_file}")

        with open(self.config.query_file) as f:
            for line in f:
                try:
                    query_dict = json.loads(line)
                    entry = SqlEntry.create(query_dict)
                    yield from self._process_query(entry)

                    self.report.num_queries_parsed += 1
                    if self.report.num_queries_parsed % 1000 == 0:
                        logger.info(f"Processed {self.report.num_queries_parsed} queries")

                except Exception as e:
                    self.report.num_parsing_failures += 1
                    logger.error(f"Error processing query: {e}")

    def _process_query(self, entry: SqlEntry) -> Iterable[MetadataWorkUnit]:
        """Process a single query and extract its flow information"""
        try:
            query_lines = entry.query.splitlines()

            # Parse the query
            parsed = parse_one(entry.query,dialect=self.config.platform)

            # Extract flow information
            flow_entries = self._extract_flow_entries(
                parsed,
                query_lines,
                {
                    'prj_id': entry.custom_keys.get('prj_id', ''),
                    'file_id': entry.custom_keys.get('file_id', '0'),
                    'sql_id': entry.custom_keys.get('sql_id', '0'),
                    'obj_id': entry.custom_keys.get('obj_id', '0'),
                    'func_id': entry.custom_keys.get('func_id', '0'),
                }
            )

            # Store in DuckDB
            self._store_flow_entries(flow_entries)

        except Exception as e:
            logger.error(f"Error processing query: {e}")
            raise

        return []

    def _extract_flow_entries(self, parsed_query, query_lines: List[str], query_info: dict) -> List[dict]:
        """Extract flow information from the parsed query"""
        flow_entries = []
        column_counter = 0

        def get_node_line_no(node):
            """Find the line number for a given node"""
            node_text = str(node)
            for i, line in enumerate(query_lines, 1):
                if node_text.strip() in line:
                    return i
            return 0

        def process_node(node, depth=0, parent_flow_id=None):
            """Process node recursively and build flow entries"""
            nonlocal flow_entries, column_counter
            flow_id = len(flow_entries) + 1

            # Handle column nodes specially
            if hasattr(node, 'this') and node.this == 'column':
                column_counter += 1
                column_no = column_counter
            else:
                column_no = 0

            entry = {
                'prj_id': query_info['prj_id'],
                'file_id': int(query_info['file_id']),
                'sql_id': int(query_info['sql_id']),
                'flow_id': flow_id,
                'obj_id': int(query_info['obj_id']),
                'func_id': int(query_info['func_id']),
                'flow_src': type(node).__name__,
                'line_no': get_node_line_no(node),
                'column_no': column_no,
                'flow_depth': depth,
                'rel_flow_id': parent_flow_id if parent_flow_id else -1,
                'sub_sql_id': -1,
                'sql_grp': 0
            }
            flow_entries.append(entry)

            # Process child nodes
            for child in node.args.values():
                if hasattr(child, 'args'):
                    process_node(child, depth + 1, flow_id)
                elif isinstance(child, list):
                    for item in child:
                        if hasattr(item, 'args'):
                            process_node(item, depth + 1, flow_id)

            return flow_entries

        return process_node(parsed_query)

    def _store_flow_entries(self, flow_entries: List[dict]) -> None:
        """Store flow entries in DuckDB AIS0109 table"""
        try:
            # Ensure table exists
            self.duckdb_conn.execute("""
                CREATE TABLE IF NOT EXISTS ais0109 (
                    prj_id VARCHAR,
                    file_id BIGINT,
                    sql_id BIGINT,
                    flow_id BIGINT,
                    obj_id BIGINT,
                    func_id BIGINT,
                    flow_src VARCHAR,
                    line_no BIGINT,
                    column_no BIGINT,
                    flow_depth BIGINT,
                    rel_flow_id BIGINT,
                    sub_sql_id BIGINT,
                    sql_grp BIGINT
                )
            """)

            # Insert flow entries
            insert_query = """
                INSERT INTO ais0109 VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """

            values = [(
                entry['prj_id'],
                entry['file_id'],
                entry['sql_id'],
                entry['flow_id'],
                entry['obj_id'],
                entry['func_id'],
                entry['flow_src'],
                entry['line_no'],
                entry['column_no'],
                entry['flow_depth'],
                entry['rel_flow_id'],
                entry['sub_sql_id'],
                entry['sql_grp']
            ) for entry in flow_entries]

            self.duckdb_conn.executemany(insert_query, values)

        except Exception as e:
            logger.error(f"Error storing flow entries: {e}")
            raise

    def get_report(self):
        """Return processing report"""
        return self.report

    def close(self):
        """Clean up resources"""
        pass