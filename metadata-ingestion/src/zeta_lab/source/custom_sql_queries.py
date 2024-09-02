import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Iterable, List

from pydantic import Field

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance, make_user_urn
from datahub.emitter.sql_parsing_builder import SqlParsingBuilder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sql_queries import SqlQueriesSource, SqlQueriesSourceConfig, QueryEntry as BaseQueryEntry, \
    SqlQueriesSourceReport
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.schema_classes import SqlParsingResultClass
from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult, sqlglot_lineage

class CustomSqlParsingResult(SqlParsingResult):
    custom_keys: Optional[Dict[str, Any]] = None

@dataclass
class CustomQueryEntry(BaseQueryEntry):
    custom_keys: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(cls, entry_dict: dict, *, config: SqlQueriesSourceConfig) -> "CustomQueryEntry":
        base_entry = super().create(entry_dict, config=config)
        return cls(
            query=base_entry.query,
            timestamp=base_entry.timestamp,
            user=base_entry.user,
            operation_type=base_entry.operation_type,
            downstream_tables=base_entry.downstream_tables,
            upstream_tables=base_entry.upstream_tables,
            custom_keys=entry_dict.get("custom_keys", {})
        )

class CustomSqlQueriesSource(SqlQueriesSource):
    def __init__(self, ctx: PipelineContext, config: SqlQueriesSourceConfig):
        if not ctx.graph:
            raise ValueError(
                "SqlQueriesSource needs a datahub_api from which to pull schema metadata"
            )

        self.graph: DataHubGraph = ctx.graph
        self.ctx = ctx
        self.config = config
        self.report = SqlQueriesSourceReport()

        self.builder = SqlParsingBuilder(usage_config=self.config.usage)

        if self.config.use_schema_resolver:
            self.schema_resolver = self.graph.initialize_schema_resolver_from_datahub(
                platform=self.config.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            self.urns = self.schema_resolver.get_urns()
        else:
            self.schema_resolver = self.graph._make_schema_resolver(
                platform=self.config.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            self.urns = None

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        with open(self.config.query_file) as f:
            for line in f:
                try:
                    query_dict = json.loads(line, strict=False)
                    entry = CustomQueryEntry.create(query_dict, config=self.config)
                    yield from self._process_query(entry)
                except Exception as e:
                    self.report.report_warning("process-query", str(e))

    def _process_query(self, entry: CustomQueryEntry) -> Iterable[MetadataWorkUnit]:
        yield from super()._process_query(entry)

        result = sqlglot_lineage(
            sql=entry.query,
            schema_resolver=self.schema_resolver,
            default_db=self.config.default_db,
            default_schema=self.config.default_schema,
        )

        custom_result = CustomSqlParsingResult(**result.dict())
        custom_result.custom_keys = entry.custom_keys

        sql_parse_result_aspect = SqlParsingResultClass(
            query=custom_result.query,
            queryType=custom_result.query_type,
            queryTypeProps=custom_result.query_type_props,
            queryFingerprint=custom_result.query_fingerprint,
            inTables=custom_result.in_tables,
            outTables=custom_result.out_tables,
            columnLineage=custom_result.column_lineage,
            custom_keys=custom_result.custom_keys
        )

        query_urn = f"urn:li:query:{custom_result.query_fingerprint}"

        mce = MetadataChangeEvent(
            proposedSnapshot=DatasetSnapshot(
                urn=query_urn,
                aspects=[sql_parse_result_aspect]
            )
        )
        yield MetadataWorkUnit(id=query_urn, mce=mce)

# Register the custom source
from datahub.ingestion.source.source_registry import source_registry
source_registry.register("custom-sql-queries", CustomSqlQueriesSource)