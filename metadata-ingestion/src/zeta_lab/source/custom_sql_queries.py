import json
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Iterable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql_queries import SqlQueriesSource, SqlQueriesSourceConfig, QueryEntry as BaseQueryEntry
from datahub.metadata.com.linkedin.pegasus2avro.metadata.aspect import ColumnLineageInfo
from datahub.metadata.com.linkedin.pegasus2avro.metadata.aspect import SqlParsingResult
from datahub.metadata.schema_classes import ChangeTypeClass
from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult as SqlglotParsingResult, sqlglot_lineage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CustomSqlParsingResult(SqlglotParsingResult):
    custom_keys: Optional[Dict[str, Any]] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.custom_keys = kwargs.get('custom_keys')


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
    def __init__(self, config: SqlQueriesSourceConfig, ctx):
        super().__init__(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        logger.info(f"Reading queries from file: {self.config.query_file}")
        with open(self.config.query_file) as f:
            for line_num, line in enumerate(f, 1):
                try:
                    query_dict = json.loads(line, strict=False)
                    logger.debug(f"Processing query {line_num}: {query_dict}")
                    entry = CustomQueryEntry.create(query_dict, config=self.config)
                    yield from self._process_query(entry)
                except Exception as e:
                    logger.error(f"Error processing query on line {line_num}: {str(e)}")
                    self.report.report_failure("process-query", f"Error on line {line_num}: {str(e)}")

    def _process_query(self, entry: CustomQueryEntry) -> Iterable[MetadataWorkUnit]:
        try:
            logger.info(f"Processing query: {entry.query[:100]}...")  # Log first 100 chars of the query

            # Validate schema_resolver before use
            if not self.schema_resolver:
                raise ValueError("schema_resolver is not initialized")

            result = sqlglot_lineage(
                sql=entry.query,
                schema_resolver=self.schema_resolver,
                default_db=self.config.default_db,
                default_schema=self.config.default_schema,
            )
            logger.debug(f"SQL parsing result: {result}")

            custom_result = CustomSqlParsingResult(**result.dict(), custom_keys=entry.custom_keys)

            column_lineage = []
            if custom_result.column_lineage:
                for cl in custom_result.column_lineage:
                    downstream_urn = cl.downstream.table
                    upstream_urns = [uc.table for uc in cl.upstreams]
                    column_lineage.append(ColumnLineageInfo(
                        downstreamColumn=f"{downstream_urn}.{cl.downstream.column}",
                        upstreamColumns=[f"{uc_urn}.{uc.column}" for uc_urn, uc in zip(upstream_urns, cl.upstreams)]
                    ))

            in_tables = [table for table in custom_result.in_tables]
            out_tables = [table for table in custom_result.out_tables]

            sql_parse_result_aspect = SqlParsingResult(
                query=entry.query,
                queryType=str(custom_result.query_type),
                queryTypeProps=custom_result.query_type_props,
                queryFingerprint=custom_result.query_fingerprint,
                inTables=in_tables,
                outTables=out_tables,
                columnLineage=column_lineage,
                customKeys={k: str(v) for k, v in custom_result.custom_keys.items()}  # Ensure all values are strings
            )

            # Use the first output table as the entity to attach the aspect to
            if out_tables:
                dataset_urn = out_tables[0]
            else:
                #  If there's no output table, create a virtual dataset for query-lineage
                custom_keys_name = "-".join(map(str, custom_result.custom_keys.values()))
                dataset_name = f"{custom_keys_name}_{custom_result.query_fingerprint}"
                dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:sql-query,{dataset_name},{self.config.env})"

            # Emit SQL parsing result as a custom aspect
            sql_parsing_mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=dataset_urn,
                aspectName="sqlParsingResult",
                aspect=sql_parse_result_aspect,
                changeType=ChangeTypeClass.UPSERT
            )
            yield MetadataWorkUnit(id=f"{dataset_urn}-sqlParsingResult", mcp=sql_parsing_mcp)

        except Exception as e:
            logger.error(f"Error in _process_query: {str(e)}")
            self.report.report_failure("process-query", str(e))


# Register the custom source
from datahub.ingestion.source.source_registry import source_registry

source_registry.register("custom-sql-queries", CustomSqlQueriesSource)
logger.info("Registered custom-sql-queries source")
