import json
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Iterable, List

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql_queries import SqlQueriesSource, SqlQueriesSourceConfig, QueryEntry as BaseQueryEntry
from datahub.metadata.com.linkedin.pegasus2avro.metadata.aspect import SqlParsingResult
from datahub.metadata.com.linkedin.pegasus2avro.metadata.aspect import ColumnLineageInfo
from datahub.metadata.schema_classes import ChangeTypeClass
from datahub.sql_parsing._models import _TableName
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
        logger.info(f"Initialized CustomSqlQueriesSource with config: {config}")

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

    def _extract_table_name_from_urn(self, urn: str) -> TableNameClass:
        # Extract the relevant parts from the URN
        parts = urn.split(',')
        if len(parts) < 2:
            raise ValueError(f"Invalid URN format: {urn}")

        full_name = parts[1].split(')')[0]  # Remove the closing parenthesis and anything after
        name_parts = full_name.split('.')

        if len(name_parts) == 3:
            return TableNameClass(database=name_parts[0], db_schema=name_parts[1], table=name_parts[2])
        elif len(name_parts) == 2:
            return TableNameClass(db_schema=name_parts[0], table=name_parts[1])
        else:
            return TableNameClass(table=full_name)

    def _get_full_dataset_urn(self, table_reference: Union[str, TableNameClass]) -> str:
        if isinstance(table_reference, str) and table_reference.startswith('urn:li:dataset:'):
            # If it's a URN, extract the table name and use it to get the correct URN
            table_name = self._extract_table_name_from_urn(table_reference)
            return self.schema_resolver.get_urn_for_table(table_name)
        elif isinstance(table_reference, str):
            # If it's a string but not a URN, assume it's a table name
            return self.schema_resolver.get_urn_for_table(TableNameClass(table=table_reference))
        else:
            # If it's already a TableNameClass, use it directly
            return self.schema_resolver.get_urn_for_table(table_reference)

    def _process_query(self, entry: CustomQueryEntry) -> Iterable[MetadataWorkUnit]:
        try:
            logger.info(f"Processing query: {entry.query[:100]}...")  # Log first 100 chars of the query

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
                    downstream_urn = self._get_full_dataset_urn(cl.downstream.table)
                    upstream_urns = [self._get_full_dataset_urn(uc.table) for uc in cl.upstreams]
                    column_lineage.append(ColumnLineageInfo(
                        downstreamColumn=f"{downstream_urn}.{cl.downstream.column}",
                        upstreamColumns=[f"{uc_urn}.{uc.column}" for uc_urn, uc in zip(upstream_urns, cl.upstreams)]
                    ))

            in_tables = [self._get_full_dataset_urn(table) for table in custom_result.in_tables]
            out_tables = [self._get_full_dataset_urn(table) for table in custom_result.out_tables]

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
                # If there's no output table, create a virtual dataset
                dataset_name = f"query_{custom_result.query_fingerprint}"
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
