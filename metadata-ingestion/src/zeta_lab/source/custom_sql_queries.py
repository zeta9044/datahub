import json
import logging
import os
from dataclasses import dataclass
from pydantic import Field
from datetime import datetime, timezone

from datahub.emitter.mce_builder import make_user_urn, make_dataset_urn_with_platform_instance
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult, sqlglot_lineage
from datahub.ingestion.source.sql_queries import SqlQueriesSource, SqlQueriesSourceConfig
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.schema_classes import SqlParsingResultClass
from typing import Dict, Any, Optional, Iterable, List


class CustomSqlParsingResult(SqlParsingResult):
    custom_keys: Optional[Dict[str, Any]] = None

class CustomSqlQueriesSource(SqlQueriesSource):
    def __init__(self, config: SqlQueriesSourceConfig, ctx):
        super().__init__(config, ctx)

    def _process_query(self, entry: "QueryEntry") -> Iterable[MetadataWorkUnit]:
        # 기존의 처리 로직을 유지
        yield from super()._process_query(entry)

        # SqlParsingResult 생성
        result = sqlglot_lineage(
            sql=entry.query,
            schema_resolver=self.schema_resolver,
            default_db=self.config.default_db,
            default_schema=self.config.default_schema,
        )

        # CustomSqlParsingResult로 변환 및 custom_keys 추가
        custom_result = CustomSqlParsingResult(**result.dict())
        custom_result.custom_keys = entry.custom_keys

        # sqlParseResult aspect 생성
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

        # 각 출력 테이블에 대해 새로운 MetadataChangeEvent 생성
        mce = MetadataChangeEvent(
            proposedSnapshot=DatasetSnapshot(
                urn=query_urn,
                aspects=[sql_parse_result_aspect]
            )
        )
        yield MetadataWorkUnit(id=query_urn, mce=mce)


@dataclass
class QueryEntry:
    query: str
    timestamp: Optional[datetime]
    user: Optional[str]
    operation_type: Optional[str]
    downstream_tables: List[str]
    upstream_tables: List[str]
    custom_keys: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def create(
            cls, entry_dict: dict, *, config: SqlQueriesSourceConfig
    ) -> "QueryEntry":
        return cls(
            query=entry_dict["query"],
            timestamp=(
                datetime.fromtimestamp(entry_dict["timestamp"], tz=timezone.utc)
                if "timestamp" in entry_dict
                else None
            ),
            user=make_user_urn(entry_dict["user"]) if "user" in entry_dict else None,
            operation_type=entry_dict.get("operation_type"),
            downstream_tables=[
                make_dataset_urn_with_platform_instance(
                    name=table,
                    platform=config.platform,
                    platform_instance=config.platform_instance,
                    env=config.env,
                )
                for table in entry_dict.get("downstream_tables", [])
            ],
            upstream_tables=[
                make_dataset_urn_with_platform_instance(
                    name=table,
                    platform=config.platform,
                    platform_instance=config.platform_instance,
                    env=config.env,
                )
                for table in entry_dict.get("upstream_tables", [])
            ],
            custom_keys=entry_dict.get("custom_keys", {})
        )

# Register the custom source
from datahub.ingestion.source.source_registry import source_registry
source_registry.register("custom-sql-queries", SqlQueriesSource)