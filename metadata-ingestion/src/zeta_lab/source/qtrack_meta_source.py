import logging
from typing import Dict, List

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from zeta_lab.metadata.make_metadata import (
    transfer_meta_instance,
    transfer_metadata,
    create_metadata_from_duckdb,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QtrackMetaSourceConfig(ConfigModel):
    pg_dsn: str
    duckdb_path: str = "metadata.db"
    gms_server: str

class QtrackMetaSource(Source):
    config: QtrackMetaSourceConfig
    report: SourceReport

    def __init__(self, config: QtrackMetaSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "QtrackMetaSource":
        config = QtrackMetaSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> List[MetadataWorkUnit]:
        # Step 1: Transfer meta instance
        if not transfer_meta_instance(self.config.pg_dsn, self.config.duckdb_path):
            self.report.report_failure("meta_instance_transfer", "Meta instance transfer failed")
            return []

        # Step 2: Transfer metadata
        if not transfer_metadata(self.config.pg_dsn, self.config.duckdb_path):
            self.report.report_failure("metadata_transfer", "Metadata transfer failed")
            return []

        # Step 3: Create metadata and send to GMS
        if not create_metadata_from_duckdb(self.config.duckdb_path, json_output_path=None, gms_server=self.config.gms_server):
            self.report.report_failure("metadata_creation", "Metadata creation and sending to GMS failed")
            return []

        # Since create_metadata_from_duckdb handles sending to GMS, we don't need to yield WorkUnits
        # However, we need to report success for DataHub to know the source ran successfully
        self.report.report_warning("metadata_ingestion", "Metadata successfully created and sent to GMS")
        return []

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass