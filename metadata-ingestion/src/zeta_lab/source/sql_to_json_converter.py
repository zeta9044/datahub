import os
import json
import logging
from typing import Dict, List, Any
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.configuration.common import ConfigModel
from pydantic import validator

logger = logging.getLogger(__name__)

class SQLToJSONConverterConfig(ConfigModel):
    input_path: str
    output_path: str
    
    @validator("input_path")
    def input_path_must_exist(cls, v):
        if not os.path.exists(v):
            raise ValueError(f"Input path {v} does not exist")
        return v

class SQLToJSONConverter(Source):
    def __init__(self, config: SQLToJSONConverterConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> 'SQLToJSONConverter':
        config = SQLToJSONConverterConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> List[Any]:
        queries = self._read_sql_files()
        json_data = self._convert_to_json(queries)
        self._write_json_file(json_data)
        return []  # This source doesn't produce any workunits

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass

    def _read_sql_files(self) -> List[Dict[str, str]]:
        queries = []
        for root, _, files in os.walk(self.config.input_path):
            for file in files:
                if file.endswith('.sql'):
                    file_path = os.path.join(root, file)
                    with open(file_path, 'r') as f:
                        query_content = f.read()
                    queries.append({
                        "name": os.path.splitext(file)[0],
                        "query": query_content
                    })
        return queries

    def _convert_to_json(self, queries: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
        return {"queries": queries}

    def _write_json_file(self, data: Dict[str, List[Dict[str, str]]]) -> None:
        try:
            with open(self.config.output_path, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"JSON file created at {self.config.output_path}")
        except Exception as e:
            self.report.report_failure("Failed to write output file", f"Error: {str(e)}")
            logger.exception("An error occurred while writing the output file")

# Add this to the source_registry
from datahub.ingestion.source.source_registry import source_registry
source_registry.register("sql_to_json_converter", SQLToJSONConverter)
logger.info("Registered sql_to_json_converter source")
