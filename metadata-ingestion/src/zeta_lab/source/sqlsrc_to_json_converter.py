import json
import logging
import os
from typing import Dict, List, Any, Iterable

import pandas as pd
from pydantic import validator

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SqlsrcToJSONConverterConfig(ConfigModel):
    input_path: str
    output_path: str

    @validator("input_path")
    def input_path_must_exist(cls, v):
        if not os.path.exists(v):
            raise ValueError(f"Input path {v} does not exist")
        return v

class SqlsrcToJSONConverterReport(SourceReport):
    num_lines_parsed: int = 0
    num_parse_failures: int = 0
    num_queries_processed: int = 0

    def report_line_parsed(self, success: bool = True) -> None:
        self.num_lines_parsed += 1
        if not success:
            self.num_parse_failures += 1

    def report_query_processed(self) -> None:
        self.num_queries_processed += 1

    def compute_stats(self) -> None:
        super().compute_stats()
        self.report.report_info("Lines parsed", f"{self.num_lines_parsed}")
        self.report.report_info("Parse failures", f"{self.num_parse_failures}")
        self.report.report_info("Queries processed", f"{self.num_queries_processed}")
        if self.num_lines_parsed > 0:
            failure_rate = f"{(self.num_parse_failures / self.num_lines_parsed):.2%}"
            self.report.report_info("Failure rate", failure_rate)

class SqlsrcToJSONConverter(Source):
    def __init__(self, config: SqlsrcToJSONConverterConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SqlsrcToJSONConverterReport()

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> 'SqlsrcToJSONConverter':
        config = SqlsrcToJSONConverterConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        try:
            data = self.process_file()
            self._write_json_file(data)
        except Exception as e:
            self.report.report_failure("Failed to process file", f"Error: {str(e)}")
            logger.exception("An error occurred while processing the file")

        return []

    def get_report(self) -> SqlsrcToJSONConverterReport:
        return self.report

    def close(self):
        pass

    def _split_queries(self, query: str) -> List[str]:
        return [q.strip() for q in query.split(';') if q.strip()]

    def process_file(self) -> List[Dict[str, Any]]:
        data = []
        selected_columns = ["prjId", "fileId", "objId", "funcId", "sqlId", "sqlSrc", "sqlSrcOrg"]

        try:
            # Read the file content
            with open(self.config.input_path, 'r', encoding='utf-8') as file:
                content = file.read()
                if not content:  # content is empty
                    raise ValueError(f"The file at {self.config.input_path} is empty.")

            # Define the delimiter for column of line
            delimiter = b'\x07'.decode('utf-8')
            # Split content by '\x05' for line
            lines = content.split('\x05')
            # remove empty line
            lines = [line for line in lines if line]
            total_lines = len(lines)
            # parsed data with columns
            parsed_data = [line.split(delimiter) for line in lines]

            # Read the parsed_data into a DataFrame
            df = pd.DataFrame(parsed_data)

            # Select only the required columns
            df_selected = df.iloc[:, [0, 1, 2, 3, 4, 7, 8]]
            df_selected.columns = selected_columns

            for index, row in df_selected.iterrows():
                try:
                    logger.debug(f"Processing Row {index},{row}")

                    custom_keys = {
                        "prj_id": row['prjId'].strip(),
                        "file_id": row['fileId'].strip(),
                        "obj_id": row['objId'].strip(),
                        "func_id": row['funcId'].strip(),
                        "sql_id": row['sqlId'].strip()
                    }

                    full_query = row['sqlSrc'].strip()
                    split_queries = self._split_queries(full_query)
                    self.report.report_line_parsed(success=True)
                    for split_query in split_queries:
                        data.append({
                            "query": split_query,
                            "custom_keys": custom_keys
                        })
                        self.report.report_query_processed()
                except Exception as e:
                    self.report.report_line_parsed(success=False)
                    self.report.report_warning(
                        f"Error processing line {index + 1}",
                        f"Error: {str(e)}"
                    )
            return data
        except Exception as e:
            self.report.report_failure("Failed to read input file", f"Error: {str(e)}")
            logger.exception("An error occurred while reading the input file")
            return []

    def _write_json_file(self, data: List[Dict[str, Any]]) -> None:
        try:
            with open(self.config.output_path, 'w') as f:
                for record in data:
                    json.dump(record, f)
                    f.write('\n')
            self.report.info(
                f"JSON file created",
                f"File created at {self.config.output_path} with {len(data)} records"
            )
        except Exception as e:
            self.report.report_failure("Failed to write output file", f"Error: {str(e)}")
            logger.exception("An error occurred while writing the output file")
