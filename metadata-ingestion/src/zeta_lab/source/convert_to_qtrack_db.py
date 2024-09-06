import json
import duckdb
import psycopg2
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import DatasetPropertiesClass
import requests

class ConvertQtrackSource(Source):
    def __init__(self, config: dict, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

    @classmethod
    def create(cls, config_dict, ctx):
        return cls(config_dict, ctx)

    def get_workunits(self):
        # DuckDB에서 데이터 읽기
        conn = duckdb.connect(self.config["duckdb_path"])
        query = """
        SELECT json FROM mce
        WHERE aspect_name = 'sqlParsingResult' AND version = 0
        """
        results = conn.execute(query).fetchall()

        for row in results:
            mce_json = json.loads(row[0])
            in_tables = mce_json.get("inTables", [])
            out_tables = mce_json.get("outTables", [])

            for table_urn in in_tables + out_tables:
                # DataHub API를 통해 데이터셋 속성 가져오기
                dataset_properties = self.get_dataset_properties(table_urn)

                if dataset_properties:
                    # PostgreSQL로 데이터 전송
                    self.send_to_postgres(table_urn, dataset_properties)

                # MetadataWorkUnit 생성 (선택적)
                mce = MetadataChangeEvent(
                    proposedSnapshot=DatasetPropertiesClass(
                        # 필요한 필드 채우기
                    )
                )
                wu = MetadataWorkUnit(id=f"{table_urn}-properties", mce=mce)
                self.report.report_workunit(wu)
                yield wu

    def get_dataset_properties(self, dataset_urn):
        url = f"{self.config['datahub_api']['server']}/aspects/{dataset_urn}?type=datasetProperties&version=0"
        response = requests.get(url, timeout=self.config['datahub_api']['timeout_sec'])
        if response.status_code == 200:
            return response.json()
        return None

    def send_to_postgres(self, table_urn, dataset_properties):
        pg_config = self.config['target_config']
        conn = psycopg2.connect(
            host=pg_config['host_port'].split(':')[0],
            port=pg_config['host_port'].split(':')[1],
            database=pg_config['database'],
            user=pg_config['username'],
            password=pg_config['password']
        )
        cur = conn.cursor()

        # 여기에 PostgreSQL 삽입 로직 구현
        # 예: cur.execute("INSERT INTO ...", (table_urn, json.dumps(dataset_properties)))

        conn.commit()
        cur.close()
        conn.close()

    def get_report(self):
        return self.report

    def close(self):
        pass