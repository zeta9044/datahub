import logging
import os
import json
from datahub.ingestion.run.pipeline import Pipeline
from zeta_lab.utilities.meta_utils import get_meta_instance, META_COLS
from zeta_lab.utilities.tool import get_server_pid, filter_json, format_json_to_report

# Set up logging
logging.basicConfig(
    level=logging.INFO,  # 로그 레벨 설정 (DEBUG, INFO, WARNING, ERROR, CRITICAL 중 선택)
    format="%(asctime)s - %(levelname)s - %(message)s",  # 포맷 설정
)
logger = logging.getLogger(__name__)


def extract_lineage(gms_server_url, prj_id):
    """
    :param gms_server_url: The URL of the GMS server to be checked for availability.
    :param prj_id: The project ID whose repository and sqlsrc.json file are required for lineage extraction.
    :return: None
    """

    try:
        # check alive of gms_server
        logger.info(f"Starting lineage extraction for project {prj_id}")
        if not gms_server_url:
            raise ValueError("Please define gms_server_url.")
        else:
            if not get_server_pid():
                raise ValueError("Please start async_lite_gms server before running this script.")
        logger.info("GMS server check completed")

        # repository path
        engine_home = os.getenv("LIAENG_HOME")
        if not engine_home:
            raise ValueError("Please define environment variable 'LIAENG_HOME' to your local Lia Engine home path.")
        repo_path = os.path.join(engine_home, 'repositorys')
        if not os.path.exists(repo_path):
            raise ValueError("Repository path does not exist.")
        prj_repo_path = os.path.join(repo_path, str(prj_id))
        if not os.path.exists(prj_repo_path):
            raise ValueError(f"Project {prj_id} repository path does not exist.")
        logger.info(f"Repository path validated: {prj_repo_path}")

        # platform,platform_instance,default_db,default_schema  through prj_id
        metadatadb_path = os.path.join(engine_home, 'bin', 'metadata.db')
        if not os.path.exists(metadatadb_path):
            raise ValueError("metadata.db file does not exist.")

        platform, platform_instance, default_db, default_schema = get_meta_instance(
            metadatadb_path,
            prj_id,
            select_columns=(
                META_COLS.PLATFORM, META_COLS.PLATFORM_INSTANCE, META_COLS.DEFAULT_DB, META_COLS.DEFAULT_SCHEMA)
        )
        logger.info(f"platform:{platform}")
        logger.info(f"platform_instance:{platform_instance}")
        logger.info(f"default_db:{default_db}")
        logger.info(f"default_schema:{default_schema}")

        # sqlsrc.json path
        sqlsrc_json_path = os.path.join(prj_repo_path, 'sqlsrc.json')
        if not os.path.exists(sqlsrc_json_path):
            raise ValueError("sqlsrc.json file does not exist.")
        logger.info(f"sqlsrc.json found at: {sqlsrc_json_path}")

        # lineage.db path on prj_repo_path
        lineage_path = os.path.join(prj_repo_path, 'lineage.db')
        if os.path.exists(lineage_path):
            os.remove(lineage_path)
            logger.info(f"Previous {lineage_path} has been removed")

        report_file = f"{prj_id}_lineage.txt"

        duckdb_sink_config = {
            "type": "datahub-lite",
            "config": {
                "type": "duckdb",
                "config": {
                    "file": lineage_path
                }
            }
        }

        queries_pipeline_config = {
            "datahub_api": {
                "server": gms_server_url,
                "timeout_sec": 60
            },
            "source": {
                "type": "sql-queries",
                "config": {
                    "query_file": sqlsrc_json_path,
                    "platform": platform,
                    "platform_instance": platform_instance,
                    "default_db": default_db,
                    "default_schema": default_schema,
                    "env": "PROD",
                }
            },
            "sink": duckdb_sink_config,
            "reporting": [
                {
                    "type": "file",
                     "config": {
                         "filename": report_file
                     }
                 },
            ]
        }

        # run pipeline
        logger.info("Starting pipeline execution")
        queries_pipeline = Pipeline.create(queries_pipeline_config)
        queries_pipeline.run()
        queries_pipeline.raise_from_status()

        try:
            # Pretty-print the JSON output
            with open(report_file, "r") as input_file:
                data = json.load(input_file)
                filtered_data = filter_json(data)
                report_txt = format_json_to_report(filtered_data)

            with open(report_file, "w") as output_file:
                output_file.write(report_txt)

        except FileNotFoundError:
            logger.error("File not found. Please check the file path.")
        except json.JSONDecodeError:
            logger.error("Invalid JSON format. Please check the file contents.")
        except PermissionError:
            logger.error("Permission denied. Check read/write permissions for the file.")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {str(e)}")

        sqlflow_pipeline_config = {
            "source": {
                "type": "sql-flow",
                "config": {
                    "query_file": sqlsrc_json_path,
                    "platform": platform,
                    "duckdb_path": lineage_path
                }
            },
            "sink": {
                "type": "console",
            }
        }

        # run pipeline
        sqlflow_pipeline = Pipeline.create(sqlflow_pipeline_config)
        sqlflow_pipeline.run()
        sqlflow_pipeline.raise_from_status()
        logger.info("Pipeline execution completed successfully")

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
