import os
from zeta_lab.utilities.tool import get_server_pid
from zeta_lab.utilities.common_logger import setup_logging, cleanup_logger
from datahub.ingestion.run.pipeline import Pipeline


def extract_lineage(gms_server_url, prj_id, log_file=None):
    """
    :param gms_server_url: The URL of the GMS server to be checked for availability.
    :param prj_id: The project ID whose repository and sqlsrc.json file are required for lineage extraction.
    :param log_file: Optional log file path. Defaults to {prj_id}_run.out if not specified.
    :return: None
    """
    log_file = log_file if log_file else f"{prj_id}_run.out"
    logger = setup_logging(log_file, __name__)

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
                    "platform": "snowflake",
                    "platform_instance": "na",
                    "default_db": "na",
                    "default_schema": "na",
                    "env": "PROD",
                }
            },
            "sink": duckdb_sink_config
        }

        # run pipeline
        logger.info("Starting pipeline execution")
        pipeline = Pipeline.create(queries_pipeline_config)
        pipeline.run()
        pipeline.raise_from_status()
        logger.info("Pipeline execution completed successfully")

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        raise
    finally:
        cleanup_logger(logger)

if __name__ == "__main__":
    # example
    gms_server_url = "http://localhost:8000"
    prj_id = 21
    try:
        extract_lineage(gms_server_url=gms_server_url, prj_id=prj_id)
    except Exception as e:
        print(e)
