import os
import logging
from zeta_lab.utilities.tool import extract_db_info, get_server_pid
from zeta_lab.utilities.meta_utils import get_meta_instance, META_COLS
from datahub.ingestion.run.pipeline import Pipeline

# Set up logging
logging.basicConfig(
    level=logging.INFO,  # 로그 레벨 설정 (DEBUG, INFO, WARNING, ERROR, CRITICAL 중 선택)
    format="%(asctime)s - %(levelname)s - %(message)s",  # 포맷 설정
)

logger = logging.getLogger(__name__)

def move_lineage(gms_server_url, prj_id):
    """
    :param gms_server_url: The URL of the GMS server to be checked for availability.
    :param prj_id: The project ID whose repository and sqlsrc.json file are required for lineage extraction.
    :return: None
    """
    try:
        # check alive of gms_server
        if not gms_server_url:
            raise ValueError("Please define gms_server_url.")
        else:
            if not get_server_pid():
                raise ValueError("Please start async_lite_gms server before running this script.")

        # config path
        engine_home = os.getenv("LIAENG_HOME")
        if not engine_home:
            raise ValueError("Please define environment variable 'LIAENG_HOME' to your local Lia Engine home path.")
        config_path = os.path.join(engine_home, 'config')
        if not os.path.exists(config_path):
            raise ValueError("Config path does not exist.")

        # service.xml path
        service_xml_path = os.path.join(config_path, 'service.xml')
        if not os.path.exists(service_xml_path):
            raise ValueError("service.xml file does not exist.")

        # security.properties path
        security_properties_path = os.path.join(config_path, 'security.properties')
        if not os.path.exists(security_properties_path):
            raise ValueError("security.properties file does not exist.")

        # repository path
        repo_path = os.path.join(engine_home, 'repositorys')
        if not os.path.exists(repo_path):
            raise ValueError("Repository path does not exist.")
        prj_repo_path = os.path.join(repo_path, str(prj_id))
        if not os.path.exists(prj_repo_path):
            raise ValueError(f"Project {prj_id} repository path does not exist.")

        # lineage.db path
        lineage_path = os.path.join(prj_repo_path, 'lineage.db')
        if not os.path.exists(lineage_path):
            raise ValueError("lineage.db file does not exist.")

        # metadata.db path
        metadatadb_path = os.path.join(engine_home, 'bin', 'metadata.db')
        if not os.path.exists(metadatadb_path):
            raise ValueError("metadata.db file does not exist.")

        host_port, database, username, password = extract_db_info(
            service_xml_path=service_xml_path,
            security_properties_path=security_properties_path)

        system_biz_id = get_meta_instance(metadatadb_path, prj_id, select_columns=META_COLS.SYSTEM_BIZ_ID)

        # Define ingestion of converting work from duckdb(metadata) to Postgres(my dbms) configuration
        convert_qtrack_pipeline_config = {
            "source": {
                "type": "convert-to-qtrack-db",
                "config": {
                    "datahub_api": {
                        "server": gms_server_url,
                        "timeout_sec": 60
                    },
                    "duckdb_path": lineage_path,
                    "prj_id": prj_id,
                    "system_biz_id": system_biz_id,
                    "logger_path": os.path.join(engine_home, 'logs'),
                    "target_config": {
                        "type": "postgres",
                        "host_port": host_port,
                        "database": database,
                        "username": username,
                        "password": password
                    }
                }
            },
            "sink": {
                "type": "console",
            }
        }

        # run pipeline
        pipeline = Pipeline.create(convert_qtrack_pipeline_config)
        pipeline.run()
        pipeline.raise_from_status()
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)