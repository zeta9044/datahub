import os
from zeta_lab.source.qtrack_meta_source import QtrackMetaSource
from zeta_lab.utilities.tool import extract_dsn_from_xml_file,get_server_pid
from datahub.ingestion.run.pipeline import Pipeline

def ingest_metadata(gms_server_url):
    """
    :param prj_id: Project identifier used to locate specific project files in the repository.
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
        config_path = os.path.join(engine_home,'config')
        if not os.path.exists(config_path):
            raise ValueError("Config path does not exist.")

        # service.xml path
        service_xml_path = os.path.join(config_path,'service.xml')
        if not os.path.exists(service_xml_path):
            raise ValueError("service.xml file does not exist.")

        # security.properties path
        security_properties_path = os.path.join(config_path,'security.properties')
        if not os.path.exists(security_properties_path):
            raise ValueError("security.properties file does not exist.")

        # postgresql dsn for sqlalchemy
        pg_dsn = extract_dsn_from_xml_file(service_xml_path,security_properties_path)
        if not pg_dsn:
            raise ValueError("Data Source Name (DSN) is not found.")

        # metadata.db path on LIAENG_HOME/bin
        metadata_path = os.path.join(engine_home,'bin','metadata.db')

        gms_sink_config = {
            "type": "datahub-rest",
            "config": {
                "server": gms_server_url,
            }
        }

        # pipeline setting
        qtrack_meta_pipeline_config = {
            "source": {
                "type": "qtrack_meta_source",
                "config": {
                    "pg_dsn" : pg_dsn,
                    "duckdb_path": metadata_path,
                    "gms_server": gms_server_url
                }
            },
            "sink": gms_sink_config
        }

        # run pipeline
        pipeline = Pipeline.create(qtrack_meta_pipeline_config)
        pipeline.run()
        pipeline.raise_from_status()
    except Exception as e:
        raise

if __name__ == "__main__":
    # example
    gms_server_url = "http://localhost:8000"
    try:
        ingest_metadata(gms_server_url=gms_server_url)
    except Exception as e:
        print(e)
