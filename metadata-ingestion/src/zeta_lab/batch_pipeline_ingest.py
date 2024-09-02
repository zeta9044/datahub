from zeta_lab.source.custom_sql_queries import CustomSqlQueriesSource  # Ensure this import is done before creating the pipeline
from datahub.ingestion.run.pipeline import Pipeline

# Extract URL as a constant
DATAHUB_URL = "http://zeta:8000"

# Define the common sink configuration
common_sink_config = {
    "type": "datahub-rest",
    "config": {
        "server": DATAHUB_URL,
    }
}

# Define your ingestion configuration for Postgres
# postgres_pipeline_config = {
#     "source": {
#         "type": "postgres",
#         "config": {
#             "platform_instance": "zeta",
#             "host_port": "zeta:5432",
#             "database": "postgres",
#             "username": "dlusr",
#             "password": "dlusr",
#             "schema_pattern": {
#                 "allow": ["dlusr.*", "admin.*"],
#             }
#         }
#     },
#     "sink": common_sink_config
# }

# Define your ingestion configuration for SQL queries
# queries_pipeline_config = {
#     "source": {
#         "type": "sql-queries",
#         "config": {
#             "query_file": "D:/zeta/ingest/queries.json",
#             "platform": "postgres",
#             "platform_instance": "zeta",
#             "default_db": "postgres",
#             "default_schema": "dlusr",
#             "env": "PROD",
#             "usage": {
#                 "format_sql_queries": "True"
#             }
#         }
#     },
#     "sink": common_sink_config
# }

# queries_pipeline_debug_config = {
#     "datahub_api": {
#       "server": DATAHUB_URL,
#       "timeout_sec": 60
#     },
#     "source": {
#         "type": "sql-queries",
#         "config": {
#             "query_file": "D:/zeta/ingest/test.json",
#             "platform": "snowflake",
#             "platform_instance": "na",
#             "default_db": "na",
#             "default_schema": "na",
#             "env": "PROD",
#             "usage": {
#                 "format_sql_queries": "True"
#             }
#         }
#     },
#     "sink": {
#         "type": "file",
#         "config": {
#             "filename": "D:/zeta/logs/queries_ingestion.log"
#         }
#     }
# }

custom_queries_pipeline_debug_config = {
    "datahub_api": {
        "server": DATAHUB_URL,
        "timeout_sec": 60
    },
    "source": {
        "type": "custom-sql-queries",
                "config": {
                    "query_file": "D:/zeta/ingest/test.json",
                    "platform": "snowflake",
                    "platform_instance": "na",
                    "default_db": "na",
                    "default_schema": "na",
                    "env": "PROD",
                    "usage": {
                        "format_sql_queries": "True"
                    }
                }
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": "D:/zeta/logs/custom_queries_ingestion.log"
                }
            }
}

# metadata_file_pipeline_config = {
#     "source": {
#         "type": "file",
#         "config": {
#             "path": "\\\wsl.localhost\\Ubuntu\\home\\zeta\\ingest\\metadata.json",
#             "file_extension": ".json",
#             "read_mode": "AUTO"
#         }
#     },
#     "sink": common_sink_config
# }


# Define a list of pipeline configurations
# pipeline_configs = [postgres_pipeline_config, queries_pipeline_config]
# pipeline_configs = [queries_pipeline_debug_config]
pipeline_configs = [custom_queries_pipeline_debug_config]
# pipeline_configs = [metadata_file_pipeline_config]

# Extract function to run pipeline
def run_pipeline(config):
    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.raise_from_status()


# Execute pipelines
for config in pipeline_configs:
    run_pipeline(config)
