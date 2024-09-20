from zeta_lab.source.convert_to_qtrack_db import ConvertQtrackSource
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

duckdb_sink_config = {
    "type": "datahub-lite",
    "config": {
        "type": "duckdb",
        "config": {
            "file": "D:/zeta/ingest/sss.db"
        }
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

queries_pipeline_debug_config = {
    "datahub_api": {
        "server": DATAHUB_URL,
        "timeout_sec": 60
    },
    "source": {
        "type": "sql-queries",
        "config": {
            "query_file": "D:/zeta/ingest/sql_queries2.json",
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
    # "sink": {
    #     "type": "file",
    #     "config": {
    #         "filename": "D:/zeta/logs/queries_ingestion.log"
    #     }
    # }
    "sink": duckdb_sink_config
}

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
    # "sink": {
    #     "type": "file",
    #     "config": {
    #         "filename": "D:/zeta/logs/custom_queries_ingestion.log"
    #     }
    # }

    "sink": duckdb_sink_config

    # "sink": {
    #     "type": "console",
    #     # "config": {
    #     #     "filename": "D:/zeta/logs/custom_queries_ingestion.log"
    #     # }
    # }
}

sql_file_pipeline_config = {
    "source": {
        "type": "sql_to_json_converter",
        "config": {
            "input_path": "D:\\zeta\\engine-samples\\datahub-query",
            "output_path": "D:/zeta/ingest/sql_queries.json",
            "dataset_name": "121"
        }
    },
    "sink": {
        "type": "console",
        # "config": {
        #     "filename":"D:/zeta/logs/converter.log"
        # }
    }
}

sqlsrc_file_pipeline_config = {
    "source": {
        "type": "custom_csv_to_json_converter",
        "config": {
            "input_path": "D:/LIAEngine/repositorys/SmilegateTest/sqlsrc.dat",
            # "input_path": "D:/zeta/sqlsrc.dat",
            "output_path": "D:/zeta/ingest/sql_queries.json"
        }
    },
    "sink": {
        "type": "console",
        # "config": {
        #     "filename":"D:/zeta/logs/converter.log"
        # }
    }
}

snowflake_queries_pipeline_debug_config = {
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
    # "sink": {
    #     "type": "file",
    #     "config": {
    #         "filename": "D:/zeta/logs/custom_queries_ingestion.log"
    #     }
    # }

    "sink": duckdb_sink_config

    # "sink": {
    #     "type": "console",
    #     # "config": {
    #     #     "filename": "D:/zeta/logs/custom_queries_ingestion.log"
    #     # }
    # }
}

# Define ingestion of converting work from duckdb(metadata) to Postgres(my dbms) configuration
convert_pipeline_debug_config = {
    "source": {
        "type": "convert_qtrack",
        "config": {
            "datahub_api": {
                "server": DATAHUB_URL,
                "timeout_sec": 60
            },
            "duckdb_path": "D:/zeta/ingest/sss.db",
            "target_config": {
                "type": "postgres",
                "host_port": "zeta:5432",
                "database": "postgres",
                "username": "dlusr",
                "password": "dlusr"
            }
        }
    },
    "sink": {
        "type": "console",
    }
}

# Define a list of pipeline configurations
# pipeline_configs = [postgres_pipeline_config, queries_pipeline_config]
pipeline_configs = [queries_pipeline_debug_config]
# pipeline_configs = [snowflake_queries_pipeline_debug_config,queries_pipeline_debug_config]
# pipeline_configs = [sql_file_pipeline_config]
# pipeline_configs = [sqlsrc_file_pipeline_config]
pipeline_configs = [queries_pipeline_debug_config,convert_pipeline_debug_config]


# Extract function to run pipeline
def run_pipeline(config):
    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.raise_from_status()


# Execute pipelines
for config in pipeline_configs:
    run_pipeline(config)
