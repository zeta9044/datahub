from datahub.ingestion.run.pipeline import Pipeline

# Define your ingestion configuration
postgres_pipeline_config = {
    "source": {
        "type": "postgres",
        "config": {
            "platform_instance": "zeta",
            "host_port": "zeta:5432",
            "database": "postgres",
            "username": "dlusr",
            "password": "dlusr",
            "schema_pattern": {
                "allow": ["dlusr.*","admin.*"],
            }
        }
    },
    "sink": {
        "type": "datahub-rest",
        "config": {
            "server": "http://zeta:8000",  # Change this to your DataHub server URL
        }
    }
}

# Define your ingestion configuration
queries_pipeline_config = {
    "source": {
        "type": "sql-queries",
        "config": {
            "query_file": "D:/zeta/ingest/queries.json",
            "platform": "postgres",
            "platform_instance": "zeta",
            "default_db": "postgres",
            "default_schema": "dlusr",
            "env": "PROD",
            "usage": {
                "format_sql_queries": "True"
            }

        }
    },
    "sink": {
        "type": "datahub-rest",
        "config": {
            "server": "http://zeta:8000",  # Change this to your DataHub server URL
        }
    }
}


for config in [postgres_pipeline_config, queries_pipeline_config]:
    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.raise_from_status()