from datahub.ingestion.run.pipeline import Pipeline

# Define your ingestion configuration
pipeline_config = {
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

# Create and run the ingestion pipeline
pipeline = Pipeline.create(pipeline_config)
pipeline.run()
pipeline.raise_from_status()