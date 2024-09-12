import asyncio
import json
import logging
import re
import sys
import time
from contextlib import asynccontextmanager
from functools import wraps
from logging.handlers import RotatingFileHandler
from typing import Dict, Any
from urllib.parse import unquote

import click
import duckdb
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import JSONResponse

from utilities.tool import format_time

# Ensure Python 3.10+ is being used
assert sys.version_info >= (3, 10), "Python 3.10+ is required."

# Global variables
queue = asyncio.Queue()
processing_event = asyncio.Event()
conn = None
request_times = {}


def log_time(func):
    """
    :param func: The asynchronous function to be wrapped by the decorator
    :return: The wrapped function that logs execution time and updates request_times dictionary"""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = format_time(end_time - start_time)
        logging.info(f"{func.__name__} took {elapsed_time} ")
        request_times[func.__name__] = request_times.get(func.__name__, []) + [elapsed_time]
        return result

    return wrapper


async def process_queue():
    """
    Processes items from an asynchronous queue in batches and inserts them into a database table.

    :return: None
    """
    global conn
    while True:
        batch = []
        try:
            while len(batch) < 1000:
                item = await asyncio.wait_for(queue.get(), timeout=1.0)
                batch.append(item)
        except asyncio.TimeoutError:
            pass

        if batch:
            try:
                conn.executemany('''
                    INSERT OR REPLACE INTO metadata_aspect_v2 (urn, aspect, version, metadata, systemMetadata, createdon)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', batch)
            except Exception as e:
                logging.error(f"Error processing batch: {e}")

        if queue.empty():
            processing_event.set()
        await asyncio.sleep(0.1)


def decapitalize(name):
    """
    :param name: The string to decapitalize.
    :return: A new string with the first character converted to lowercase. If the input string is empty, returns an empty string.
    """
    return name[0].lower() + name[1:] if name else ''


def extract_dataset_key(urn):
    """
    :param urn: A string in the format 'urn:li:dataset:(urn:li:dataPlatform:platform,name,origin)'
    :return: A dictionary containing the parsed 'name', 'platform', and 'origin' if a match is found, otherwise None
    """
    match = re.match(r'urn:li:dataset:\(urn:li:dataPlatform:(\w+),(.*),(\w+)\)', urn)
    if match:
        platform, name, origin = match.groups()
        return {
            "name": name,
            "platform": f"urn:li:dataPlatform:{platform}",
            "origin": origin
        }
    return None


def process_special_aspect(aspect_value):
    """
    :param aspect_value: The input parameter which can be of any type but is usually a dictionary containing a 'value' key.
    :return: Parsed JSON object if 'value' is a valid JSON string, original 'value' if JSON parsing fails, or the original input if it is not a dictionary with a 'value' key.
    """
    if isinstance(aspect_value, dict) and 'value' in aspect_value:
        try:
            return json.loads(aspect_value['value'])
        except json.JSONDecodeError:
            return aspect_value['value']
    return aspect_value


def process_aspect(aspect: str, metadata: str) -> Dict[str, Any]:
    """
    :param aspect: The type of metadata aspect being processed. It can be one of the following: 'schemaMetadata', 'datasetProperties', or 'datasetKey'.
    :param metadata: The JSON string containing metadata information that needs to be processed.
    :return: A dictionary with processed metadata based on the aspect type. If the aspect is 'schemaMetadata', a dictionary with field paths and native data"""
    try:
        data = json.loads(metadata)
        if aspect == 'schemaMetadata':
            return {
                "fields": [
                    {
                        "fieldPath": field.get('fieldPath'),
                        "nativeDataType": field.get('nativeDataType')
                    } for field in data.get('fields', [])
                ]
            }
        elif aspect in ['datasetProperties', 'datasetKey']:
            return data
        else:
            return data
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON for aspect {aspect}")
        return {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    :param app: An instance of the FastAPI application.
    :return: Yields control back to the calling context during the startup and shutdown phases of the FastAPI application lifecycle.
    """
    global conn
    try:
        conn = duckdb.connect(app.state.db_file)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS metadata_aspect_v2 (
                urn VARCHAR,
                aspect VARCHAR,
                version BIGINT,
                metadata JSON,
                systemMetadata JSON,
                createdon BIGINT,
                PRIMARY KEY (urn, aspect, version)
            )
        ''')
        asyncio.create_task(process_queue())
        yield
    except Exception as e:
        logging.error(f"Error during startup: {e}")
        raise
    finally:
        if conn:
            conn.close()


app = FastAPI(lifespan=lifespan)


@app.get("/config")
@log_time
async def get_config():
    """
    Handles the GET request to retrieve the configuration settings.

    :return: A dictionary containing version details, no-code status, stateful ingestion capability, and retention policy.
    """
    return {
        "versions": {
            "acryldata/datahub": {
                "version": "0.13.3.3"
            }
        },
        "noCode": "true",
        "statefulIngestionCapable": True,
        "retention": "true"
    }


@app.post("/entities")
@log_time
async def ingest_entity(request: Request, action: str = Query(...)):
    """
    :param request: The HTTP request object containing the entity data to be ingested.
    :param action: Query parameter specifying the action to be performed. It must be "ingest".
    :return: A dictionary with the status of the operation, indicating if the entity data has been queued successfully.
    :raises HTTPException: If the action is not "ingest" (400) or if a required URN"""
    if action != "ingest":
        raise HTTPException(status_code=400, detail="Invalid action")

    try:
        data = await request.json()
        entity = data.get("entity", {}).get("value", {})
        system_metadata = data.get("systemMetadata", {})

        for snapshot_type, snapshot_value in entity.items():
            urn = snapshot_value.get("urn")
            if not urn:
                raise HTTPException(status_code=400, detail="URN is required")

            aspects = snapshot_value.get("aspects", [])
            for aspect in aspects:
                for full_aspect_name, aspect_value in aspect.items():
                    aspect_name = full_aspect_name.split('.')[-1]
                    aspect_name = decapitalize(aspect_name)

                    await queue.put((
                        urn,
                        aspect_name,
                        0,  # Latest version
                        json.dumps(aspect_value),
                        json.dumps(system_metadata),
                        int(time.time() * 1000)
                    ))

            dataset_key = extract_dataset_key(urn)
            if dataset_key:
                await queue.put((
                    urn,
                    "datasetKey",
                    0,  # Latest version
                    json.dumps(dataset_key),
                    json.dumps(system_metadata),
                    int(time.time() * 1000)
                ))

        return {"status": "queued"}
    except Exception as e:
        logging.error(f"Error in ingest_entity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/aspects")
@log_time
async def ingest_aspect(request: Request, action: str = Query(...)):
    """
    :param request: FastAPI Request object containing the payload for the aspect ingestion.
    :param action: Specifies the type of action to be performed. Must be either "ingestProposal" or "ingestProposalBatch".
    :return: A JSON object indicating the status of the request and the number of proposals that were queued.
    """
    if action not in ["ingestProposal", "ingestProposalBatch"]:
        raise HTTPException(status_code=400, detail="Invalid action")

    try:
        data = await request.json()

        if action == "ingestProposal":
            proposals = [data.get("proposal", {})]
        else:  # ingestProposalBatch
            proposals = data.get("proposals", [])

        for proposal in proposals:
            urn = proposal.get("entityUrn")
            aspect_name = proposal.get("aspectName")
            aspect_value = proposal.get("aspect", {})
            system_metadata = proposal.get("systemMetadata", {})

            if not urn or not aspect_name:
                raise HTTPException(status_code=400, detail="EntityUrn and aspectName are required")

            aspect_value = process_special_aspect(aspect_value)

            await queue.put((
                urn,
                aspect_name,
                0,  # Latest version
                json.dumps(aspect_value),
                json.dumps(system_metadata),
                int(time.time() * 1000)
            ))

        return {"status": "queued", "count": len(proposals)}
    except Exception as e:
        logging.error(f"Error in ingest_aspect: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/aspects/{encoded_urn}")
@log_time
async def get_aspect(encoded_urn: str, aspect: str = Query(...), version: int = Query(0)):
    """
    :param encoded_urn: The encoded URN of the entity being requested.
    :param aspect: The specific aspect of the entity's metadata to retrieve.
    :param version: The version number of the metadata aspect to retrieve.
    :return:"""
    global conn
    processing_event.clear()
    await processing_event.wait()

    urn = unquote(encoded_urn)

    # 특수 URN 처리
    if urn == "urn:li:telemetry:clientId" and aspect == "telemetryClientId":
        # 텔레메트리 클라이언트 ID에 대한 더미 데이터 반환
        dummy_data = {
            "clientId": "dummy-client-id",
            "lastUpdated": int(time.time() * 1000)
        }
        return {
            "aspect": {
                "telemetryClientId": dummy_data
            },
            "systemMetadata": None
        }

    try:
        result = conn.execute('''
            SELECT metadata, systemMetadata
            FROM metadata_aspect_v2
            WHERE urn = ? AND aspect = ? AND version = ?
        ''', (urn, aspect, version)).fetchone()

        if not result:
            message = f"Aspect not found for URN: {urn}, Aspect: {aspect}, Version: {version}"
            logging.warning(message)
            raise HTTPException(status_code=404, detail=message)

        return {
            "aspect": {
                aspect: json.loads(result[0])
            },
            "systemMetadata": json.loads(result[1]) if result[1] else None
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logging.error(f"Error in get_aspect: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.post("/usageStats")
@log_time
async def ingest_usage_stats(request: Request, action: str = Query(...)):
    """
    :param request: A FastAPI Request object containing the request data.
    :param action: A query parameter specifying the action to be performed. Must be "batchIngest".
    :return: A dictionary containing the status and the count of ingested buckets. In case of error, raises HTTPException with appropriate status code and error message.
    """
    if action != "batchIngest":
        raise HTTPException(status_code=400, detail="Invalid action")

    try:
        data = await request.json()
        buckets = data.get("buckets", [])

        for bucket in buckets:
            urn = bucket.get("key", {}).get("datasetUrn")
            if not urn:
                raise HTTPException(status_code=400, detail="DatasetUrn is required")

            await queue.put((
                urn,
                "datasetUsageStatistics",
                0,  # Latest version
                json.dumps(bucket),
                json.dumps({}),  # Empty system metadata for usage stats
                int(time.time() * 1000)
            ))

        return {"status": "queued", "count": len(buckets)}
    except Exception as e:
        logging.error(f"Error in ingest_usage_stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/graphql")
@log_time
async def graphql_endpoint(request: Request):
    """
    :param request: The incoming HTTP request containing the GraphQL query and variables.
    :return: A JSONResponse containing the data retrieved based on the GraphQL query,
             or an error message if the query is unsupported or if an error occurs during execution.
    """
    try:
        data = await request.json()
        query = data.get('query')
        variables = data.get('variables', {})

        logging.info(f"Received GraphQL query: {query}")
        logging.info(f"Variables: {variables}")

        if "scrollAcrossEntities" in query:
            or_filters = variables.get('orFilters', [])

            where_clauses = []
            params = []

            for or_filter in or_filters:
                and_clauses = []
                for and_filter in or_filter['and']:
                    field = and_filter['field']
                    value = and_filter.get('value') or and_filter.get('values', [])[0]

                    if field == 'platform.keyword':
                        and_clauses.append("urn LIKE ?")
                        params.append(f"urn:li:dataset:(urn:li:dataPlatform:{value.split(':')[-1]},%")
                    elif field == 'origin':
                        and_clauses.append("urn LIKE ?")
                        params.append(f"%,{value})")

                if and_clauses:
                    where_clauses.append(f"({' AND '.join(and_clauses)})")

            final_where = " OR ".join(where_clauses) if where_clauses else "1=1"

            query = f"""
                SELECT DISTINCT urn, aspect, metadata
                FROM metadata_aspect_v2
                WHERE {final_where}
            """

            logging.debug(f"Executing SQL query: {query}")
            logging.debug(f"SQL parameters: {params}")

            result = conn.execute(query, params).fetchall()
            logging.debug(f"SQL query result: {result}")

            search_results = []
            for row in result:
                urn, aspect, metadata = row
                entity = {
                    "urn": urn,
                    "type": urn.split(':')[2],
                }
                if aspect == 'schemaMetadata':
                    schema_metadata = json.loads(metadata)
                    entity['schemaMetadata'] = {
                        'fields': [
                            {
                                'fieldPath': field.get('fieldPath'),
                                'nativeDataType': field.get('nativeDataType')
                            }
                            for field in schema_metadata.get('fields', [])
                        ]
                    }
                search_results.append({"entity": entity})

            total = len(search_results)

            response = {
                "data": {
                    "scrollAcrossEntities": {
                        "nextScrollId": None,
                        "searchResults": search_results,
                        "start": 0,
                        "count": total,
                        "total": total,
                        "pageInfo": {
                            "startCursor": "0",
                            "endCursor": str(total - 1),
                            "hasNextPage": False
                        }
                    }
                }
            }

            logging.info(f"Sending response: {json.dumps(response, indent=2)}")

            return JSONResponse(content=response)
        else:
            logging.warning("Unsupported GraphQL query received")
            return JSONResponse(content={"errors": ["Unsupported query"]}, status_code=400)

    except Exception as e:
        logging.error(f"Error occurred during GraphQL query execution: {str(e)}")
        return JSONResponse(content={"errors": [str(e)]}, status_code=500)


@app.get("/health")
async def health_check():
    """
    :return: A dictionary with health"""
    try:
        # Check DuckDB connection
        conn.execute("SELECT 1")

        # Check queue status
        queue_size = queue.qsize()

        # Check performance metrics
        avg_response_times = {k: sum(v) / len(v) for k, v in request_times.items() if v}

        return {
            "status": "healthy",
            "database": "connected",
            "queue_size": queue_size,
            "avg_response_times": avg_response_times
        }
    except Exception as e:
        logging.error(f"Health check failed: {e}")
        return JSONResponse(
            content={
                "status": "unhealthy",
                "error": str(e)
            },
            status_code=500
        )


@click.command()
@click.option('--log-file', default='async_lite_gms.log', help='Path to log file')
@click.option('--db-file', default='datahub.db', help='Path to DuckDB database file')
@click.option('--log-level', default='INFO', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
              help='Logging level')
@click.option('--port', default=8000, type=int, help='Port to run the server on')
def main(log_file, db_file, log_level, port):
    """
    :param log_file: Path to log file
    :param db_file:"""
    # Logging setup
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5),
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Disable uvicorn access logs
    logging.getLogger("uvicorn.access").handlers = []

    # Store configuration in app state
    app.state.db_file = db_file

    # Start the FastAPI application
    import uvicorn
    logging.info(f"Starting async_lite_gms server on port {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level=log_level.lower())


if __name__ == "__main__":
    main()
