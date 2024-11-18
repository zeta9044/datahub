import asyncio
import json
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, contextmanager
from functools import wraps
from typing import Dict, Any, List
from urllib.parse import unquote

import click
import duckdb
from cachetools import TTLCache
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import JSONResponse

from utilities.tool import format_time

# Ensure Python 3.10+ is being used
assert sys.version_info >= (3, 10), "Python 3.10+ is required."

# Constants
BATCH_SIZE = 1000
BATCH_TIMEOUT = 1.0
WORKER_POOL_SIZE = 4
CACHE_TTL = 300  # 5 minutes
CACHE_MAX_SIZE = 10000

# Metrics storage
request_metrics = {
    'counts': {},  # Endpoint request counts
    'latencies': {},  # Endpoint latencies
    'db_latencies': [],  # Database operation latencies
    'queue_sizes': []  # Historical queue sizes
}

# Global variables
queue = asyncio.Queue()
processing_event = asyncio.Event()
request_times = {}
thread_pool = ThreadPoolExecutor(max_workers=WORKER_POOL_SIZE)

# Cache initialization
aspect_cache = TTLCache(maxsize=CACHE_MAX_SIZE, ttl=CACHE_TTL)
metadata_cache = TTLCache(maxsize=CACHE_MAX_SIZE, ttl=CACHE_TTL)

class DatabaseManager:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.conn = None
        self._lock = asyncio.Lock()

    @contextmanager
    def _transaction(self):
        """Context manager for database transactions"""
        try:
            self.conn.begin()
            yield
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e

    async def connect(self):
        if not self.conn:
            self.conn = duckdb.connect(self.db_file)
            await self._initialize_db()

    async def _initialize_db(self):
        with self._transaction():
            self.conn.execute('''
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
            # Add indexes for common queries
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_urn ON metadata_aspect_v2(urn)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_aspect ON metadata_aspect_v2(aspect)')


    async def execute_fetchall(self, query: str, params: tuple = None):
        async with self._lock:
            start_time = time.time()
            try:
                result = await asyncio.to_thread(
                    lambda: self.conn.execute(query, params).fetchall()
                )
                duration = time.time() - start_time
                request_metrics['db_latencies'].append(duration)
                if len(request_metrics['db_latencies']) > 1000:
                    request_metrics['db_latencies'].pop(0)
                return result
            except Exception as e:
                logging.error(f"Database query error: {e}")
                raise

    async def execute_fetchone(self, query: str, params: tuple = None):
        async with self._lock:
            start_time = time.time()
            try:
                result = await asyncio.to_thread(
                    lambda: self.conn.execute(query, params).fetchone()
                )
                duration = time.time() - start_time
                request_metrics['db_latencies'].append(duration)
                if len(request_metrics['db_latencies']) > 1000:
                    request_metrics['db_latencies'].pop(0)
                return result
            except Exception as e:
                logging.error(f"Database query error: {e}")
                raise

    async def execute_batch(self, query: str, params: List[tuple]):
        async with self._lock:
            start_time = time.time()
            try:
                result = await asyncio.to_thread(
                    lambda: self.conn.executemany(query, params)
                )
                duration = time.time() - start_time
                request_metrics['db_latencies'].append(duration)
                if len(request_metrics['db_latencies']) > 1000:
                    request_metrics['db_latencies'].pop(0)
                return result
            except Exception as e:
                logging.error(f"Database batch operation error: {e}")
                raise

class MetricsManager:
    @staticmethod
    def track_request(endpoint: str):
        request_metrics['counts'][endpoint] = request_metrics['counts'].get(endpoint, 0) + 1

    @staticmethod
    def track_latency(endpoint: str, duration: float):
        if endpoint not in request_metrics['latencies']:
            request_metrics['latencies'][endpoint] = []
        request_metrics['latencies'][endpoint].append(duration)
        # Keep only last 1000 latencies per endpoint
        if len(request_metrics['latencies'][endpoint]) > 1000:
            request_metrics['latencies'][endpoint].pop(0)

    @staticmethod
    def track_queue_size(size: int):
        request_metrics['queue_sizes'].append(size)
        # Keep only last 1000 queue sizes
        if len(request_metrics['queue_sizes']) > 1000:
            request_metrics['queue_sizes'].pop(0)

def log_time(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        MetricsManager.track_request(func.__name__)
        result = await func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = format_time(end_time - start_time)
        MetricsManager.track_latency(func.__name__, elapsed_time)
        logging.info(f"{func.__name__} took {elapsed_time}")
        request_times[func.__name__] = request_times.get(func.__name__, []) + [elapsed_time]
        return result
    return wrapper

# process_queue 수정
@log_time
async def process_queue(db_manager: DatabaseManager):
    while not app.state.should_exit:  # 종료 플래그 추가
        batch = []
        try:
            while len(batch) < BATCH_SIZE and not app.state.should_exit:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=BATCH_TIMEOUT)
                    batch.append(item)
                    MetricsManager.track_queue_size(queue.qsize())  # 현재 큐 크기 추적
                except asyncio.TimeoutError:
                    break

            if batch:
                await db_manager.execute_batch('''
                    INSERT OR REPLACE INTO metadata_aspect_v2 
                    (urn, aspect, version, metadata, systemMetadata, createdon)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', batch)

                for urn, aspect, _, _, _, _ in batch:
                    cache_key = f"{urn}:{aspect}"
                    aspect_cache.pop(cache_key, None)
                    metadata_cache.pop(urn, None)

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

@log_time
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
    app.state.should_exit = False
    db_manager = DatabaseManager(app.state.db_file)

    try:
        await db_manager.connect()
        app.state.db_manager = db_manager
        queue_task = asyncio.create_task(process_queue(db_manager))
        yield
    except Exception as e:
        logging.error(f"Error during startup: {e}")
        raise
    finally:
        app.state.should_exit = True
        if hasattr(app.state, 'db_manager') and app.state.db_manager.conn:
            app.state.db_manager.conn.close()

app = FastAPI(lifespan=lifespan)

@app.get("/config")
async def get_config():  # @log_time 데코레이터 제거
    try:
        return JSONResponse(content={
            "versions": {
                "acryldata/datahub": {
                    "version": "0.14.1.0"
                }
            },
            "noCode": "true",
            "statefulIngestionCapable": True,
            "retention": "true"
        })
    except Exception as e:
        logging.error(f"Config endpoint error: {e}")
        return JSONResponse(
            content={"error": str(e)},
            status_code=500
        )

@app.post("/entities")
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
        start_time = time.time()
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

        elapsed = time.time() - start_time
        logging.info(f"ingest_entity took {elapsed:.2f}s")

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
    if not queue.empty():
        processing_event.clear()
        await processing_event.wait()

    urn = unquote(encoded_urn)
    cache_key = f"{urn}:{aspect}"

    # Check cache first
    cached_result = aspect_cache.get(cache_key)
    if cached_result:
        return cached_result

    # Special URN handling
    if urn == "urn:li:telemetry:clientId" and aspect == "telemetryClientId":
        result = {
            "aspect": {
                "telemetryClientId": {
                    "clientId": "dummy-client-id",
                    "lastUpdated": int(time.time() * 1000)
                }
            },
            "systemMetadata": None
        }
        return result

    try:
        db_manager = app.state.db_manager
        result = await db_manager.execute_fetchone('''
            SELECT metadata, systemMetadata
            FROM metadata_aspect_v2
            WHERE urn = ? AND aspect = ? AND version = ?
        ''', (urn, aspect, version))

        if result is None:
            message = f"Aspect not found for URN: {urn}, Aspect: {aspect}, Version: {version}"
            logging.info(message)
            raise HTTPException(status_code=404, detail=message)
        print(result)
        response = {
            "aspect": {
                aspect: json.loads(result[0])
            },
            "systemMetadata": json.loads(result[1]) if result[1] else None
        }

        # Cache the result
        aspect_cache[cache_key] = response
        return response

    except HTTPException:
        raise
    except Exception as e:
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
    if not queue.empty():
        processing_event.clear()
        await processing_event.wait()

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

            logging.info(f"Executing SQL query: {query}")
            logging.info(f"SQL parameters: {params}")

            db_manager = app.state.db_manager
            result = await db_manager.execute_fetchall(query, params)
            logging.info(f"SQL query result: {result}")

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
            logging.debug("Unsupported GraphQL query received")
            return JSONResponse(content={"errors": ["Unsupported query"]}, status_code=400)

    except Exception as e:
        logging.error(f"Error occurred during GraphQL query execution: {str(e)}")
        return JSONResponse(content={"errors": [str(e)]}, status_code=500)


@app.get("/health")
async def health_check():
    try:
        queue_size = queue.qsize()
        db_manager = app.state.db_manager

        # Calculate average response times
        avg_response_times = {}
        for endpoint, latencies in request_metrics['latencies'].items():
            if latencies:
                # Convert all latencies to float if they're strings
                numeric_latencies = []
                for lat in latencies:
                    if isinstance(lat, str):
                        try:
                            # Remove any units (ms, s) and convert to float
                            lat = float(''.join(c for c in lat if c.isdigit() or c == '.'))
                        except ValueError:
                            continue
                    numeric_latencies.append(float(lat))

                if numeric_latencies:
                    avg_response_times[endpoint] = f"{sum(numeric_latencies) / len(numeric_latencies):.2f}ms"

        # Calculate queue size statistics
        queue_sizes = request_metrics['queue_sizes']
        queue_stats = {
            'current_size': queue_size,
            'avg_size': f"{sum(queue_sizes) / len(queue_sizes):.2f}" if queue_sizes else "0",
            'max_size': f"{max(queue_sizes)}" if queue_sizes else "0"
        }

        # Get cache stats
        cache_stats = {
            "aspect_cache_size": len(aspect_cache),
            "metadata_cache_size": len(metadata_cache),
            "aspect_cache_hits": aspect_cache.currsize,
            "metadata_cache_hits": metadata_cache.currsize
        }

        # Get request metrics
        metrics = {
            'total_requests_by_endpoint': request_metrics['counts'],
            'average_latency_by_endpoint': avg_response_times,
            'queue_statistics': queue_stats
        }

        # Calculate database metrics
        db_latencies = request_metrics['db_latencies']
        db_stats = {
            'avg_operation_time': f"{sum(db_latencies) / len(db_latencies):.3f}ms" if db_latencies else "0ms",
            'max_operation_time': f"{max(db_latencies):.3f}ms" if db_latencies else "0ms",
            'total_operations': len(db_latencies)
        }

        return {
            "status": "healthy",
            "database": {
                "status": "connected",
                "metrics": db_stats
            },
            "metrics": metrics,
            "cache_stats": cache_stats,
            "worker_pool": {
                "size": WORKER_POOL_SIZE,
                "active": thread_pool._work_queue.qsize()
            }
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
@click.option('--db-file', default='async_lite_gms.db', help='Path to DuckDB database file')
@click.option('--log-level', default='ERROR',
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
              help='Logging level')
@click.option('--port', default=8000, type=int, help='Port to run the server on')
@click.option('--workers', default=WORKER_POOL_SIZE, type=int, help='Number of worker threads')
@click.option('--batch-size', default=BATCH_SIZE, type=int, help='Database batch size')
@click.option('--cache-ttl', default=CACHE_TTL, type=int, help='Cache TTL in seconds')
def main(log_file, db_file, log_level, port, workers, batch_size, cache_ttl):
    global WORKER_POOL_SIZE, BATCH_SIZE, CACHE_TTL

    WORKER_POOL_SIZE = workers
    BATCH_SIZE = batch_size
    CACHE_TTL = cache_ttl

    # Logging setup
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,  # 중요! 기존 로거를 비활성화하지 않음
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": log_file,
                "maxBytes": 10 * 1024 * 1024,
                "backupCount": 5,
                "formatter": "default",
            },
            "console": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "formatter": "default",
            }
        },
        "root": {
            "level": log_level,
            "handlers": ["console", "file"]
        }
    }
    # Store configuration in app state
    app.state.db_file = db_file

    # Start the FastAPI application
    import uvicorn
    logging.info(f"Starting async_lite_gms server on port {port}...")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level=log_level.lower(),
        log_config=log_config,  # 커스텀 로그 설정 적용
        access_log=False  # access 로그 비활성화 (선택사항)
    )

if __name__ == "__main__":
    main()
