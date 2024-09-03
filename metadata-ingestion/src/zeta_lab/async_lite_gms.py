import asyncio
import json
import logging
import re
import sys
import time
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
from typing import List, Optional, Dict, Any
from urllib.parse import unquote

import duckdb
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Ensure Python 3.10+ is being used
assert sys.version_info >= (3, 10), "Python 3.10+ is required."

# Logging setup
log_file_path = 'D:/zeta/logs/async_lite_gms.log'
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

file_handler = RotatingFileHandler(log_file_path, maxBytes=10*1024*1024, backupCount=5)
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

logging.getLogger("uvicorn").handlers = []
logging.getLogger("uvicorn.access").handlers = []

# Async queue and event
queue = asyncio.Queue()
processing_event = asyncio.Event()

# Global DuckDB connection
conn = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global conn
    conn = duckdb.connect('D:/zeta/ingest/datahub.db')
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
    if conn:
        conn.close()

app = FastAPI(lifespan=lifespan)

class AspectModel(BaseModel):
    urn: str
    aspect: str
    metadata: Dict[str, Any]
    systemMetadata: Optional[Dict[str, Any]] = None

class AndFilterInput:
    def __init__(self, field: str, values: Optional[List[str]] = None, value: Optional[str] = None, condition: str = 'EQUAL', negated: bool = False):
        self.field = field
        self.values = values or []
        if value is not None:
            self.values.append(value)
        self.condition = condition
        self.negated = negated

class OrFilterInput:
    def __init__(self, and_filters: List[AndFilterInput]):
        self.and_filters = and_filters

class ScrollAcrossEntitiesInput:
    def __init__(
            self,
            query: str,
            types: List[str],
            orFilters: List[OrFilterInput],
            count: int,
            scrollId: Optional[str] = None,
    ):
        self.query = query
        self.types = types
        self.orFilters = orFilters
        self.count = count
        self.scrollId = scrollId

async def process_queue():
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
                logger.error(f"Error processing batch: {e}")

        if queue.empty():
            processing_event.set()
        await asyncio.sleep(0.1)

def decapitalize(name):
    return name[0].lower() + name[1:] if name else ''

def extract_dataset_key(urn):
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
    if isinstance(aspect_value, dict) and 'value' in aspect_value:
        try:
            return json.loads(aspect_value['value'])
        except json.JSONDecodeError:
            return aspect_value['value']
    return aspect_value

def process_aspect(aspect: str, metadata: str) -> Dict[str, Any]:
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
        logger.error(f"Error decoding JSON for aspect {aspect}")
        return {}

def build_where_clause(or_filters: List[OrFilterInput]) -> tuple:
    where_clauses = []
    params = []
    for or_filter in or_filters:
        and_clauses = []
        for and_filter in or_filter.and_filters:
            if and_filter.field == 'platform.keyword':
                and_clauses.append("urn LIKE ?")
                params.append(f"{and_filter.values[0]}:%")
            elif and_filter.field == 'removed':
                if and_filter.negated:
                    and_clauses.append("urn NOT LIKE ?")
                else:
                    and_clauses.append("urn LIKE ?")
                params.append("%:DELETED")
            elif and_filter.field == 'customProperties':
                and_clauses.append("metadata LIKE ?")
                params.append(f"%{and_filter.values[0]}%")
            elif and_filter.field == 'origin':
                and_clauses.append("metadata LIKE ?")
                params.append(f"%\"origin\":\"{and_filter.values[0]}\"%")
        if and_clauses:
            where_clauses.append(f"({' AND '.join(and_clauses)})")

    final_where = " OR ".join(where_clauses) if where_clauses else "1=1"
    return final_where, params

@app.get("/config")
async def get_config():
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
async def ingest_entity(request: Request, action: str = Query(...)):
    if action != "ingest":
        raise HTTPException(status_code=400, detail="Invalid action")

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

@app.post("/aspects")
async def ingest_aspect(request: Request, action: str = Query(...)):
    if action not in ["ingestProposal", "ingestProposalBatch"]:
        raise HTTPException(status_code=400, detail="Invalid action")

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

@app.get("/aspects/{encoded_urn}")
async def get_aspect(encoded_urn: str, aspect: str = Query(...), version: int = Query(0)):
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

    result = conn.execute('''
        SELECT metadata, systemMetadata
        FROM metadata_aspect_v2
        WHERE urn = ? AND aspect = ? AND version = ?
    ''', (urn, aspect, version)).fetchone()

    if not result:
        raise HTTPException(status_code=404, detail="Aspect not found")

    return {
        "aspect": {
            aspect: json.loads(result[0])
        },
        "systemMetadata": json.loads(result[1]) if result[1] else None
    }

@app.post("/usageStats")
async def ingest_usage_stats(request: Request, action: str = Query(...)):
    if action != "batchIngest":
        raise HTTPException(status_code=400, detail="Invalid action")

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

@app.post("/api/graphql")
async def graphql_endpoint(request: Request):
    try:
        data = await request.json()
        query = data.get('query')
        variables = data.get('variables', {})

        logger.info(f"Received GraphQL query: {query}")
        logger.info(f"Variables: {variables}")

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

            logger.debug(f"Executing SQL query: {query}")
            logger.debug(f"SQL parameters: {params}")

            result = conn.execute(query, params).fetchall()
            logger.debug(f"SQL query result: {result}")

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

            logger.info(f"Sending response: {json.dumps(response, indent=2)}")

            return JSONResponse(content=response)
        else:
            logger.warning("Unsupported GraphQL query received")
            return JSONResponse(content={"errors": ["Unsupported query"]}, status_code=400)

    except Exception as e:
        logger.error(f"Error occurred during GraphQL query execution: {str(e)}")
        return JSONResponse(content={"errors": [str(e)]}, status_code=500)

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting async_lite_gms server...")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")