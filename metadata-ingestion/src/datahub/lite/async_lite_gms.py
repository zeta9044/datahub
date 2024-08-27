import asyncio
import json
import re
import time
from typing import List, Optional, Union, Dict, Any
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
from urllib.parse import unquote
import duckdb
import logging

logger = logging.getLogger(__name__)
# 비동기 큐 및 이벤트 생성
queue = asyncio.Queue()
processing_event = asyncio.Event()

# DuckDB 연결을 전역 변수로 선언
conn = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global conn
    conn = duckdb.connect('D:/zeta/ingest/datahub.db')

    # 테이블 생성
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
    # Shutdown
    if conn:
        conn.close()

app = FastAPI(lifespan=lifespan)

class AspectModel(BaseModel):
    urn: str
    aspect: str
    metadata: Dict[str, Any]
    systemMetadata: Optional[Dict[str, Any]] = None

async def process_queue():
    global conn
    while True:
        batch = []
        try:
            while len(batch) < 1000:  # 최대 배치 크기
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
                print(f"Error processing batch: {e}")

        if queue.empty():
            processing_event.set()
        await asyncio.sleep(0.1)

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

def decapitalize(name):
    return name[0].lower() + name[1:] if name else ''

def extract_dataset_key(urn):
    # URN 형식: urn:li:dataset:(urn:li:dataPlatform:platform,name,origin)
    match = re.match(r'urn:li:dataset:\(urn:li:dataPlatform:(\w+),(.*),(\w+)\)', urn)
    if match:
        platform, name, origin = match.groups()
        return {
            "name": name,
            "platform": f"urn:li:dataPlatform:{platform}",
            "origin": origin
        }
    return None

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
                # Extract the aspect name from the full name
                aspect_name = full_aspect_name.split('.')[-1]
                # Convert only the first letter to lowercase
                aspect_name = decapitalize(aspect_name)

                await queue.put((
                    urn,
                    aspect_name,
                    0,  # Latest version
                    json.dumps(aspect_value),
                    json.dumps(system_metadata),
                    int(time.time() * 1000)
                ))

        # Generate and store datasetKey
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

def process_special_aspect(aspect_value):
    if isinstance(aspect_value, dict) and 'value' in aspect_value:
        try:
            # Parse the JSON string
            return json.loads(aspect_value['value'])
        except json.JSONDecodeError:
            # If parsing fails, return the original value
            return aspect_value['value']
    return aspect_value

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

        # Process special aspects
        if aspect_name in ['dataPlatformInstance', 'subTypes', 'browsePathsV2', 'container']:
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
        variables = data.get('variables')

        # 여기서 GraphQL 쿼리를 파싱하고 해당하는 DuckDB 쿼리로 변환해야 합니다.
        # 이 예제에서는 간단한 구현만 제공합니다.
        if "dataset" in query:
            urn = variables.get('urn')
            result = conn.execute('''
                SELECT * FROM metadata_aspect_v2
                WHERE urn = ?
            ''', [urn]).fetchall()
            return JSONResponse(content={"data": {"dataset": result}})
        elif "search" in query:
            input_string = variables.get('input')
            result = conn.execute('''
                SELECT DISTINCT urn FROM metadata_aspect_v2
                WHERE urn LIKE ?
            ''', [f'%{input_string}%']).fetchall()
            return JSONResponse(content={"data": {"search": {"entities": result}}})
        else:
            return JSONResponse(content={"errors": ["Unsupported query"]}, status_code=400)
    except Exception as e:
        logger.error(f"Error in GraphQL query execution: {str(e)}")
        return JSONResponse(content={"errors": [str(e)]}, status_code=500)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)