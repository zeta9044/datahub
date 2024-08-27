import asyncio
import json
import time
from typing import List, Optional, Type

import duckdb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datahub.metadata.schema_classes import _Aspect

app = FastAPI()

# DuckDB 연결
conn = duckdb.connect('datahub.db')

# 테이블 생성
conn.execute('''
    CREATE TABLE IF NOT EXISTS aspects (
        urn VARCHAR,
        aspect VARCHAR,
        version BIGINT,
        metadata JSON,
        systemMetadata JSON,
        createdon BIGINT,
        PRIMARY KEY (urn, aspect, version)
    )
''')

# 비동기 큐 및 이벤트 생성
queue = asyncio.Queue()
processing_event = asyncio.Event()

class AspectRequest(BaseModel):
    urn: str
    aspect: Type[_Aspect]
    metadata: dict
    systemMetadata: Optional[dict] = None

class AspectResponse(BaseModel):
    aspect: dict
    systemMetadata: Optional[dict] = None

async def process_queue():
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
                    INSERT OR REPLACE INTO aspects (urn, aspect, version, metadata, systemMetadata, createdon)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', batch)
            except Exception as e:
                print(f"Error processing batch: {e}")

        if queue.empty():
            processing_event.set()
        await asyncio.sleep(0.1)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(process_queue())

@app.post("/aspects")
async def ingest_aspect(request: AspectRequest):
    await queue.put((
        request.urn,
        request.aspect.get_aspect_name(),
        0,  # Latest version
        json.dumps(request.metadata),
        json.dumps(request.systemMetadata) if request.systemMetadata else None,
        int(time.time() * 1000)
    ))
    return {"status": "queued"}

@app.get("/aspects/{urn}")
async def get_aspect(urn: str, aspect: str, version: int = 0):
    processing_event.clear()
    await processing_event.wait()

    result = conn.execute('''
        SELECT metadata, systemMetadata
        FROM aspects
        WHERE urn = ? AND aspect = ? AND version = ?
    ''', (urn, aspect, version)).fetchone()

    if not result:
        raise HTTPException(status_code=404, detail="Aspect not found")

    return AspectResponse(
        aspect=json.loads(result[0]),
        systemMetadata=json.loads(result[1]) if result[1] else None
    )

@app.post("/aspects:batchIngest")
async def batch_ingest_aspects(requests: List[AspectRequest]):
    for req in requests:
        await queue.put((
            req.urn,
            req.aspect.get_aspect_name(),
            0,  # Latest version
            json.dumps(req.metadata),
            json.dumps(req.systemMetadata) if req.systemMetadata else None,
            int(time.time() * 1000)
        ))
    return {"status": "queued", "count": len(requests)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)