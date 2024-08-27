import asyncio
import argparse
import yaml
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
import duckdb
from aiohttp import web
from datahub.lite.duckdb_lite_config import DuckDBLiteConfig

class RequestQueue:
    def __init__(self):
        self.queue = asyncio.Queue()

    async def enqueue(self, request):
        await self.queue.put(request)

    async def dequeue(self):
        return await self.queue.get()

class TaskManager:
    def __init__(self, config: DuckDBLiteConfig):
        self.conn = duckdb.connect(config.file, read_only=config.read_only, config=config.options)
        self.lock = asyncio.Lock()

    async def execute_query(self, query: str, params: Optional[List[Any]] = None) -> List[Any]:
        async with self.lock:
            return self.conn.execute(query, params or []).fetchall()

class MonitoringSystem:
    def __init__(self):
        self.metrics = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "average_response_time": 0,
        }

    def update_metrics(self, success: bool, response_time: float):
        self.metrics["total_requests"] += 1
        if success:
            self.metrics["successful_requests"] += 1
        else:
            self.metrics["failed_requests"] += 1
        self.metrics["average_response_time"] = (
            (self.metrics["average_response_time"] * (self.metrics["total_requests"] - 1) + response_time)
            / self.metrics["total_requests"]
        )

    def get_metrics(self):
        return self.metrics

class AsyncDuckDBServer:
    def __init__(self, config: dict):
        self.config = config
        db_config = DuckDBLiteConfig(
            file=config['database']['file'],
            read_only=config['database'].get('read_only', False),
            options=config['database'].get('options', {})
        )
        self.queue = RequestQueue()
        self.task_manager = TaskManager(db_config)
        self.monitoring = MonitoringSystem()
        if not db_config.read_only:
            self._init_db()

    def _init_db(self):
        self.task_manager.conn.execute(
            "CREATE TABLE IF NOT EXISTS metadata_aspect_v2 "
            "(urn VARCHAR, aspect_name VARCHAR, version BIGINT, metadata JSON, system_metadata JSON, createdon BIGINT)"
        )
        self.task_manager.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS aspect_idx ON metadata_aspect_v2 (urn, aspect_name, version)"
        )

    async def handle_request(self, query: str, params: Optional[List[Any]] = None) -> Dict[str, Any]:
        start_time = datetime.now()
        try:
            await self.queue.enqueue((query, params))
            result = await self.process_request()
            self.monitoring.update_metrics(True, (datetime.now() - start_time).total_seconds())
            return {"result": result}
        except Exception as e:
            self.monitoring.update_metrics(False, (datetime.now() - start_time).total_seconds())
            logger.error(f"Error processing request: {str(e)}")
            return {"error": str(e)}

    async def process_request(self) -> List[Any]:
        query, params = await self.queue.dequeue()
        return await self.task_manager.execute_query(query, params)

    async def health_check(self) -> Dict[str, Any]:
        try:
            await self.task_manager.execute_query("SELECT 1")
            return {"status": "healthy"}
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return {"status": "unhealthy", "reason": str(e)}

    async def get_metrics(self) -> Dict[str, Any]:
        return self.monitoring.get_metrics()

async def handle_query(request: web.Request) -> web.Response:
    data = await request.json()
    query = data.get('query')
    params = data.get('params', [])
    result = await request.app['service'].handle_request(query, params)
    return web.json_response(result)

async def handle_health_check(request: web.Request) -> web.Response:
    result = await request.app['service'].health_check()
    return web.json_response(result)

async def handle_metrics(request: web.Request) -> web.Response:
    metrics = await request.app['service'].get_metrics()
    return web.json_response(metrics)

def load_config(config_file: str) -> dict:
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def setup_logging(config: dict):
    logging.basicConfig(
        filename=config['logging']['file'],
        level=config['logging']['level'],
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

async def init_app(config: dict) -> web.Application:
    server = AsyncDuckDBServer(config)
    
    app = web.Application()
    app['service'] = server
    app.router.add_post('/query', handle_query)
    app.router.add_get('/health', handle_health_check)
    app.router.add_get('/metrics', handle_metrics)
    
    return app

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Async DuckDB Server")
    parser.add_argument('--config', default='config.yml', help='Path to the configuration file')
    args = parser.parse_args()

    config = load_config(args.config)
    setup_logging(config)

    logger = logging.getLogger(__name__)
    logger.info("Starting Async DuckDB Server")

    app = asyncio.get_event_loop().run_until_complete(init_app(config))
    web.run_app(app, host=config['server']['host'], port=config['server']['port'])
