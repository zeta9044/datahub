import aiohttp
import asyncio
import time
import statistics
import json
from datetime import datetime

class AsyncLiteGMSLoadTest:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.results = {}

    async def measure_endpoint(self, session, method, endpoint, payload=None, params=None):
        """Single endpoint measurement"""
        start_time = time.time()
        try:
            if method.upper() == "GET":
                async with session.get(f"{self.base_url}{endpoint}", params=params) as response:
                    await response.text()
            else:  # POST
                async with session.post(f"{self.base_url}{endpoint}", json=payload, params=params) as response:
                    await response.text()
            duration = time.time() - start_time
            return duration, response.status
        except Exception as e:
            return None, str(e)

    async def run_concurrent_requests(self, method, endpoint, count, payload=None, params=None):
        """Run multiple concurrent requests to an endpoint"""
        async with aiohttp.ClientSession() as session:
            tasks = []
            for _ in range(count):
                tasks.append(self.measure_endpoint(session, method, endpoint, payload, params))
            results = await asyncio.gather(*tasks)
            return [r for r in results if r[0] is not None]  # Filter out failed requests

    def calculate_statistics(self, durations):
        """Calculate performance statistics"""
        if not durations:
            return {}
        return {
            "min": min(durations),
            "max": max(durations),
            "mean": statistics.mean(durations),
            "median": statistics.median(durations),
            "p95": sorted(durations)[int(len(durations) * 0.95)],
            "total_requests": len(durations),
            "requests_per_second": len(durations) / sum(durations)
        }

    async def test_config_endpoint(self, concurrent_requests=100):
        """Test GET /config endpoint"""
        results = await self.run_concurrent_requests("GET", "/config", concurrent_requests)
        self.results["config_endpoint"] = self.calculate_statistics([r[0] for r in results])

    async def test_ingest_entity(self, concurrent_requests=50):
        """Test POST /entities endpoint"""
        payload = {
            "entity": {
                "value": {
                    "dataset": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,test_table,PROD)",
                        "aspects": [
                            {
                                "SchemaMetadata": {
                                    "fields": [
                                        {"fieldPath": "id", "nativeDataType": "int"},
                                        {"fieldPath": "name", "nativeDataType": "varchar"}
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        }
        results = await self.run_concurrent_requests(
            "POST", "/entities", concurrent_requests,
            payload=payload, params={"action": "ingest"}
        )
        self.results["ingest_entity"] = self.calculate_statistics([r[0] for r in results])

    async def test_aspect_query(self, concurrent_requests=100):
        """Test GET /aspects endpoint"""
        encoded_urn = "urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amysql%2Ctest_table%2CPROD%29"
        results = await self.run_concurrent_requests(
            "GET", f"/aspects/{encoded_urn}",
            concurrent_requests,
            params={"aspect": "schemaMetadata", "version": 0}
        )
        self.results["aspect_query"] = self.calculate_statistics([r[0] for r in results])

    async def test_queue_processing(self, total_requests=1000):
        """Test queue processing performance"""
        start_time = time.time()
        async with aiohttp.ClientSession() as session:
            # Submit many requests to fill the queue
            tasks = []
            for i in range(total_requests):
                payload = {
                    "proposal": {
                        "entityUrn": f"urn:li:dataset:(urn:li:dataPlatform:mysql,test_table_{i},PROD)",
                        "aspectName": "schemaMetadata",
                        "aspect": {
                            "value": json.dumps({
                                "fields": [
                                    {"fieldPath": "id", "nativeDataType": "int"}
                                ]
                            })
                        }
                    }
                }
                tasks.append(
                    self.measure_endpoint(
                        session, "POST", "/aspects",
                        payload=payload,
                        params={"action": "ingestProposal"}
                    )
                )
            results = await asyncio.gather(*tasks)

        # Monitor queue size through health endpoint until empty or timeout
        timeout = time.time() + 60  # 60 second timeout
        while time.time() < timeout:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}/health") as response:
                    health_data = await response.json()
                    if health_data["queue_size"] == 0:
                        break
            await asyncio.sleep(1)

        processing_time = time.time() - start_time
        self.results["queue_processing"] = {
            "total_requests": total_requests,
            "total_time": processing_time,
            "requests_per_second": total_requests / processing_time
        }

    async def run_full_test(self):
        """Run all performance tests"""
        print("Starting performance tests...")

        # Run all tests
        await self.test_config_endpoint()
        await self.test_ingest_entity()
        await self.test_aspect_query()
        await self.test_queue_processing()

        # Generate report
        report = {
            "timestamp": datetime.now().isoformat(),
            "results": self.results
        }

        # Print and save results
        print("\nPerformance Test Results:")
        print(json.dumps(report, indent=2))

        with open(f"performance_test_results_{int(time.time())}.json", "w") as f:
            json.dump(report, f, indent=2)

        return report

async def main():
    tester = AsyncLiteGMSLoadTest()
    await tester.run_full_test()

if __name__ == "__main__":
    asyncio.run(main())