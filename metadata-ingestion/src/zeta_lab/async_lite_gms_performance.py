import asyncio
import json
import statistics
import sys
import time
from datetime import datetime
from urllib.parse import quote

import aiohttp


class AsyncLiteGMSLoadTestV2:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.results = {}
        self.metrics_history = {
            'queue_sizes': [],
            'latencies': {},
            'db_latencies': [],
        }

    def generate_test_payloads(self):
        """Generate test payloads for different endpoints"""
        timestamp = int(time.time())
        test_urn = f"urn:li:dataset:(urn:li:dataPlatform:mysql,test_table_{timestamp},PROD)"
        encoded_urn = quote(test_urn)

        return {
            "entities": {
                "method": "POST",
                "endpoint": "/entities",
                "params": {"action": "ingest"},
                "payload": {
                    "entity": {
                        "value": {
                            "dataset": {
                                "urn": test_urn,
                                "aspects": [
                                    {
                                        "SchemaMetadata": {
                                            "fields": [
                                                {"fieldPath": "id", "nativeDataType": "int"},
                                                {"fieldPath": "name", "nativeDataType": "varchar"},
                                                {"fieldPath": "value", "nativeDataType": "double"}
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            },
            "aspects": {
                "method": "POST",
                "endpoint": "/aspects",
                "params": {"action": "ingestProposal"},
                "payload": {
                    "proposal": {
                        "entityUrn": test_urn,
                        "aspectName": "datasetProperties",
                        "aspect": {
                            "value": json.dumps({
                                "description": "Test dataset description",
                                "customProperties": {
                                    "team": "data_platform",
                                    "sla": "tier_1"
                                }
                            })
                        }
                    }
                }
            },
            "get_aspect": {
                "method": "GET",
                "endpoint": f"/aspects/{encoded_urn}",
                "params": {
                    "aspect": "datasetProperties",
                    "version": "0"
                },
                "payload": None
            },
            "graphql": {
                "method": "POST",
                "endpoint": "/api/graphql",
                "params": None,
                "payload": {
                    "query": """
                        query scrollAcrossEntities($input: String) {
                            scrollAcrossEntities(input: $input) {
                                entities {
                                    urn
                                    type
                                }
                            }
                        }
                    """,
                    "variables": {
                        "orFilters": [{
                            "and": [{
                                "field": "platform.keyword",
                                "value": "urn:li:dataPlatform:mysql"
                            }, {
                                "field": "origin",
                                "value": "PROD"
                            }]
                        }]
                    }
                }
            }
        }

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

    async def run_endpoint_test(self, endpoint_name, config, concurrent_requests=50, duration=30):
        """Run test for a specific endpoint"""
        print(f"\nTesting {endpoint_name} endpoint: {config['endpoint']}")
        start_time = time.time()
        request_count = 0
        latencies = []
        errors = []

        async with aiohttp.ClientSession() as session:
            while time.time() - start_time < duration:
                batch_tasks = []
                for _ in range(concurrent_requests):
                    task = asyncio.create_task(self.measure_endpoint(
                        session,
                        config['method'],
                        config['endpoint'],
                        payload=config['payload'],
                        params=config['params']
                    ))
                    batch_tasks.append(task)
                    request_count += 1

                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                for result in batch_results:
                    if isinstance(result, tuple):
                        duration, status = result
                        if duration is not None:
                            latencies.append(duration)
                        else:
                            errors.append(status)
                    else:
                        errors.append(str(result))

                await asyncio.sleep(0.1)  # Rate limiting

        test_duration = time.time() - start_time
        successful_requests = len(latencies)

        if latencies:  # Ensure we have valid latencies before calculating statistics
            return {
                "endpoint": config['endpoint'],
                "method": config['method'],
                "performance_metrics": {
                    "total_requests": request_count,
                    "successful_requests": successful_requests,
                    "failed_requests": len(errors),
                    "error_rate": f"{(len(errors) / request_count * 100):.2f}%",
                    "throughput": self.format_requests_per_second(successful_requests / test_duration),
                    "latency": {
                        "min": self.format_duration(min(latencies)),
                        "max": self.format_duration(max(latencies)),
                        "avg": self.format_duration(statistics.mean(latencies)),
                        "p50": self.format_duration(statistics.median(latencies)),
                        "p95": self.format_duration(sorted(latencies)[int(len(latencies) * 0.95)])
                    }
                }
            }
        else:
            return {
                "endpoint": config['endpoint'],
                "method": config['method'],
                "performance_metrics": {
                    "total_requests": request_count,
                    "successful_requests": 0,
                    "failed_requests": len(errors),
                    "error_rate": "100.00%",
                    "errors": errors[:5]  # Include first 5 errors for debugging
                }
            }

    def format_duration(self, seconds):
        """Convert seconds to appropriate time unit"""
        if seconds < 0.001:
            return f"{seconds*1000000:.2f}μs"
        elif seconds < 1:
            return f"{seconds*1000:.2f}ms"
        elif seconds < 60:
            return f"{seconds:.2f}s"
        elif seconds < 3600:
            minutes = seconds // 60
            remaining_seconds = seconds % 60
            return f"{int(minutes)}m {remaining_seconds:.2f}s"
        else:
            hours = seconds // 3600
            remaining = seconds % 3600
            minutes = remaining // 60
            seconds = remaining % 60
            return f"{int(hours)}h {int(minutes)}m {seconds:.2f}s"

    def format_requests_per_second(self, rps):
        """Format requests per second"""
        if rps < 1:
            return f"{rps:.2f} req/s"
        elif rps < 1000:
            return f"{int(rps)} req/s"
        else:
            return f"{rps/1000:.1f}k req/s"

    async def run_full_test(self, concurrent_requests=50, duration=30):
        """Run complete performance test suite"""
        print(f"Starting performance test suite with {concurrent_requests} concurrent requests...")
        print(f"Test duration per endpoint: {duration} seconds")

        test_configs = self.generate_test_payloads()
        all_results = {}

        # Test each endpoint
        for endpoint_name, config in test_configs.items():
            try:
                results = await self.run_endpoint_test(
                    endpoint_name,
                    config,
                    concurrent_requests,
                    duration
                )
                all_results[endpoint_name] = results
            except Exception as e:
                print(f"Error testing {endpoint_name}: {e}")
                all_results[endpoint_name] = {
                    "endpoint": config['endpoint'],
                    "method": config['method'],
                    "error": str(e)
                }

        # Get final health check
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}/health") as response:
                    all_results["system_health"] = await response.json()
        except Exception as e:
            print(f"Error getting final health check: {e}")

        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"performance_test_results_{timestamp}.json"
        with open(results_file, "w") as f:
            json.dump(all_results, f, indent=2)

        print(f"\nTest results saved to {results_file}")
        return all_results

async def main():
    try:
        import argparse
        parser = argparse.ArgumentParser(description='Performance Test for async_lite_gms')
        parser.add_argument('--concurrent', type=int, default=50, help='Number of concurrent requests')
        parser.add_argument('--duration', type=float, default=5.0, help='Test duration in minutes')

        args = parser.parse_args()

        tester = AsyncLiteGMSLoadTestV2()
        duration_seconds = int(args.duration * 60)  # Convert minutes to seconds

        print(f"Starting performance test...")
        print(f"Configuration:")
        print(f"- Concurrent requests: {args.concurrent}")
        print(f"- Test duration: {args.duration} minutes ({duration_seconds} seconds)")

        results = await tester.run_full_test(
            concurrent_requests=args.concurrent,
            duration=duration_seconds
        )

        # 결과 출력
        print("\nTest Summary:")
        for endpoint_name, data in results.items():
            if endpoint_name != "system_health":
                print(f"\n{endpoint_name.upper()} Endpoint Results:")
                if "error" in data:
                    print(f"Error: {data['error']}")
                else:
                    metrics = data["performance_metrics"]
                    print(f"Endpoint: {data['endpoint']}")
                    print(f"Method: {data['method']}")
                    print(f"Throughput: {metrics['throughput']}")
                    print(f"Success/Total: {metrics['successful_requests']}/{metrics['total_requests']}")
                    if "latency" in metrics:
                        print(f"Avg Latency: {metrics['latency']['avg']}")
                        print(f"95th Percentile: {metrics['latency']['p95']}")
                    print(f"Error Rate: {metrics['error_rate']}")

        # 최종 시스템 상태 출력
        if "system_health" in results:
            print("\nFinal System Health:")
            health = results["system_health"]
            print(f"Queue Size: {health['metrics']['queue_statistics'].get('current_size', 'N/A')}")
            print(f"Active Workers: {health['worker_pool']['active']}/{health['worker_pool']['size']}")

    except KeyboardInterrupt:
        print("\nTest interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nTest failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())