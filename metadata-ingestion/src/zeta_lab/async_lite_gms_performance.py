import aiohttp
import asyncio
import time
import statistics
import json
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from typing import Dict, List, Any

class AsyncLiteGMSLoadTestV2:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.results = {}
        self.metrics_history = {
            'queue_sizes': [],
            'latencies': {},
            'db_latencies': [],
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

    async def collect_metrics(self, duration=60):
        """Collect system metrics over time"""
        print(f"\nCollecting system metrics for {duration} seconds...")
        start_time = time.time()
        metrics_data = []

        while time.time() - start_time < duration:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{self.base_url}/health") as response:
                        health_data = await response.json()
                        # Extract and convert metrics to numeric values
                        queue_stats = health_data.get('metrics', {}).get('queue_statistics', {})
                        if isinstance(queue_stats, str):
                            try:
                                queue_size = float(queue_stats.split()[0])  # Extract numeric part
                            except (ValueError, IndexError):
                                queue_size = 0
                        else:
                            queue_size = queue_stats.get('current_size', 0)

                        # Get database latency and convert to float if it's a string
                        db_latency = health_data.get('database', {}).get('metrics', {}).get('avg_operation_time', 0)
                        if isinstance(db_latency, str):
                            try:
                                # Convert ms to seconds if necessary
                                if 'ms' in db_latency:
                                    db_latency = float(db_latency.replace('ms', '')) / 1000
                                else:
                                    db_latency = float(db_latency.replace('s', ''))
                            except (ValueError, AttributeError):
                                db_latency = 0

                        # Get worker pool active count
                        worker_active = health_data.get('worker_pool', {}).get('active', 0)
                        if isinstance(worker_active, str):
                            try:
                                worker_active = int(worker_active)
                            except ValueError:
                                worker_active = 0

                        metrics_data.append({
                            'timestamp': time.time(),
                            'queue_size': float(queue_size),
                            'db_latency': float(db_latency),
                            'worker_active': int(worker_active),
                        })
            except Exception as e:
                print(f"Error collecting metrics: {e}")

            await asyncio.sleep(1)

        return metrics_data

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

    async def run_load_test(self, concurrent_users=50, duration=60):
        """Run a sustained load test with metrics collection"""
        print(f"Starting load test with {concurrent_users} concurrent users for {duration} seconds...")

        # Start metrics collection
        metrics_task = asyncio.create_task(self.collect_metrics(duration))

        # Run load test
        start_time = time.time()
        tasks = []
        request_count = 0
        latencies = []

        async with aiohttp.ClientSession() as session:
            while time.time() - start_time < duration:
                # Generate batch of requests
                batch_tasks = []
                for _ in range(concurrent_users):
                    task = asyncio.create_task(self.measure_endpoint(
                        session, "POST", "/entities",
                        payload=self.generate_test_payload(),
                        params={"action": "ingest"}
                    ))
                    batch_tasks.append(task)
                    request_count += 1

                # Wait for batch completion
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                latencies.extend([r[0] for r in batch_results if r[0] is not None])

                await asyncio.sleep(1)  # Rate limiting

        # Collect metrics
        metrics_data = await metrics_task

        # Process results
        test_duration = time.time() - start_time
        successful_requests = len([l for l in latencies if l is not None])

        return {
            "test_configuration": {
                "concurrent_users": concurrent_users,
                "duration": self.format_duration(test_duration),
                "total_requests": request_count,
                "successful_requests": successful_requests
            },
            "performance_metrics": {
                "throughput": self.format_requests_per_second(successful_requests / test_duration),
                "latency": {
                    "min": self.format_duration(min(latencies)),
                    "max": self.format_duration(max(latencies)),
                    "avg": self.format_duration(statistics.mean(latencies)),
                    "p50": self.format_duration(statistics.median(latencies)),
                    "p95": self.format_duration(sorted(latencies)[int(len(latencies) * 0.95)])
                }
            },
            "system_metrics": {
                "queue_size": {
                    "current": f"{metrics_data[-1]['queue_size']:.2f}",
                    "avg": f"{statistics.mean([m['queue_size'] for m in metrics_data]):.2f}",
                    "max": f"{max([m['queue_size'] for m in metrics_data]):.2f}"
                },
                "db_latency": {
                    "avg": self.format_duration(statistics.mean([m['db_latency'] for m in metrics_data])),
                    "max": self.format_duration(max([m['db_latency'] for m in metrics_data]))
                },
                "worker_pool": {
                    "avg_active": f"{statistics.mean([m['worker_active'] for m in metrics_data]):.2f}",
                    "max_active": f"{max([m['worker_active'] for m in metrics_data])}"
                }
            }
        }

    def generate_test_payload(self):
        """Generate test payload for entities endpoint"""
        return {
            "entity": {
                "value": {
                    "dataset": {
                        "urn": f"urn:li:dataset:(urn:li:dataPlatform:mysql,test_table_{int(time.time())},PROD)",
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

    def plot_metrics(self, metrics_data: List[Dict[str, Any]], output_file: str):
        """Generate performance visualization"""
        df = pd.DataFrame(metrics_data)
        df['timestamp'] = df['timestamp'] - df['timestamp'].min()

        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 12))

        # Queue Size
        ax1.plot(df['timestamp'], df['queue_size'])
        ax1.set_title('Queue Size Over Time')
        ax1.set_xlabel('Time (s)')
        ax1.set_ylabel('Queue Size')
        ax1.grid(True)

        # DB Latency
        ax2.plot(df['timestamp'], df['db_latency'] * 1000)  # Convert to ms
        ax2.set_title('Database Operation Latency')
        ax2.set_xlabel('Time (s)')
        ax2.set_ylabel('Latency (ms)')
        ax2.grid(True)

        # Active Workers
        ax3.plot(df['timestamp'], df['worker_active'])
        ax3.set_title('Active Workers')
        ax3.set_xlabel('Time (s)')
        ax3.set_ylabel('Count')
        ax3.grid(True)

        plt.tight_layout()
        plt.savefig(output_file)
        plt.close()

    async def run_full_test(self, concurrent_users=[10, 50, 100], duration=60):
        """Run complete performance test suite"""
        print("Starting performance test suite...")

        all_results = {}

        for users in concurrent_users:
            print(f"\nTesting with {users} concurrent users...")
            results = await self.run_load_test(users, duration)
            all_results[f"concurrent_users_{users}"] = results

        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"performance_test_results_{timestamp}.json"

        with open(results_file, "w") as f:
            json.dump(all_results, f, indent=2)

        print(f"\nTest results saved to {results_file}")
        return all_results

async def main():
    tester = AsyncLiteGMSLoadTestV2()
    results = await tester.run_full_test()
    print("\nTest Summary:")
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    asyncio.run(main())