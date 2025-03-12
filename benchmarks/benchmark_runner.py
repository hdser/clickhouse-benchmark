#!/usr/bin/env python3
"""
Generic benchmark runner that can be extended for different databases.
"""
import time
import statistics
import logging
import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Union

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('benchmark_runner')


@dataclass
class QueryBenchmark:
    """Definition of a benchmark query."""
    name: str
    query: str
    description: str
    run_count: int = 3
    results: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class BenchmarkResult:
    """Results of a benchmark run."""
    query_name: str
    execution_time: float
    memory_usage: float
    rows_read: int
    bytes_read: int
    rows_returned: int
    query: str
    additional_metrics: Dict[str, Any] = field(default_factory=dict)


class BenchmarkRunner(ABC):
    """Abstract base class for database benchmarks."""
    
    def __init__(self, db_name: str):
        """Initialize the benchmark runner."""
        self.db_name = db_name
        self.benchmarks: List[QueryBenchmark] = []
        self.connected = False
        logger.info(f"Initialized {db_name} benchmark runner")
    
    @abstractmethod
    def connect(self, **connection_params) -> bool:
        """Connect to the database. To be implemented by subclasses."""
        pass
    
    @abstractmethod
    def _run_benchmark_query(self, name: str, query: str) -> Dict[str, Any]:
        """Run a single benchmark query and measure performance. To be implemented by subclasses."""
        pass
    
    def add_benchmark(self, name: str, query: str, description: str, run_count: int = 3):
        """Add a benchmark query to the list."""
        self.benchmarks.append(QueryBenchmark(name, query, description, run_count))
        logger.info(f"Added benchmark: {name}")
    
    def add_benchmark_from_dict(self, benchmark_dict: Dict[str, Any]):
        """Add a benchmark from a dictionary definition."""
        name = benchmark_dict.get("name")
        query = benchmark_dict.get("query")
        description = benchmark_dict.get("description", "")
        run_count = benchmark_dict.get("run_count", 3)
        
        if not name or not query:
            logger.error("Benchmark missing required name or query fields")
            return
        
        self.add_benchmark(name, query, description, run_count)
    
    def add_benchmarks_from_list(self, benchmarks: List[Dict[str, Any]]):
        """Add multiple benchmarks from a list of dictionary definitions."""
        for benchmark in benchmarks:
            self.add_benchmark_from_dict(benchmark)
    
    def run_all_benchmarks(self):
        """Run all benchmark queries."""
        if not self.connected:
            logger.error("Not connected to database. Call connect() first.")
            return None
        
        logger.info(f"Starting {self.db_name} benchmark run...")
        
        for benchmark in self.benchmarks:
            logger.info(f"Running benchmark: {benchmark.name}")
            logger.info(f"Description: {benchmark.description}")
            
            for i in range(benchmark.run_count):
                logger.info(f"Run {i+1}/{benchmark.run_count}")
                result = self._run_benchmark_query(benchmark.name, benchmark.query)
                benchmark.results.append(result)
        
        logger.info("All benchmarks completed")
        return self.format_results()
    
    def format_results(self) -> Dict[str, Any]:
        """Format benchmark results into a structured report."""
        results = {
            "database": self.db_name,
            "benchmark_summary": [],
            "detailed_results": {}
        }
        
        for benchmark in self.benchmarks:
            # Calculate stats
            execution_times = [r["execution_time"] for r in benchmark.results]
            memory_usages = [r["memory_usage"] for r in benchmark.results]
            rows_read = [r["rows_read"] for r in benchmark.results]
            bytes_read = [r["bytes_read"] for r in benchmark.results]
            
            # Process additional metrics if available
            written_rows = [r["additional_metrics"].get("written_rows", 0) for r in benchmark.results]
            written_bytes = [r["additional_metrics"].get("written_bytes", 0) for r in benchmark.results]
            result_bytes = [r["additional_metrics"].get("result_bytes", 0) for r in benchmark.results]
            
            avg_time = statistics.mean(execution_times) if execution_times else 0
            avg_memory = statistics.mean(memory_usages) if memory_usages else 0
            avg_rows_read = statistics.mean(rows_read) if rows_read else 0
            avg_bytes_read = statistics.mean(bytes_read) if bytes_read else 0
            avg_written_rows = statistics.mean(written_rows) if written_rows else 0
            avg_written_bytes = statistics.mean(written_bytes) if written_bytes else 0
            avg_result_bytes = statistics.mean(result_bytes) if result_bytes else 0
            
            if len(execution_times) > 1:
                std_dev_time = statistics.stdev(execution_times)
                std_dev_memory = statistics.stdev(memory_usages) if any(memory_usages) else 0
                std_dev_rows_read = statistics.stdev(rows_read) if any(rows_read) else 0
                std_dev_bytes_read = statistics.stdev(bytes_read) if any(bytes_read) else 0
            else:
                std_dev_time = 0
                std_dev_memory = 0
                std_dev_rows_read = 0
                std_dev_bytes_read = 0
            
            # Add summary
            results["benchmark_summary"].append({
                "name": benchmark.name,
                "description": benchmark.description,
                "avg_execution_time": avg_time,
                "std_dev_time": std_dev_time,
                "avg_memory_usage": avg_memory,
                "std_dev_memory": std_dev_memory,
                "avg_rows_read": avg_rows_read,
                "std_dev_rows_read": std_dev_rows_read,
                "avg_bytes_read": avg_bytes_read,
                "std_dev_bytes_read": std_dev_bytes_read,
                "avg_written_rows": avg_written_rows,
                "avg_written_bytes": avg_written_bytes,
                "avg_result_bytes": avg_result_bytes,
                "runs": len(benchmark.results)
            })
            
            # Add detailed results
            results["detailed_results"][benchmark.name] = benchmark.results
        
        return results
    
    def save_results_to_file(self, results: Dict[str, Any], filename: str):
        """Save benchmark results to a JSON file."""
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"Results saved to {filename}")
    
    def print_summary_table(self, results: Dict[str, Any]):
        """Print a formatted summary table of benchmark results."""
        summary = results["benchmark_summary"]
        detailed_results = results["detailed_results"]
        
        # Print header
        print("\n" + "=" * 120)
        print(f"{'Query Name':<25} | {'Avg Time (s)':<12} | {'Memory':<12} | {'Rows Read':<12} | {'Data Read':<12} | {'Rows Written':<12} | {'Description':<30}")
        print("-" * 120)
        
        # Print rows
        for item in summary:
            name = item["name"]
            avg_time = f"{item['avg_execution_time']:.4f}"
            avg_mem = self._format_bytes(item['avg_memory_usage'])
            avg_rows_read = f"{item['avg_rows_read']:,.0f}"
            avg_bytes_read = self._format_bytes(item['avg_bytes_read'])
            avg_written_rows = f"{item['avg_written_rows']:,.0f}"
            desc = item["description"][:30]
            
            print(f"{name[:25]:<25} | {avg_time:<12} | {avg_mem:<12} | {avg_rows_read:<12} | {avg_bytes_read:<12} | {avg_written_rows:<12} | {desc:<30}")
        
        print("=" * 120 + "\n")
        
        # Print expanded metrics table
        print("\nEXPANDED METRICS\n" + "=" * 120)
        print(f"{'Query Name':<25} | {'Data Written':<12} | {'Result Rows':<12} | {'Result Bytes':<12} | {'Runs':<5}")
        print("-" * 120)
        
        # Print rows
        for item in summary:
            name = item["name"]
            avg_written_bytes = self._format_bytes(item['avg_written_bytes'])
            avg_result_rows = f"{item.get('avg_rows_returned', 0):,.0f}"
            avg_result_bytes = self._format_bytes(item['avg_result_bytes'])
            runs = item["runs"]
            
            print(f"{name[:25]:<25} | {avg_written_bytes:<12} | {avg_result_rows:<12} | {avg_result_bytes:<12} | {runs:<5}")
        
        print("=" * 120 + "\n")
        
        # Check for and print information about failed queries
        failed_queries = []
        for name, results_list in detailed_results.items():
            for result in results_list:
                if "additional_metrics" in result and "error" in result["additional_metrics"]:
                    failed_queries.append({
                        "name": name,
                        "error": result["additional_metrics"]["error"],
                        "error_message": result["additional_metrics"].get("error_message", "Unknown error"),
                        "error_details": result["additional_metrics"].get("error_details", {}),
                        "optimization_hints": result["additional_metrics"].get("optimization_hints", [])
                    })
        
        if failed_queries:
            print("\nFAILED QUERIES\n" + "=" * 120)
            for i, failure in enumerate(failed_queries):
                print(f"Query #{i+1}: {failure['name']}")
                print(f"Error Type: {failure['error']}")
                
                # Print error details if available
                if failure.get("error_details"):
                    print("Error Details:")
                    for key, value in failure["error_details"].items():
                        print(f"  - {key}: {value}")
                
                # Print optimization hints if available
                if failure.get("optimization_hints"):
                    print("Optimization Suggestions:")
                    for hint in failure["optimization_hints"]:
                        print(f"  - {hint}")
                
                # Print a shortened version of the error message
                error_msg = failure["error_message"]
                if len(error_msg) > 200:
                    error_msg = error_msg[:197] + "..."
                print(f"Error Message: {error_msg}")
                
                # Add a separator between failed queries
                if i < len(failed_queries) - 1:
                    print("-" * 80)
            
            print("=" * 120 + "\n")
    
    def _format_bytes(self, size_bytes: Union[int, float, str]) -> str:
        """Format bytes to human-readable format."""
        # Convert to float if it's a string
        if isinstance(size_bytes, str):
            try:
                size_bytes = float(size_bytes)
            except (ValueError, TypeError):
                return "0B"  # Return zero if conversion fails
        
        # Handle zero or None values
        if not size_bytes:
            return "0B"
            
        size_name = ("B", "KB", "MB", "GB", "TB", "PB")
        i = 0
        while size_bytes >= 1024 and i < len(size_name) - 1:
            size_bytes /= 1024
            i += 1
        return f"{size_bytes:.2f} {size_name[i]}"