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
            
            avg_time = statistics.mean(execution_times) if execution_times else 0
            avg_memory = statistics.mean(memory_usages) if memory_usages else 0
            
            if len(execution_times) > 1:
                std_dev_time = statistics.stdev(execution_times)
                std_dev_memory = statistics.stdev(memory_usages) if any(memory_usages) else 0
            else:
                std_dev_time = 0
                std_dev_memory = 0
            
            # Add summary
            results["benchmark_summary"].append({
                "name": benchmark.name,
                "description": benchmark.description,
                "avg_execution_time": avg_time,
                "std_dev_time": std_dev_time,
                "avg_memory_usage": avg_memory,
                "std_dev_memory": std_dev_memory,
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
        
        # Print header
        print("\n" + "=" * 100)
        print(f"{'Query Name':<30} | {'Avg Time (s)':<15} | {'Std Dev (s)':<15} | {'Avg Memory':<15} | {'Description':<30}")
        print("-" * 100)
        
        # Print rows
        for item in summary:
            name = item["name"]
            avg_time = f"{item['avg_execution_time']:.4f}"
            std_dev = f"{item['std_dev_time']:.4f}"
            avg_mem = self._format_bytes(item['avg_memory_usage'])
            desc = item["description"][:30]
            
            print(f"{name:<30} | {avg_time:<15} | {std_dev:<15} | {avg_mem:<15} | {desc:<30}")
        
        print("=" * 100 + "\n")
    
    
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