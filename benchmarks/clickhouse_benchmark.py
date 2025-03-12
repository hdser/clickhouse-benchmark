#!/usr/bin/env python3
"""
ClickHouse-specific implementation of the benchmark runner.
"""
import time
import logging
import re
from typing import Dict, Any, Optional, List
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError

from .benchmark_runner import BenchmarkRunner, BenchmarkResult

logger = logging.getLogger('clickhouse_benchmark')


class ClickHouseBenchmark(BenchmarkRunner):
    """ClickHouse specific benchmark implementation."""
    
    def __init__(self):
        """Initialize the ClickHouse benchmark runner."""
        super().__init__(db_name="ClickHouse")
        self.client: Optional[Client] = None
    
    def connect(self, **connection_params) -> bool:
        """Connect to the ClickHouse database."""
        host = connection_params.get('host')
        port = connection_params.get('port', 8443)
        username = connection_params.get('username', 'default')
        password = connection_params.get('password', '')
        database = connection_params.get('database', 'default')
        secure = connection_params.get('secure', True)
        
        logger.info(f"Connecting to ClickHouse at {host}:{port}")
        try:
            self.client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database,
                secure=secure
            )
            # Test connection
            self.client.command("SELECT 1")
            logger.info("ClickHouse connection established successfully")
            self.connected = True
            return True
        except Exception as e:
            logger.error(f"Error connecting to ClickHouse: {e}")
            self.connected = False
            return False
    
    def _parse_memory_limit(self, limit_str: str) -> int:
        """
        Parse a memory limit string into bytes.
        
        Formats supported:
        - Plain numbers (interpreted as bytes)
        - Numbers with units: KB, MB, GB, TB
        
        Examples:
        - "1024" -> 1024
        - "1KB" -> 1024
        - "5MB" -> 5242880
        - "9GB" -> 9663676416
        """
        # Remove any whitespace
        limit_str = limit_str.strip()
        
        # Check if it's just a number
        if limit_str.isdigit():
            return int(limit_str)
            
        # Parse number with unit
        match = re.match(r'^(\d+(\.\d+)?)\s*([KMGTkmgt]?[Bb]?)$', limit_str)
        if not match:
            logger.warning(f"Invalid memory limit format: {limit_str}. Using default.")
            return 0
            
        value, _, unit = match.groups()
        
        # Convert to float first to handle decimal values
        value = float(value)
        
        # Normalize unit to uppercase
        unit = unit.upper()
        
        # Convert to bytes based on unit
        if unit in ('K', 'KB'):
            value *= 1024
        elif unit in ('M', 'MB'):
            value *= 1024 * 1024
        elif unit in ('G', 'GB'):
            value *= 1024 * 1024 * 1024
        elif unit in ('T', 'TB'):
            value *= 1024 * 1024 * 1024 * 1024
            
        # Return as integer
        return int(value)
    
    def _run_benchmark_queries(self, benchmarks, memory_limits=None) -> List[Dict[str, Any]]:
        """
        Run all benchmark queries in sequence first, save their query IDs,
        then process their statistics from the system.query_log.
        
        Args:
            benchmarks: List of QueryBenchmark objects to run
            memory_limits: Optional dict mapping benchmark names to memory limits in bytes
                           e.g. {"complex_join_test": "4GB"}
        """
        if not self.client:
            logger.error("ClickHouse client not initialized. Call connect() first.")
            return []
            
        # If memory_limits is None, initialize as empty dict
        if memory_limits is None:
            memory_limits = {}
            
        # First, execute all the queries and collect their query IDs
        query_execution_data = []
        
        # Execute a query to reset query cache
        self.client.command("SYSTEM DROP MARK CACHE")
        self.client.command("SYSTEM DROP UNCOMPRESSED CACHE")
        
        # Set up profiling
        self.client.command("SET log_queries=1")
        self.client.command("SET log_query_threads=1")
        
        logger.info("Executing all benchmark queries and collecting query IDs...")
        
        for benchmark in benchmarks:
            name = benchmark.name
            query = benchmark.query
            
            # Check if this benchmark has a specific memory limit
            memory_limit = memory_limits.get(name)
            
            for run in range(benchmark.run_count):
                logger.info(f"Executing benchmark: {name} (Run {run+1}/{benchmark.run_count})")
                
                try:
                    # Apply memory limit if specified for this benchmark
                    if memory_limit:
                        logger.info(f"Setting memory limit for {name}: {memory_limit}")
                        # Format the memory limit properly for ClickHouse
                        # First check if it's already a number
                        try:
                            # If it's already a number (bytes), use it directly
                            bytes_value = int(memory_limit)
                            self.client.command(f"SET max_memory_usage = {bytes_value}")
                        except ValueError:
                            # If it's a string with unit (like "9GB"), parse it
                            memory_limit_parsed = self._parse_memory_limit(memory_limit)
                            self.client.command(f"SET max_memory_usage = {memory_limit_parsed}")
                    
                    # Run the query with timing
                    start_time = time.time()
                    result = self.client.query(query)
                    execution_time = time.time() - start_time
                    
                    query_id = result.query_id
                    rows_returned = len(result.result_rows)
                    
                    logger.info(f"Query executed in {execution_time:.4f} seconds, ID: {query_id}")
                    
                    query_execution_data.append({
                        "benchmark_name": name,
                        "query": query,
                        "query_id": query_id,
                        "execution_time": execution_time,
                        "rows_returned": rows_returned,
                        "run": run
                    })
                    
                    # Reset memory limit if we set one
                    if memory_limit:
                        self.client.command("SET max_memory_usage = 0")  # Reset to default
                except Exception as e:
                    execution_time = time.time() - start_time
                    error_msg = str(e)
                    
                    # Analyze the error type more specifically
                    if "MEMORY_LIMIT_EXCEEDED" in error_msg:
                        error_type = "MEMORY_LIMIT_EXCEEDED"
                        
                        # Extract memory limit details if possible
                        memory_details = {}
                        
                        # Try to extract requested memory
                        would_use_match = re.search(r'would use ([\d\.]+) ([KMGTPiB]+)', error_msg)
                        if would_use_match:
                            memory_details["requested_memory"] = f"{would_use_match.group(1)} {would_use_match.group(2)}"
                        
                        # Try to extract current RSS
                        current_rss_match = re.search(r'current RSS ([\d\.]+) ([KMGTPiB]+)', error_msg)
                        if current_rss_match:
                            memory_details["current_rss"] = f"{current_rss_match.group(1)} {current_rss_match.group(2)}"
                        
                        # Try to extract maximum memory
                        max_memory_match = re.search(r'maximum: ([\d\.]+) ([KMGTPiB]+)', error_msg)
                        if max_memory_match:
                            memory_details["maximum_memory"] = f"{max_memory_match.group(1)} {max_memory_match.group(2)}"
                        
                        logger.error(f"Memory limit exceeded: {memory_details}")
                        
                        # Add the memory details to the error message
                        error_details = memory_details
                    else:
                        error_type = "ERROR"
                        # Try to extract any error code information
                        code_match = re.search(r'error code (\d+)', error_msg)
                        if code_match:
                            error_details = {"error_code": code_match.group(1)}
                        else:
                            error_details = {}
                    
                    logger.error(f"Query failed: {error_type} - {error_msg}")
                    
                    # Add error entry with enhanced details
                    query_execution_data.append({
                        "benchmark_name": name,
                        "query": query,
                        "query_id": None,
                        "execution_time": execution_time,
                        "rows_returned": 0,
                        "run": run,
                        "error": error_type,
                        "error_message": error_msg,
                        "error_details": error_details
                    })
                
                # Brief pause between queries
                time.sleep(1)
                
        # Allow some time for query log to be updated
        logger.info("Waiting for query log to be updated...")
        time.sleep(10)
        
        # Now process all the query logs to get the statistics
        results = []
        for exec_data in query_execution_data:
            if exec_data.get("error"):
                # For failed queries, create a result with enhanced error information
                additional_metrics = {
                    "error": exec_data["error"],
                    "error_message": exec_data["error_message"],
                    "written_rows": 0,
                    "written_bytes": 0,
                    "result_bytes": 0
                }
                
                # Add any extracted error details
                if "error_details" in exec_data:
                    additional_metrics["error_details"] = exec_data["error_details"]
                
                # For memory limit errors, add a suggested optimization hint
                if exec_data["error"] == "MEMORY_LIMIT_EXCEEDED":
                    # Calculate memory usage as the difference between requested and available memory if possible
                    if "error_details" in exec_data and "requested_memory" in exec_data["error_details"]:
                        additional_metrics["memory_usage_estimated"] = True
                        additional_metrics["optimization_hints"] = [
                            "Consider adding LIMIT to reduce result set size",
                            "Break down the query into smaller parts",
                            "Use WITH clauses for complex subqueries",
                            "Add more specific WHERE conditions",
                            "Consider using approximate functions like approxDistinct()",
                            "Reduce the number of columns in SELECT"
                        ]
                
                result = BenchmarkResult(
                    query_name=exec_data["benchmark_name"],
                    execution_time=exec_data["execution_time"],
                    memory_usage=0,
                    rows_read=0,
                    bytes_read=0,
                    rows_returned=0,
                    query=exec_data["query"],
                    additional_metrics=additional_metrics
                ).__dict__
                results.append(result)
                continue
                
            # Get query stats for successful queries
            stats = self._get_query_stats(exec_data["query_id"])
            
            if stats:
                # Get the stats values, ensuring we handle None values
                memory_usage = int(stats.get("memory_usage") or 0)
                rows_read = int(stats.get("read_rows") or 0)
                bytes_read = int(stats.get("read_bytes") or 0)
                written_rows = int(stats.get("written_rows") or 0)
                written_bytes = int(stats.get("written_bytes") or 0)
                result_rows = int(stats.get("result_rows") or exec_data["rows_returned"] or 0)
                result_bytes = int(stats.get("result_bytes") or 0)
                
                logger.info(f"Stats for query {exec_data['query_id']}:")
                logger.info(f"Memory usage: {self._format_bytes(memory_usage)}")
                logger.info(f"Rows read: {rows_read:,}")
                logger.info(f"Data read: {self._format_bytes(bytes_read)}")
                logger.info(f"Rows written: {written_rows:,}")
                logger.info(f"Data written: {self._format_bytes(written_bytes)}")
                logger.info(f"Result rows: {result_rows:,}")
                logger.info(f"Result bytes: {self._format_bytes(result_bytes)}")
                
                # Create benchmark result
                result = BenchmarkResult(
                    query_name=exec_data["benchmark_name"],
                    execution_time=exec_data["execution_time"],
                    memory_usage=memory_usage,
                    rows_read=rows_read,
                    bytes_read=bytes_read,
                    rows_returned=result_rows,
                    query=exec_data["query"],
                    additional_metrics={
                        "written_rows": written_rows,
                        "written_bytes": written_bytes,
                        "result_bytes": result_bytes
                    }
                ).__dict__
            else:
                # Fallback if stats not available
                logger.warning(f"Could not get stats for query ID: {exec_data['query_id']}")
                result = BenchmarkResult(
                    query_name=exec_data["benchmark_name"],
                    execution_time=exec_data["execution_time"],
                    memory_usage=0,
                    rows_read=0,
                    bytes_read=0,
                    rows_returned=exec_data["rows_returned"],
                    query=exec_data["query"],
                    additional_metrics={
                        "written_rows": 0,
                        "written_bytes": 0,
                        "result_bytes": 0,
                        "warning": "Query stats not available"
                    }
                ).__dict__
                
            results.append(result)
            
        return results
        
    def _get_query_stats(self, query_id: str) -> Dict[str, Any]:
        """
        Get query statistics from system.query_log for a specific query_id.
        Retries multiple times with increasing delays if the stats are not yet available.
        Also checks for query exceptions when stats aren't found.
        """
        if not query_id:
            return {}
            
        # Define the query to get the statistics from successful queries
        stats_query = f"""
        SELECT 
            memory_usage,
            read_rows,
            read_bytes,
            written_rows,
            written_bytes,
            result_rows,
            result_bytes,
            query
        FROM system.query_log
        WHERE query_id = '{query_id}' AND type = 'QueryFinish'
        LIMIT 1
        """
        
        # Also define a query to check for exceptions (for failed queries)
        exception_query = f"""
        SELECT 
            exception,
            exception_code,
            initial_query_start_time,
            query_duration_ms,
            peak_memory_usage,
            query
        FROM system.query_log
        WHERE query_id = '{query_id}' AND exception != ''
        LIMIT 1
        """
        
        # Try to get query stats with retries
        max_attempts = 15
        stats = {}
        
        for attempt in range(max_attempts):
            try:
                # First check for normal query finish
                stats_result = self.client.query(stats_query)
                if stats_result.result_rows:
                    row = stats_result.result_rows[0]
                    
                    # Build a dictionary of stats
                    stats = {
                        "memory_usage": row[0],
                        "read_rows": row[1],
                        "read_bytes": row[2],
                        "written_rows": row[3],
                        "written_bytes": row[4],
                        "result_rows": row[5],
                        "result_bytes": row[6],
                        "query": row[7]
                    }
                    
                    logger.debug(f"Got query stats on attempt {attempt + 1}")
                    break
                
                # If no normal finish, check for exceptions
                exception_result = self.client.query(exception_query)
                if exception_result.result_rows:
                    row = exception_result.result_rows[0]
                    
                    # Build a dictionary with exception info and any available stats
                    stats = {
                        "exception": row[0],
                        "exception_code": row[1],
                        "query_start_time": row[2],
                        "query_duration_ms": row[3] / 1000.0,  # Convert to seconds
                        "memory_usage": row[4] or 0,  # peak_memory_usage
                        "read_rows": 0,
                        "read_bytes": 0,
                        "written_rows": 0,
                        "written_bytes": 0,
                        "result_rows": 0,
                        "result_bytes": 0,
                        "query": row[5],
                        "failed": True
                    }
                    
                    logger.debug(f"Found exception info for query {query_id} on attempt {attempt + 1}")
                    break
                    
                if attempt < max_attempts - 1:
                    delay = (attempt + 1) * 0.5  # Increasing delay
                    logger.debug(f"No stats yet for query ID {query_id}, retrying in {delay}s (attempt {attempt + 1}/{max_attempts})")
                    time.sleep(delay)
            except Exception as e:
                logger.warning(f"Failed to get query stats for ID {query_id}: {e}")
                if attempt < max_attempts - 1:
                    time.sleep((attempt + 1) * 0.5)
        
        return stats
    
    def _run_benchmark_query(self, name: str, query: str) -> Dict[str, Any]:
        """
        Run a single benchmark query and measure performance.
        This method is kept for backward compatibility but is now implemented
        in terms of _run_benchmark_queries for batch processing.
        """
        if not self.client:
            logger.error("ClickHouse client not initialized. Call connect() first.")
            return {}
        
        # Create a temporary benchmark object
        from .benchmark_runner import QueryBenchmark
        temp_benchmark = QueryBenchmark(name=name, query=query, description="", run_count=1)
        
        # Use the batch processing method
        results = self._run_benchmark_queries([temp_benchmark])
        
        # Return the first (and only) result
        if results:
            return results[0]
        else:
            # Create a basic error result if something went wrong
            return BenchmarkResult(
                query_name=name,
                execution_time=0,
                memory_usage=0,
                rows_read=0,
                bytes_read=0,
                rows_returned=0,
                query=query,
                additional_metrics={"error": "Failed to execute query"}
            ).__dict__
    
    def run_all_benchmarks(self, memory_limits=None, skip_benchmarks=None):
        """
        Run all benchmark queries using the batch approach.
        
        Args:
            memory_limits: Optional dict mapping benchmark names to memory limits
                           e.g. {"complex_join_test": "4294967296"} (4GB in bytes)
            skip_benchmarks: Optional list of benchmark names to skip
        """
        if not self.connected:
            logger.error("Not connected to database. Call connect() first.")
            return None
        
        logger.info(f"Starting {self.db_name} benchmark run...")
        
        # Filter benchmarks if skip_benchmarks is provided
        benchmarks_to_run = self.benchmarks
        if skip_benchmarks:
            benchmarks_to_run = [b for b in self.benchmarks if b.name not in skip_benchmarks]
            logger.info(f"Skipping benchmarks: {skip_benchmarks}")
        
        # Use the new batch approach with memory limits
        all_results = self._run_benchmark_queries(benchmarks_to_run, memory_limits)
        
        # Organize results by benchmark
        for result in all_results:
            benchmark_name = result["query_name"]
            for benchmark in self.benchmarks:
                if benchmark.name == benchmark_name:
                    benchmark.results.append(result)
                    break
        
        logger.info("All benchmarks completed")
        return self.format_results()
    
    def benchmark_table_info(self, database: Optional[str] = None) -> Dict[str, Any]:
        """Get information about tables in the database."""
        if not self.client:
            logger.error("ClickHouse client not initialized. Call connect() first.")
            return {}
        
        if database:
            query = f"SHOW TABLES FROM {database}"
        else:
            query = "SHOW TABLES"
            
        tables = self.client.query(query).result_rows
        table_info = {}
        
        for table_row in tables:
            table_name = table_row[0]
            
            # Get table size
            size_query = f"""
            SELECT 
                sum(bytes) as size_bytes,
                sum(rows) as total_rows,
                min(modification_time) as creation_time,
                max(modification_time) as last_modified
            FROM system.parts
            WHERE table = '{table_name}' AND active = 1
            """
            
            size_result = self.client.query(size_query).result_rows
            
            # Get table structure
            structure_query = f"DESCRIBE TABLE {table_name}"
            structure_result = self.client.query(structure_query).result_rows
            
            columns = []
            for col in structure_result:
                columns.append({
                    "name": col[0],
                    "type": col[1],
                    "default_type": col[3],
                    "default_expression": col[4]
                })
            
            if size_result:
                size_bytes = size_result[0][0] or 0
                total_rows = size_result[0][1] or 0
                creation_time = size_result[0][2]
                last_modified = size_result[0][3]
            else:
                size_bytes = 0
                total_rows = 0
                creation_time = None
                last_modified = None
            
            table_info[table_name] = {
                "size_bytes": size_bytes,
                "size_human": self._format_bytes(size_bytes),
                "total_rows": total_rows,
                "creation_time": creation_time,
                "last_modified": last_modified,
                "columns": columns
            }
        
        return table_info