#!/usr/bin/env python3
"""
ClickHouse-specific implementation of the benchmark runner.
"""
import time
import logging
from typing import Dict, Any, Optional
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
    
    def _run_benchmark_query(self, name: str, query: str) -> Dict[str, Any]:
        """Run a single benchmark query and measure performance."""
        if not self.client:
            logger.error("ClickHouse client not initialized. Call connect() first.")
            return {}
        
        # Execute a query to reset query cache
        self.client.command("SYSTEM DROP MARK CACHE")
        self.client.command("SYSTEM DROP UNCOMPRESSED CACHE")
        
        # Set up profiling
        self.client.command("SET log_queries=1")
        self.client.command("SET log_query_threads=1")
        
        # Run the query with timing
        start_time = time.time()
        try:
            result = self.client.query(query)
            execution_time = time.time() - start_time
            rows_returned = len(result.result_rows)
            query_id = result.query_id
            
            logger.debug(f"Query ID: {query_id}")
            
            
            
            # Try to get query stats a few times
            stats = None
            max_attempts = 3
            for attempt in range(max_attempts):
                # Wait for the query log to be updated (small delay)
                time.sleep(10)

                stats_query = f"""
                SELECT 
                    memory_usage,
                    read_rows,
                    read_bytes,
                    result_rows,
                    query
                FROM system.query_log
                WHERE query_id = '{query_id}' AND type = 'QueryFinish'
                LIMIT 1
                """
                
                try:
                    stats_result = self.client.query(stats_query)
                    stats = stats_result.result_rows
                    print('====== ',query_id)
                    print(stats)
                    if stats:
                        logger.debug(f"Got query stats on attempt {attempt + 1}")
                        break
                        
                    if attempt < max_attempts - 1:
                        logger.debug(f"No stats yet, retrying in {(attempt + 1) * 0.5}s")
                        time.sleep((attempt + 1) * 0.5)  # Increasing delay
                except Exception as e:
                    logger.warning(f"Failed to get query stats: {e}")
                    stats = None
                    
            if stats:
                memory_usage = int(stats[0][0]) if stats[0][0] is not None else 0
                rows_read = int(stats[0][1]) if stats[0][1] is not None else 0
                bytes_read = int(stats[0][2]) if stats[0][2] is not None else 0
                rows_returned = int(stats[0][3]) if stats[0][3] is not None else 0
                executed_query = stats[0][4]
            else:
                # Fallback if stats not available
                memory_usage = 0
                rows_read = 0
                bytes_read = 0
                rows_returned = len(result.result_rows)
                executed_query = query
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Query failed: {e}")
            
            # Check if it's a memory limit error
            if "MEMORY_LIMIT_EXCEEDED" in str(e):
                return BenchmarkResult(
                    query_name=name,
                    execution_time=execution_time,
                    memory_usage=0,  # We don't know how much it tried to use
                    rows_read=0,
                    bytes_read=0,
                    rows_returned=0,
                    query=query,
                    additional_metrics={
                        "error": "MEMORY_LIMIT_EXCEEDED",
                        "error_message": str(e)
                    }
                ).__dict__
            
            # Other error types
            return BenchmarkResult(
                query_name=name,
                execution_time=execution_time,
                memory_usage=0,
                rows_read=0,
                bytes_read=0,
                rows_returned=0,
                query=query,
                additional_metrics={
                    "error": "ERROR",
                    "error_message": str(e)
                }
            ).__dict__
                
        logger.info(f"Query executed in {execution_time:.4f} seconds")
        logger.info(f"Memory usage: {self._format_bytes(memory_usage)}")
        rows_read = int(rows_read) if rows_read is not None else 0
        logger.info(f"Rows read: {rows_read:,}")
        logger.info(f"Data read: {self._format_bytes(bytes_read)}")
        logger.info(f"Rows returned: {rows_returned:,}")
        
        return BenchmarkResult(
            query_name=name,
            execution_time=execution_time,
            memory_usage=memory_usage,
            rows_read=rows_read,
            bytes_read=bytes_read,
            rows_returned=rows_returned,
            query=executed_query
        ).__dict__
    
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