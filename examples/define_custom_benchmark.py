#!/usr/bin/env python3
"""
Example script showing how to define and run custom benchmarks.
"""
import os
import argparse
import logging
from dotenv import load_dotenv

from benchmarks import ClickHouseBenchmark
from benchmarks.query_definitions import CustomBenchmarks

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('custom_benchmark')


def main():
    """Run a custom benchmark."""
    # Load environment variables from .env file
    load_dotenv()
    
    parser = argparse.ArgumentParser(description='ClickHouse Custom Benchmark Example')
    parser.add_argument('--host', help='ClickHouse host address (overrides env variable)')
    parser.add_argument('--port', type=int, help='ClickHouse port (overrides env variable)')
    parser.add_argument('--username', help='ClickHouse username (overrides env variable)')
    parser.add_argument('--password', help='ClickHouse password (overrides env variable)')
    parser.add_argument('--database', help='ClickHouse database (overrides env variable)')
    parser.add_argument('--secure', type=bool, help='Use secure connection (overrides env variable)')
    parser.add_argument('--output', default='custom_benchmark_results.json', 
                        help='Output file for results')
    parser.add_argument('--runs', type=int, default=3, help='Number of runs per benchmark')
    parser.add_argument('--env-file', default='.env', help='Path to environment file')
    
    args = parser.parse_args()
    
    # Load from specified env file if provided
    if args.env_file and args.env_file != '.env':
        load_dotenv(args.env_file)
    
    # Get connection details from environment variables with command line overrides
    host = args.host or os.getenv('CLICKHOUSE_HOST')
    port = args.port or int(os.getenv('CLICKHOUSE_PORT', 8443))
    username = args.username or os.getenv('CLICKHOUSE_USER', 'default')
    password = args.password or os.getenv('CLICKHOUSE_PASSWORD', '')
    database = args.database or os.getenv('CLICKHOUSE_DATABASE', 'default')
    secure_str = os.getenv('CLICKHOUSE_SECURE', 'true').lower()
    secure = args.secure if args.secure is not None else (secure_str == 'true')
    
    # Validate required parameters
    if not host:
        raise ValueError("ClickHouse host not provided. Set CLICKHOUSE_HOST in .env file or use --host argument.")
    
    # Initialize benchmark
    benchmark = ClickHouseBenchmark()
    connected = benchmark.connect(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        secure=secure
    )
    
    if not connected:
        logger.error("Failed to connect to ClickHouse. Exiting.")
        return 1
    
    # Create a custom benchmark collection
    custom = CustomBenchmarks(
        name="my_custom_benchmarks",
        description="My custom benchmark queries for specific use cases"
    )
    
    # Add custom queries
    custom.add_query(
        name="simple_system_query",
        description="Simple query to test system tables",
        query="SELECT * FROM system.numbers LIMIT 1000"
    )
    
    custom.add_query(
        name="large_aggregation",
        description="Test aggregation performance",
        query="""
        SELECT 
            toStartOfDay(visit_started_at) as day,
            COUNT(*) as visits,
            COUNT(DISTINCT peer_id) as unique_peers,
            AVG(length(protocols)) as avg_protocols
        FROM visits
        WHERE visit_started_at >= NOW() - INTERVAL 90 DAY
        GROUP BY day
        ORDER BY day DESC
        """,
        run_count=2  # Override default run count for this query
    )
    
    # Add a query to test memory usage with large result sets
    custom.add_query(
        name="memory_test_large_result",
        description="Test memory usage with large result sets",
        query="SELECT * FROM visits LIMIT 100000"
    )
    
    # Add a query with complex joins to test query planning
    custom.add_query(
        name="complex_join_test",
        description="Test query planning with complex joins",
        query="""
        WITH 
            crawl_stats AS (
                SELECT 
                    id, 
                    created_at,
                    crawled_peers,
                    dialable_peers,
                    undialable_peers
                FROM crawls
                WHERE state = 'succeeded'
                ORDER BY created_at DESC
                LIMIT 10
            )
        SELECT 
            cs.id as crawl_id,
            cs.created_at,
            cs.crawled_peers,
            COUNT(DISTINCT v.peer_id) as unique_peers,
            COUNT(DISTINCT n.neighbor_discovery_id_prefix) as neighbor_count
        FROM crawl_stats cs
        LEFT JOIN visits v ON cs.id = v.crawl_id
        LEFT JOIN neighbors n ON cs.id = n.crawl_id
        GROUP BY cs.id, cs.created_at, cs.crawled_peers, cs.dialable_peers, cs.undialable_peers
        ORDER BY cs.created_at DESC
        """
    )
    
    # Add benchmarks to the runner
    benchmark.add_benchmarks_from_list(custom.get_queries())
    
    # Update run count if specified
    if args.runs != 3:
        for bm in benchmark.benchmarks:
            if bm.run_count == 3:  # Only update if using default
                bm.run_count = args.runs
    
    # Run benchmarks
    results = benchmark.run_all_benchmarks()
    
    # Save and print results
    benchmark.save_results_to_file(results, args.output)
    benchmark.print_summary_table(results)
    
    logger.info(f"Benchmark results saved to {args.output}")
    return 0


if __name__ == "__main__":
    exit(main())