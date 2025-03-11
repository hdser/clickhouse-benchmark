#!/usr/bin/env python3
"""
Example script to run the Nebula benchmark.
"""
import os
import argparse
import logging
from dotenv import load_dotenv

from benchmarks import ClickHouseBenchmark
from benchmarks.query_definitions import NebulaBenchmarks

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('nebula_benchmark')


def main():
    """Run the Nebula benchmark."""
    # Load environment variables from .env file
    load_dotenv()
    
    parser = argparse.ArgumentParser(description='ClickHouse Nebula Benchmark Tool')
    parser.add_argument('--host', help='ClickHouse host address (overrides env variable)')
    parser.add_argument('--port', type=int, help='ClickHouse port (overrides env variable)')
    parser.add_argument('--username', help='ClickHouse username (overrides env variable)')
    parser.add_argument('--password', help='ClickHouse password (overrides env variable)')
    parser.add_argument('--database', help='ClickHouse database (overrides env variable)')
    parser.add_argument('--secure', type=bool, help='Use secure connection (overrides env variable)')
    parser.add_argument('--output', default='nebula_benchmark_results.json', 
                        help='Output file for results')
    parser.add_argument('--runs', type=int, default=3, help='Number of runs per benchmark')
    parser.add_argument('--env-file', default='.env', help='Path to environment file')
    parser.add_argument('--table-info', action='store_true', help='Show table information before running benchmarks')
    
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
    
    # Show table information if requested
    if args.table_info:
        table_info = benchmark.benchmark_table_info(database)
        print("\n===== Table Information =====")
        for table_name, info in table_info.items():
            print(f"\nTable: {table_name}")
            print(f"Size: {info['size_human']} ({info['total_rows']:,} rows)")
            print(f"Created: {info['creation_time']}")
            print(f"Last Modified: {info['last_modified']}")
            print(f"Columns: {len(info['columns'])}")
            
            # Print first few columns
            if info['columns']:
                print("\nColumn Sample:")
                for i, col in enumerate(info['columns'][:5]):
                    print(f"  {col['name']} ({col['type']})")
                if len(info['columns']) > 5:
                    print(f"  ... and {len(info['columns']) - 5} more columns")
        print("\n=============================\n")
    
    # Add benchmark queries
    nebula_benchmarks = NebulaBenchmarks()
    benchmark.add_benchmarks_from_list(nebula_benchmarks.get_queries())
    
    # Update run count if specified
    if args.runs != 3:
        for bm in benchmark.benchmarks:
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