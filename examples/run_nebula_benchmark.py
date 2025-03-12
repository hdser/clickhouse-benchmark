#!/usr/bin/env python3
"""
Example script to run the Nebula benchmark.
"""
import os
import argparse
import logging
import json
from dotenv import load_dotenv

from benchmarks import ClickHouseBenchmark
from benchmarks.query_definitions import NebulaBenchmarks

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('nebula_benchmark')


def parse_memory_limits(memory_limits_file):
    """Parse a memory limits JSON file or string."""
    if not memory_limits_file:
        return {}
        
    if os.path.exists(memory_limits_file):
        with open(memory_limits_file, 'r') as f:
            return json.load(f)
    else:
        try:
            return json.loads(memory_limits_file)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON format for memory limits: {memory_limits_file}")
            return {}


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
    parser.add_argument('--memory-limits', help='JSON file or string with memory limits for specific benchmarks')
    parser.add_argument('--skip-benchmarks', help='Comma-separated list of benchmark names to skip')
    parser.add_argument('--retry-failed', help='JSON file with previous results to retry only failed benchmarks')
    
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
    
    # Parse memory limits if provided
    memory_limits = parse_memory_limits(args.memory_limits) if args.memory_limits else {}
    
    # Parse skip benchmarks
    skip_benchmarks = []
    if args.skip_benchmarks:
        skip_benchmarks = [name.strip() for name in args.skip_benchmarks.split(',')]
        
    # Handle retry failed benchmarks if a previous result file is provided
    if args.retry_failed and os.path.exists(args.retry_failed):
        try:
            with open(args.retry_failed, 'r') as f:
                previous_results = json.load(f)
                
            # Find which benchmarks failed
            failed_benchmarks = []
            for name, results in previous_results.get("detailed_results", {}).items():
                if any("error" in result.get("additional_metrics", {}) for result in results):
                    failed_benchmarks.append(name)
                    
            if failed_benchmarks:
                logger.info(f"Retrying previously failed benchmarks: {', '.join(failed_benchmarks)}")
                # Add these to the skip list (we'll invert it below)
                skip_benchmarks.extend([name for name in previous_results.get("detailed_results", {}).keys() 
                                      if name not in failed_benchmarks])
        except Exception as e:
            logger.error(f"Failed to process retry file: {e}")
    
    # Add benchmark queries
    nebula_benchmarks = NebulaBenchmarks()
    benchmark.add_benchmarks_from_list(nebula_benchmarks.get_queries())
    
    # Update run count if specified
    if args.runs != 3:
        for bm in benchmark.benchmarks:
            bm.run_count = args.runs
    
    # Run benchmarks
    results = benchmark.run_all_benchmarks(memory_limits=memory_limits, skip_benchmarks=skip_benchmarks)
    
    # Save and print results
    benchmark.save_results_to_file(results, args.output)
    benchmark.print_summary_table(results)
    
    logger.info(f"Benchmark results saved to {args.output}")
    
    # Check if any benchmarks failed, use as exit code
    failed = False
    for benchmark_summary in results.get("benchmark_summary", []):
        if benchmark_summary.get("error_count", 0) > 0:
            failed = True
            break
    
    for benchmark_name, benchmark_results in results.get("detailed_results", {}).items():
        for result in benchmark_results:
            if "additional_metrics" in result and "error" in result["additional_metrics"]:
                failed = True
                break
        if failed:
            break
    
    logger.info("All benchmarks completed successfully" if not failed else "Some benchmarks failed")
    return 1 if failed else 0


if __name__ == "__main__":
    exit(main())