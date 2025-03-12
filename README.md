# ClickHouse Benchmark Tool

A modular and extensible benchmarking tool for ClickHouse databases, with a focus on memory usage and query performance.

## Features

- Measure query execution time, memory usage, rows processed, and data volume
- Comprehensive metrics tracking: read/written rows, bytes, memory usage, and more
- Advanced memory management with per-query memory limits
- Intelligent handling of memory-intensive queries
- Run multiple benchmark iterations to calculate averages and standard deviations
- Extensible architecture for adding support for different databases
- Pre-defined benchmarks for Nebula database schema
- Easy-to-use interface for creating custom benchmarks
- Comprehensive logging and reporting
- Support for skipping problematic queries
- Support for retrying failed benchmarks

## Installation

### From Source

```bash
git clone https://github.com/yourusername/clickhouse-benchmark.git
cd clickhouse-benchmark
pip install -e .
```

### Requirements

- Python 3.7+
- ClickHouse Connect
- python-dotenv

## Configuration

Create a `.env` file in the root directory with your ClickHouse connection details:

```
CLICKHOUSE_HOST=your-clickhouse-host
CLICKHOUSE_PORT=9440
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=your_database
CLICKHOUSE_SECURE=true
```

## Usage

### Running Nebula Benchmarks

```bash
# Using the installed command
ch-benchmark --runs 3 --output nebula_results.json

# With memory limits for specific queries
ch-benchmark --memory-limits '{"complex_multi_table_query": "9GB"}'

# Skipping problematic queries
ch-benchmark --skip-benchmarks "complex_multi_table_query,neighbors_with_join"

# Or using the script directly
python examples/run_nebula_benchmark.py --runs 3 --output nebula_results.json
```

### Running Custom Benchmarks

```bash
# Using the installed command
ch-custom-benchmark --runs 3 --output custom_results.json

# With a file containing custom query definitions
ch-custom-benchmark --custom-queries my_queries.json

# Or using the script directly
python examples/define_custom_benchmark.py --runs 3 --output custom_results.json
```

### Memory Limit Format

Memory limits can be specified in a JSON file or directly as a command-line argument:

```json
{
  "complex_multi_table_query": "9GB",
  "peer_connectivity_analysis": "4GB",
  "neighbors_with_join": "8589934592"
}
```

### Command-line Arguments

Common arguments:
- `--host`: ClickHouse host address (overrides env variable)
- `--port`: ClickHouse port (overrides env variable)
- `--username`: ClickHouse username (overrides env variable)
- `--password`: ClickHouse password (overrides env variable)
- `--database`: ClickHouse database (overrides env variable)
- `--secure`: Use secure connection (overrides env variable)
- `--output`: Output file for results (default: benchmark_results.json)
- `--runs`: Number of runs per benchmark (default: 3)
- `--env-file`: Path to environment file (default: .env)

Advanced arguments:
- `--memory-limits`: JSON file or string with memory limits for specific benchmarks
- `--skip-benchmarks`: Comma-separated list of benchmark names to skip
- `--retry-failed`: JSON file with previous results to retry only failed benchmarks

Nebula benchmark arguments:
- `--table-info`: Show table information before running benchmarks

Custom benchmark arguments:
- `--custom-queries`: JSON file with custom query definitions

## Creating Custom Benchmarks

You can create custom benchmarks by using the `CustomBenchmarks` class:

```python
from benchmarks import ClickHouseBenchmark
from benchmarks.query_definitions import CustomBenchmarks

# Initialize benchmark
benchmark = ClickHouseBenchmark()
benchmark.connect(host="localhost", port=9000, username="default", password="")

# Create a custom benchmark collection
custom = CustomBenchmarks(
    name="my_custom_benchmarks",
    description="My custom benchmark queries"
)

# Add custom queries
custom.add_query(
    name="simple_query",
    description="Simple test query",
    query="SELECT * FROM my_table LIMIT 1000"
)

# Add benchmarks to the runner
benchmark.add_benchmarks_from_list(custom.get_queries())

# Define memory limits for specific benchmarks
memory_limits = {
    "simple_query": "4GB"
}

# Run benchmarks with memory limits
results = benchmark.run_all_benchmarks(memory_limits=memory_limits)

# Save and print results
benchmark.save_results_to_file(results, "custom_results.json")
benchmark.print_summary_table(results)
```

See `examples/define_custom_benchmark.py` for a more detailed example.

## Handling Memory-Intensive Queries

The benchmark tool now intelligently handles memory-intensive queries:

1. You can set per-query memory limits to avoid OOM errors
2. For queries that exceed memory limits, the tool provides:
   - Detailed error information
   - Memory usage statistics
   - Optimization suggestions

Example output for a failed query:

```
FAILED QUERIES
========================================================================================================================
Query #1: complex_multi_table_query
Error Type: MEMORY_LIMIT_EXCEEDED
Error Details:
  - requested_memory: 4.88 GiB
  - current_rss: 7.18 GiB
  - maximum_memory: 7.20 GiB
Optimization Suggestions:
  - Consider adding LIMIT to reduce result set size
  - Break down the query into smaller parts
  - Use WITH clauses for complex subqueries
  - Add more specific WHERE conditions
  - Consider using approximate functions like approxDistinct()
  - Reduce the number of columns in SELECT
Error Message: HTTPDriver for https://your-clickhouse-host:443 received ClickHouse error code 241...
========================================================================================================================
```

## Metrics Tracked

The benchmark tool tracks and reports on the following metrics:

- Execution time
- Memory usage
- Rows read
- Bytes read
- Rows written
- Bytes written
- Result rows
- Result bytes

All metrics are reported with averages and standard deviations when multiple runs are performed.

## Extending for Other Databases

To add support for a different database system:

1. Create a new class that extends `BenchmarkRunner` in the `benchmarks` directory
2. Implement the required methods:
   - `connect()`: Establish connection to the database
   - `_run_benchmark_query()`: Execute a benchmark query and collect metrics
3. Create query definition modules specific to your database

## Project Structure

```
clickhouse-benchmark/
├── benchmarks/
│   ├── benchmark_runner.py        # Base benchmark functionality
│   ├── clickhouse_benchmark.py    # ClickHouse specific implementation
│   └── query_definitions/
│       ├── base.py                # Base class for query collections
│       ├── nebula_benchmarks.py   # Nebula-specific benchmark queries
│       └── custom_benchmarks.py   # Template for custom benchmarks
├── examples/
│   ├── run_nebula_benchmark.py    # Example for running Nebula benchmarks
│   └── define_custom_benchmark.py # Example for defining custom benchmarks
└── setup.py                       # Package setup script
```

## License

This project is licensed under the [MIT License](LICENSE).