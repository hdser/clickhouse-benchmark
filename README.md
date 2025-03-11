# ClickHouse Benchmark Tool

A modular and extensible benchmarking tool for ClickHouse databases, with a focus on memory usage and query performance.

## Features

- Measure query execution time, memory usage, rows processed, and data volume
- Run multiple benchmark iterations to calculate averages and standard deviations
- Extensible architecture for adding support for different databases
- Pre-defined benchmarks for Nebula database schema
- Easy-to-use interface for creating custom benchmarks
- Comprehensive logging and reporting

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

# Or using the script directly
python examples/run_nebula_benchmark.py --runs 3 --output nebula_results.json
```

### Running Custom Benchmarks

```bash
# Using the installed command
ch-custom-benchmark --runs 3 --output custom_results.json

# Or using the script directly
python examples/define_custom_benchmark.py --runs 3 --output custom_results.json
```

### Command-line Arguments

- `--host`: ClickHouse host address (overrides env variable)
- `--port`: ClickHouse port (overrides env variable)
- `--username`: ClickHouse username (overrides env variable)
- `--password`: ClickHouse password (overrides env variable)
- `--database`: ClickHouse database (overrides env variable)
- `--secure`: Use secure connection (overrides env variable)
- `--output`: Output file for results (default: benchmark_results.json)
- `--runs`: Number of runs per benchmark (default: 3)
- `--env-file`: Path to environment file (default: .env)
- `--table-info`: Show table information before running benchmarks (Nebula benchmark only)

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

# Run benchmarks
results = benchmark.run_all_benchmarks()

# Save and print results
benchmark.save_results_to_file(results, "custom_results.json")
benchmark.print_summary_table(results)
```

See `examples/define_custom_benchmark.py` for a more detailed example.

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

MIT