"""
Benchmark query collections.
"""
from .base import BenchmarkQueryCollection
from .nebula_benchmarks import NebulaBenchmarks
from .custom_benchmarks import CustomBenchmarks

__all__ = [
    'BenchmarkQueryCollection',
    'NebulaBenchmarks',
    'CustomBenchmarks'
]