#!/usr/bin/env python3
"""
Template for creating custom benchmark query collections.
"""
from typing import List, Dict, Any

from .base import BenchmarkQueryCollection


class CustomBenchmarks(BenchmarkQueryCollection):
    """
    Template for creating custom benchmark query collections.
    Extend this class to create your own benchmark query collections.
    """
    
    def __init__(self, name: str = "custom_benchmarks", description: str = "Custom benchmark queries"):
        self._name = name
        self._description = description
        self._queries = []
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def description(self) -> str:
        return self._description
    
    def add_query(self, name: str, query: str, description: str = "", run_count: int = 3):
        """Add a query to the collection."""
        self._queries.append({
            "name": name,
            "query": query,
            "description": description,
            "run_count": run_count
        })
    
    def get_queries(self) -> List[Dict[str, Any]]:
        """Return the list of benchmark queries."""
        return self._queries