#!/usr/bin/env python3
"""
Base definitions for benchmark query collections.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BenchmarkQueryCollection(ABC):
    """
    Abstract base class for benchmark query collections.
    Provides a standard interface for defining benchmark queries.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the benchmark collection."""
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        """Return a description of the benchmark collection."""
        pass
    
    @abstractmethod
    def get_queries(self) -> List[Dict[str, Any]]:
        """Return a list of benchmark queries."""
        pass