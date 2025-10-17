"""
SQL Operations Module

Implements SQL-like operations on in-memory table structures
"""

from .filter import Filter
from .projection import Projection
from .groupby import GroupBy
from .aggregation import Aggregation
from .join import Join

__all__ = ['Filter', 'Projection', 'GroupBy', 'Aggregation', 'Join']
