"""
Query Engine

Coordinates multiple operations to execute complex queries
"""

class QueryEngine:
    """Execute complex queries by chaining operations"""
    
    def __init__(self, table):
        """
        Initialize query engine with a table
        
        Args:
            table: Table object to query
        """
        self.table = table
        self.result = table
        self._groups = None
    
    def filter(self, condition_func):
        """
        Apply filter operation
        
        Args:
            condition_func: Function that takes row dict and returns bool
            
        Returns:
            self: For method chaining
        """
        from ..operations.filter import Filter
        self.result = Filter.apply(self.result, condition_func)
        return self
    
    def select(self, columns):
        """
        Apply projection (select columns)
        
        Args:
            columns: List of column names
            
        Returns:
            self: For method chaining
        """
        from ..operations.projection import Projection
        self.result = Projection.apply(self.result, columns)
        return self
    
    def group_by(self, columns):
        """
        Group data by columns
        
        Args:
            columns: List of column names to group by
            
        Returns:
            self: For method chaining
        """
        from ..operations.groupby import GroupBy
        self._groups = GroupBy.apply(self.result, columns)
        self._group_columns = columns
        return self
    
    def aggregate(self, agg_specs):
        """
        Apply aggregation functions
        
        Args:
            agg_specs: Dict like {'column_name': ['sum', 'avg']}
            
        Returns:
            self: For method chaining
        """
        from ..operations.aggregation import Aggregation
        from .table import Table
        
        if self._groups is None:
            raise ValueError("Must call group_by() before aggregate()")
        
        # Apply aggregations
        results = Aggregation.apply_to_groups(
            self._groups,
            self.result.headers,
            agg_specs
        )
        
        # Convert to table format
        if results:
            # Extract headers from first result
            headers = list(results[0].keys())
            data = [[r.get(h) for h in headers] for r in results]
            
            self.result = Table(headers, data, self.result.name + '_aggregated')
        
        self._groups = None  # Reset groups
        return self
    
    def join(self, other_table, left_key, right_key, join_type='inner'):
        """
        Join with another table
        
        Args:
            other_table: Table to join with
            left_key: Column name in current table
            right_key: Column name in other table
            join_type: 'inner', 'left', 'right', or 'outer'
            
        Returns:
            self: For method chaining
        """
        from ..operations.join import Join
        
        if join_type == 'inner':
            self.result = Join.inner_join(self.result, other_table, left_key, right_key)
        elif join_type == 'left':
            self.result = Join.left_join(self.result, other_table, left_key, right_key)
        elif join_type == 'right':
            self.result = Join.right_join(self.result, other_table, left_key, right_key)
        elif join_type == 'outer':
            self.result = Join.outer_join(self.result, other_table, left_key, right_key)
        else:
            raise ValueError(f"Unknown join type: {join_type}")
        
        return self
    
    def order_by(self, columns, ascending=True):
        """
        Sort results by columns
        
        Args:
            columns: List of column names or single column name
            ascending: Sort order (True for ascending)
            
        Returns:
            self: For method chaining
        """
        if isinstance(columns, str):
            columns = [columns]
        
        # Get column indices
        col_indices = [self.result.headers.index(col) for col in columns]
        
        # Sort data
        def sort_key(row):
            return tuple(row[i] if i < len(row) else None for i in col_indices)
        
        sorted_data = sorted(self.result.data, key=sort_key, reverse=not ascending)
        
        from .table import Table
        self.result = Table(self.result.headers, sorted_data, self.result.name)
        
        return self
    
    def limit(self, n):
        """
        Limit number of results
        
        Args:
            n: Maximum number of rows
            
        Returns:
            self: For method chaining
        """
        from .table import Table
        self.result = Table(
            self.result.headers,
            self.result.data[:n],
            self.result.name
        )
        return self
    
    def execute(self):
        """
        Execute query and return result
        
        Returns:
            Table: Query result
        """
        return self.result
    
    def reset(self):
        """Reset to original table"""
        self.result = self.table
        self._groups = None
        return self


# TODO: Add query optimization
# TODO: Add query plan visualization
# TODO: Add performance metrics
