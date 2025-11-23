"""
Table Data Structure

In-memory representation of a data table
"""

class Table:
    """In-memory table structure for SQL operations"""
    
    def __init__(self, headers, data, name=None):
        """
        Initialize table
        
        Args:
            headers (list): Column names
            data (list): List of rows (each row is a list)
            name (str): Optional table name
        """
        self.headers = headers
        self.data = data
        self.name = name or 'unnamed_table'
        self.types = {}
        self._infer_column_types()
    
    def _infer_column_types(self):
        """Infer data types for each column"""
        from ..parsers.data_types import DataTypeInferrer
        
        if not self.data:
            for header in self.headers:
                self.types[header] = 'unknown'
            return
        
        self.types = DataTypeInferrer.infer_column_types(self.data, self.headers)
    
    def get_column(self, col_name):
        """
        Extract a single column as a list
        
        Args:
            col_name (str): Column name
            
        Returns:
            list: Values from the column
        """
        if col_name not in self.headers:
            raise ValueError(f"Column '{col_name}' not found")
        
        col_idx = self.headers.index(col_name)
        return [row[col_idx] if col_idx < len(row) else None 
                for row in self.data]
    
    def get_row(self, index):
        """
        Get a row by index
        
        Args:
            index (int): Row index
            
        Returns:
            dict: Row as dictionary
        """
        if index < 0 or index >= len(self.data):
            raise IndexError(f"Row index {index} out of range")
        
        row = self.data[index]
        return {header: row[i] if i < len(row) else None 
                for i, header in enumerate(self.headers)}
    
    def add_column(self, col_name, values):
        """
        Add a new column to the table
        
        Args:
            col_name (str): Name of new column
            values (list): Values for the column
        """
        if col_name in self.headers:
            raise ValueError(f"Column '{col_name}' already exists")
        
        if len(values) != len(self.data):
            raise ValueError("Number of values must match number of rows")
        
        self.headers.append(col_name)
        
        for i, row in enumerate(self.data):
            row.append(values[i])
    
    def row_count(self):
        """Return number of rows"""
        return len(self.data)
    
    def col_count(self):
        """Return number of columns"""
        return len(self.headers)
    
    def shape(self):
        """Return (rows, columns) tuple"""
        return (self.row_count(), self.col_count())
    
    def head(self, n=5):
        """
        Return first n rows
        
        Args:
            n (int): Number of rows
            
        Returns:
            Table: New table with first n rows
        """
        return Table(self.headers, self.data[:n], self.name)
    
    def tail(self, n=5):
        """
        Return last n rows
        
        Args:
            n (int): Number of rows
            
        Returns:
            Table: New table with last n rows
        """
        return Table(self.headers, self.data[-n:], self.name)
    
    def __repr__(self):
        """String representation of table"""
        rows, cols = self.shape()
        return f"Table(name='{self.name}', rows={rows}, columns={cols})"
    
    def __str__(self):
        """Pretty print table"""
        return self.to_string()
    
    def to_string(self, max_rows=10, max_col_width=20):
        """
        Convert table to formatted string
        
        Args:
            max_rows (int): Maximum rows to display
            max_col_width (int): Maximum column width
            
        Returns:
            str: Formatted table string
        """
        if not self.data:
            return f"Empty Table: {self.name}"
        
        # Truncate long strings
        def truncate(val, width):
            s = str(val)
            return s if len(s) <= width else s[:width-3] + '...'
        
        # Build output
        lines = []
        lines.append(f"Table: {self.name}")
        lines.append(f"Shape: {self.shape()}")
        lines.append("")
        
        # Headers
        header_line = " | ".join(truncate(h, max_col_width) for h in self.headers)
        lines.append(header_line)
        lines.append("-" * len(header_line))
        
        # Data rows
        display_data = self.data[:max_rows]
        for row in display_data:
            row_line = " | ".join(
                truncate(row[i] if i < len(row) else '', max_col_width)
                for i in range(len(self.headers))
            )
            lines.append(row_line)
        
        if len(self.data) > max_rows:
            lines.append(f"... ({len(self.data) - max_rows} more rows)")
        
        return "\n".join(lines)


# TODO: Add index support for faster lookups
# TODO: Add data validation
# TODO: Add column statistics methods
