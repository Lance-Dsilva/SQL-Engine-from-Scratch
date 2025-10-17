import os

def create_file_structure():
    """Create the SQL component file structure with starter code"""
    
    # Define the structure
    structure = {
        'sql_component': {
            'parsers': {
                '__init__.py': '''"""
CSV Parsing Module

This module contains custom CSV parsing functionality without using
external libraries like pandas or the csv module.
"""

from .csv_parser import CSVParser
from .data_types import DataTypeInferrer
from .chunk_reader import ChunkReader

__all__ = ['CSVParser', 'DataTypeInferrer', 'ChunkReader']
''',
                'csv_parser.py': '''"""
Core CSV Parser Implementation

Handles CSV file parsing with support for:
- Configurable delimiters
- Quoted fields
- Escaped characters
- Header detection
"""

class CSVParser:
    def __init__(self, delimiter=',', quote_char='"', escape_char='\\\\'):
        """
        Initialize CSV parser with configurable parameters
        
        Args:
            delimiter (str): Field separator character (default: comma)
            quote_char (str): Character used for quoting fields (default: double quote)
            escape_char (str): Character used for escaping (default: backslash)
        """
        self.delimiter = delimiter
        self.quote_char = quote_char
        self.escape_char = escape_char
    
    def parse_line(self, line):
        """
        Parse a single CSV line into fields
        
        Args:
            line (str): A single line from CSV file
            
        Returns:
            list: List of parsed fields
        """
        fields = []
        current_field = ''
        in_quotes = False
        escape_next = False
        
        for char in line:
            # Handle escape character
            if escape_next:
                current_field += char
                escape_next = False
                continue
            
            if char == self.escape_char:
                escape_next = True
                continue
            
            # Handle quote character
            if char == self.quote_char:
                in_quotes = not in_quotes
                continue
            
            # Handle delimiter
            if char == self.delimiter and not in_quotes:
                fields.append(current_field.strip())
                current_field = ''
                continue
            
            # Regular character
            current_field += char
        
        # Add the last field
        fields.append(current_field.strip())
        
        return fields
    
    def detect_header(self, first_line_fields):
        """
        Detect if first line is a header
        
        Args:
            first_line_fields (list): Parsed fields from first line
            
        Returns:
            bool: True if likely a header, False otherwise
        """
        # Heuristic: Headers typically contain non-numeric strings
        numeric_count = 0
        
        for field in first_line_fields:
            try:
                float(field)
                numeric_count += 1
            except (ValueError, TypeError):
                pass
        
        # If more than half are non-numeric, likely a header
        return numeric_count < len(first_line_fields) / 2
    
    def parse_file(self, filepath, has_header=True, chunk_size=None):
        """
        Parse entire CSV file or return chunk generator
        
        Args:
            filepath (str): Path to CSV file
            has_header (bool): Whether first line is header
            chunk_size (int): If set, return generator for chunked reading
            
        Returns:
            dict or generator: Parsed data or chunk generator
        """
        if chunk_size:
            # Return generator for chunked reading
            return self._parse_chunked(filepath, has_header, chunk_size)
        else:
            # Parse entire file
            return self._parse_complete(filepath, has_header)
    
    def _parse_complete(self, filepath, has_header):
        """Parse complete file into memory"""
        headers = []
        data = []
        
        with open(filepath, 'r', encoding='utf-8') as file:
            first_line = file.readline().strip()
            
            if not first_line:
                return {'headers': [], 'data': []}
            
            first_fields = self.parse_line(first_line)
            
            # Determine if first line is header
            if has_header or self.detect_header(first_fields):
                headers = first_fields
            else:
                headers = [f'column_{i}' for i in range(len(first_fields))]
                data.append(first_fields)
            
            # Read remaining lines
            for line in file:
                line = line.strip()
                if line:  # Skip empty lines
                    fields = self.parse_line(line)
                    data.append(fields)
        
        return {
            'headers': headers,
            'data': data
        }
    
    def _parse_chunked(self, filepath, has_header, chunk_size):
        """Generator for chunked file reading"""
        # Implementation will be in chunk_reader.py
        from .chunk_reader import ChunkReader
        reader = ChunkReader(filepath, chunk_size, self)
        return reader.read_chunks(has_header)


# TODO: Add methods for handling multi-line fields
# TODO: Add better error handling and validation
''',
                'data_types.py': '''"""
Data Type Inference Module

Automatically detect and convert data types from string representations
"""

class DataTypeInferrer:
    """Infer and convert data types from string values"""
    
    @staticmethod
    def infer_type(value):
        """
        Infer the data type of a string value
        
        Args:
            value (str): String value to analyze
            
        Returns:
            tuple: (converted_value, type_name)
        """
        if not value:
            return (None, 'null')
        
        # Remove whitespace
        value = value.strip()
        
        # Check for null values
        if value.lower() in ['null', 'na', 'n/a', 'none', '']:
            return (None, 'null')
        
        # Try boolean
        bool_result = DataTypeInferrer._try_boolean(value)
        if bool_result is not None:
            return (bool_result, 'boolean')
        
        # Try integer
        int_result = DataTypeInferrer._try_integer(value)
        if int_result is not None:
            return (int_result, 'integer')
        
        # Try float
        float_result = DataTypeInferrer._try_float(value)
        if float_result is not None:
            return (float_result, 'float')
        
        # Try date
        date_result = DataTypeInferrer._try_date(value)
        if date_result is not None:
            return (date_result, 'date')
        
        # Default to string
        return (value, 'string')
    
    @staticmethod
    def _try_boolean(value):
        """Try to convert to boolean"""
        value_lower = value.lower()
        if value_lower in ['true', 't', 'yes', 'y', '1']:
            return True
        elif value_lower in ['false', 'f', 'no', 'n', '0']:
            return False
        return None
    
    @staticmethod
    def _try_integer(value):
        """Try to convert to integer"""
        try:
            return int(value)
        except ValueError:
            return None
    
    @staticmethod
    def _try_float(value):
        """Try to convert to float"""
        try:
            result = float(value)
            # Make sure it's not an integer disguised as float
            if '.' in value or 'e' in value.lower():
                return result
            return None
        except ValueError:
            return None
    
    @staticmethod
    def _try_date(value):
        """Try to parse as date"""
        # Common date formats
        date_patterns = [
            '%Y-%m-%d',           # 2024-01-15
            '%m/%d/%Y',           # 01/15/2024
            '%d/%m/%Y',           # 15/01/2024
            '%Y/%m/%d',           # 2024/01/15
            '%d-%m-%Y',           # 15-01-2024
            '%Y-%m-%d %H:%M:%S',  # 2024-01-15 10:30:00
        ]
        
        # Simple pattern matching (without datetime library for true from-scratch)
        # This is a simplified version
        if '-' in value or '/' in value:
            parts = value.replace('-', '/').split('/')
            if len(parts) == 3:
                try:
                    # Try to parse as integers
                    nums = [int(p.split()[0]) for p in parts]
                    # Basic validation
                    if any(n > 0 for n in nums):
                        return value  # Return as string for now
                except ValueError:
                    pass
        
        return None
    
    @staticmethod
    def infer_column_types(data, headers):
        """
        Infer types for all columns in dataset
        
        Args:
            data (list): List of rows
            headers (list): Column names
            
        Returns:
            dict: Mapping of column names to inferred types
        """
        column_types = {}
        
        for col_idx, header in enumerate(headers):
            # Sample first non-null values
            sample_values = []
            for row in data[:100]:  # Sample first 100 rows
                if col_idx < len(row) and row[col_idx]:
                    sample_values.append(row[col_idx])
                if len(sample_values) >= 10:
                    break
            
            # Infer type from samples
            if not sample_values:
                column_types[header] = 'string'
                continue
            
            types_found = []
            for val in sample_values:
                _, type_name = DataTypeInferrer.infer_type(str(val))
                types_found.append(type_name)
            
            # Use most common type (excluding null)
            non_null_types = [t for t in types_found if t != 'null']
            if non_null_types:
                column_types[header] = max(set(non_null_types), 
                                          key=non_null_types.count)
            else:
                column_types[header] = 'string'
        
        return column_types


# TODO: Add more sophisticated date parsing
# TODO: Add custom type definitions
''',
                'chunk_reader.py': '''"""
Chunked File Reader

Enables processing of large CSV files that don't fit in memory
by reading and processing data in configurable chunks
"""

class ChunkReader:
    """Read large CSV files in chunks"""
    
    def __init__(self, filepath, chunk_size=10000, parser=None):
        """
        Initialize chunk reader
        
        Args:
            filepath (str): Path to CSV file
            chunk_size (int): Number of rows per chunk
            parser (CSVParser): Parser instance to use
        """
        self.filepath = filepath
        self.chunk_size = chunk_size
        self.parser = parser
    
    def read_chunks(self, has_header=True):
        """
        Generator that yields chunks of data
        
        Args:
            has_header (bool): Whether file has header row
            
        Yields:
            dict: Chunk data with metadata
        """
        headers = []
        chunk_data = []
        chunk_number = 0
        total_rows = 0
        
        with open(self.filepath, 'r', encoding='utf-8') as file:
            # Read header if present
            first_line = file.readline().strip()
            
            if not first_line:
                return
            
            if has_header:
                headers = self.parser.parse_line(first_line)
            else:
                # Parse first line as data
                first_fields = self.parser.parse_line(first_line)
                headers = [f'column_{i}' for i in range(len(first_fields))]
                chunk_data.append(first_fields)
                total_rows += 1
            
            # Read file line by line
            for line in file:
                line = line.strip()
                
                if not line:  # Skip empty lines
                    continue
                
                fields = self.parser.parse_line(line)
                chunk_data.append(fields)
                total_rows += 1
                
                # Yield chunk when it reaches chunk_size
                if len(chunk_data) >= self.chunk_size:
                    yield {
                        'headers': headers,
                        'data': chunk_data,
                        'chunk_number': chunk_number,
                        'rows_in_chunk': len(chunk_data),
                        'total_rows_so_far': total_rows,
                        'is_last': False
                    }
                    
                    chunk_number += 1
                    chunk_data = []  # Clear for next chunk
            
            # Yield remaining data if any
            if chunk_data:
                yield {
                    'headers': headers,
                    'data': chunk_data,
                    'chunk_number': chunk_number,
                    'rows_in_chunk': len(chunk_data),
                    'total_rows_so_far': total_rows,
                    'is_last': True
                }


# TODO: Add progress tracking
# TODO: Add memory usage monitoring
# TODO: Handle corrupted chunks gracefully
'''
            },
            'operations': {
                '__init__.py': '''"""
SQL Operations Module

Implements SQL-like operations on in-memory table structures
"""

from .filter import Filter
from .projection import Projection
from .groupby import GroupBy
from .aggregation import Aggregation
from .join import Join

__all__ = ['Filter', 'Projection', 'GroupBy', 'Aggregation', 'Join']
''',
                'filter.py': '''"""
Filter Operation

Implements row filtering based on conditions
"""

class Filter:
    """Filter rows based on conditions"""
    
    @staticmethod
    def apply(table, condition_func):
        """
        Filter table rows based on condition
        
        Args:
            table: Table object to filter
            condition_func: Function that takes row dict and returns bool
            
        Returns:
            Table: New table with filtered rows
        """
        from ..core.table import Table
        
        filtered_rows = []
        
        for row in table.data:
            # Convert row to dictionary for easy access
            row_dict = {}
            for i, header in enumerate(table.headers):
                if i < len(row):
                    row_dict[header] = row[i]
                else:
                    row_dict[header] = None
            
            # Apply condition
            try:
                if condition_func(row_dict):
                    filtered_rows.append(row)
            except Exception as e:
                # Skip rows that cause errors in condition
                print(f"Warning: Error evaluating condition on row: {e}")
                continue
        
        return Table(table.headers, filtered_rows, table.name)
    
    @staticmethod
    def apply_multiple(table, conditions, logic='AND'):
        """
        Apply multiple conditions with AND/OR logic
        
        Args:
            table: Table object
            conditions: List of condition functions
            logic: 'AND' or 'OR'
            
        Returns:
            Table: Filtered table
        """
        if logic == 'AND':
            result = table
            for condition in conditions:
                result = Filter.apply(result, condition)
            return result
        else:  # OR logic
            all_rows = set()
            for condition in conditions:
                filtered = Filter.apply(table, condition)
                for row in filtered.data:
                    all_rows.add(tuple(row))
            
            from ..core.table import Table
            return Table(table.headers, [list(row) for row in all_rows], table.name)


# TODO: Add support for complex conditions
# TODO: Add null-safe comparisons
# TODO: Optimize for large datasets
''',
                'projection.py': '''"""
Projection Operation

Select specific columns from table
"""

class Projection:
    """Select specific columns from table"""
    
    @staticmethod
    def apply(table, columns):
        """
        Select specific columns from table
        
        Args:
            table: Table object
            columns: List of column names to select
            
        Returns:
            Table: New table with only selected columns
        """
        from ..core.table import Table
        
        # Validate all columns exist
        for col in columns:
            if col not in table.headers:
                raise ValueError(f"Column '{col}' not found in table. "
                               f"Available columns: {table.headers}")
        
        # Get column indices
        col_indices = [table.headers.index(col) for col in columns]
        
        # Extract data for selected columns
        projected_data = []
        for row in table.data:
            projected_row = [row[i] if i < len(row) else None 
                           for i in col_indices]
            projected_data.append(projected_row)
        
        return Table(columns, projected_data, table.name)
    
    @staticmethod
    def apply_with_rename(table, column_mapping):
        """
        Select and rename columns
        
        Args:
            table: Table object
            column_mapping: Dict of {old_name: new_name}
            
        Returns:
            Table: New table with renamed columns
        """
        from ..core.table import Table
        
        old_columns = list(column_mapping.keys())
        new_columns = list(column_mapping.values())
        
        # Project columns
        result = Projection.apply(table, old_columns)
        
        # Rename headers
        return Table(new_columns, result.data, table.name)


# TODO: Add computed columns (expressions)
# TODO: Add column transformations
''',
                'groupby.py': '''"""
Group By Operation

Group rows by one or more columns
"""

class GroupBy:
    """Group table rows by column values"""
    
    @staticmethod
    def apply(table, group_columns):
        """
        Group rows by specified columns
        
        Args:
            table: Table object
            group_columns: List of column names to group by
            
        Returns:
            dict: Mapping of group keys to lists of rows
        """
        # Validate group columns
        for col in group_columns:
            if col not in table.headers:
                raise ValueError(f"Column '{col}' not found in table")
        
        # Get indices of grouping columns
        group_indices = [table.headers.index(col) for col in group_columns]
        
        # Build groups
        groups = {}
        
        for row in table.data:
            # Create group key as tuple of values
            group_key = tuple(
                row[i] if i < len(row) else None 
                for i in group_indices
            )
            
            # Add row to appropriate group
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)
        
        return groups
    
    @staticmethod
    def to_table(groups, group_columns):
        """
        Convert groups back to table format
        
        Args:
            groups: Output from apply()
            group_columns: Original grouping columns
            
        Returns:
            list: List of dicts with group info
        """
        result = []
        for group_key, rows in groups.items():
            group_dict = {}
            for i, col in enumerate(group_columns):
                group_dict[col] = group_key[i]
            group_dict['_rows'] = rows
            group_dict['_count'] = len(rows)
            result.append(group_dict)
        
        return result


# TODO: Add support for grouping by computed values
# TODO: Add group filtering (HAVING clause)
''',
                'aggregation.py': '''"""
Aggregation Operations

Compute aggregate statistics on grouped or full datasets
"""

class Aggregation:
    """Aggregate functions for data analysis"""
    
    @staticmethod
    def sum(values):
        """
        Calculate sum of values
        
        Args:
            values: List of numeric values
            
        Returns:
            float: Sum of values (excluding None)
        """
        total = 0
        count = 0
        
        for val in values:
            if val is not None:
                try:
                    total += float(val)
                    count += 1
                except (ValueError, TypeError):
                    continue
        
        return total if count > 0 else None
    
    @staticmethod
    def count(values):
        """Count non-null values"""
        return len([v for v in values if v is not None])
    
    @staticmethod
    def avg(values):
        """
        Calculate average of values
        
        Args:
            values: List of numeric values
            
        Returns:
            float: Average (mean) of values
        """
        non_null = []
        
        for val in values:
            if val is not None:
                try:
                    non_null.append(float(val))
                except (ValueError, TypeError):
                    continue
        
        if not non_null:
            return None
        
        return sum(non_null) / len(non_null)
    
    @staticmethod
    def max_val(values):
        """Find maximum value"""
        non_null = [v for v in values if v is not None]
        
        if not non_null:
            return None
        
        max_v = non_null[0]
        for val in non_null[1:]:
            try:
                if val > max_v:
                    max_v = val
            except TypeError:
                continue
        
        return max_v
    
    @staticmethod
    def min_val(values):
        """Find minimum value"""
        non_null = [v for v in values if v is not None]
        
        if not non_null:
            return None
        
        min_v = non_null[0]
        for val in non_null[1:]:
            try:
                if val < min_v:
                    min_v = val
            except TypeError:
                continue
        
        return min_v
    
    @staticmethod
    def median(values):
        """Calculate median value"""
        non_null = []
        
        for val in values:
            if val is not None:
                try:
                    non_null.append(float(val))
                except (ValueError, TypeError):
                    continue
        
        if not non_null:
            return None
        
        # Sort values
        sorted_vals = sorted(non_null)
        n = len(sorted_vals)
        
        if n % 2 == 0:
            return (sorted_vals[n//2 - 1] + sorted_vals[n//2]) / 2
        else:
            return sorted_vals[n//2]
    
    @staticmethod
    def apply_to_groups(groups, table_headers, agg_specs):
        """
        Apply aggregation functions to grouped data
        
        Args:
            groups: Output from GroupBy.apply()
            table_headers: Original table headers
            agg_specs: Dict like {'column_name': ['sum', 'avg', 'count']}
            
        Returns:
            list: List of aggregation results
        """
        results = []
        
        # Map function names to functions
        func_map = {
            'sum': Aggregation.sum,
            'count': Aggregation.count,
            'avg': Aggregation.avg,
            'average': Aggregation.avg,
            'mean': Aggregation.avg,
            'max': Aggregation.max_val,
            'min': Aggregation.min_val,
            'median': Aggregation.median,
        }
        
        for group_key, rows in groups.items():
            result_row = {'group_key': group_key}
            
            # Apply each aggregation
            for col_name, agg_funcs in agg_specs.items():
                if col_name not in table_headers:
                    continue
                
                col_idx = table_headers.index(col_name)
                
                # Extract column values
                values = [row[col_idx] if col_idx < len(row) else None 
                         for row in rows]
                
                # Apply each function
                if isinstance(agg_funcs, str):
                    agg_funcs = [agg_funcs]
                
                for func_name in agg_funcs:
                    func_name_lower = func_name.lower()
                    if func_name_lower in func_map:
                        func = func_map[func_name_lower]
                        result_row[f'{col_name}_{func_name}'] = func(values)
            
            results.append(result_row)
        
        return results


# TODO: Add standard deviation, variance
# TODO: Add percentile calculations
# TODO: Add custom aggregation functions
''',
                'join.py': '''"""
Join Operations

Combine two tables based on common columns
"""

class Join:
    """Join operations for combining tables"""
    
    @staticmethod
    def inner_join(left_table, right_table, left_key, right_key):
        """
        Inner join: Return only matching rows
        
        Args:
            left_table: Left table
            right_table: Right table
            left_key: Column name in left table
            right_key: Column name in right table
            
        Returns:
            Table: Joined table
        """
        from ..core.table import Table
        
        # Validate keys exist
        if left_key not in left_table.headers:
            raise ValueError(f"Column '{left_key}' not found in left table")
        if right_key not in right_table.headers:
            raise ValueError(f"Column '{right_key}' not found in right table")
        
        # Build index for right table
        right_index = {}
        right_key_idx = right_table.headers.index(right_key)
        
        for row in right_table.data:
            key_val = row[right_key_idx] if right_key_idx < len(row) else None
            if key_val not in right_index:
                right_index[key_val] = []
            right_index[key_val].append(row)
        
        # Perform join
        result_rows = []
        left_key_idx = left_table.headers.index(left_key)
        
        for left_row in left_table.data:
            key_val = left_row[left_key_idx] if left_key_idx < len(left_row) else None
            
            # Find matching right rows
            if key_val in right_index:
                for right_row in right_index[key_val]:
                    # Combine rows
                    combined = list(left_row) + list(right_row)
                    result_rows.append(combined)
        
        # Create result headers (avoid duplicates)
        result_headers = list(left_table.headers)
        for h in right_table.headers:
            if h != right_key:
                result_headers.append(h)
            else:
                # Don't duplicate the join key
                pass
        
        return Table(result_headers, result_rows, 
                    f"{left_table.name}_join_{right_table.name}")
    
    @staticmethod
    def left_join(left_table, right_table, left_key, right_key):
        """
        Left join: Return all left rows, with nulls for non-matches
        
        Args:
            left_table: Left table
            right_table: Right table
            left_key: Column name in left table
            right_key: Column name in right table
            
        Returns:
            Table: Joined table
        """
        from ..core.table import Table
        
        # Validate keys
        if left_key not in left_table.headers:
            raise ValueError(f"Column '{left_key}' not found in left table")
        if right_key not in right_table.headers:
            raise ValueError(f"Column '{right_key}' not found in right table")
        
        # Build index for right table
        right_index = {}
        right_key_idx = right_table.headers.index(right_key)
        
        for row in right_table.data:
            key_val = row[right_key_idx] if right_key_idx < len(row) else None
            if key_val not in right_index:
                right_index[key_val] = []
            right_index[key_val].append(row)
        
        # Determine right table column count (excluding join key)
        right_col_count = len(right_table.headers) - 1
        null_right_row = [None] * right_col_count
        
        # Perform join
        result_rows = []
        left_key_idx = left_table.headers.index(left_key)
        
        for left_row in left_table.data:
            key_val = left_row[left_key_idx] if left_key_idx < len(left_row) else None
            
            if key_val in right_index:
                # Matching rows found
                for right_row in right_index[key_val]:
                    # Filter out the join key from right row
                    filtered_right = [right_row[i] for i, h in enumerate(right_table.headers) 
                                     if h != right_key and i < len(right_row)]
                    combined = list(left_row) + filtered_right
                    result_rows.append(combined)
            else:
                # No match, add nulls
                combined = list(left_row) + null_right_row
                result_rows.append(combined)
        
        # Create result headers
        result_headers = list(left_table.headers)
        for h in right_table.headers:
            if h != right_key:
                result_headers.append(h)
        
        return Table(result_headers, result_rows, 
                    f"{left_table.name}_left_join_{right_table.name}")
    
    @staticmethod
    def right_join(left_table, right_table, left_key, right_key):
        """Right join: Same as left join with tables swapped"""
        return Join.left_join(right_table, left_table, right_key, left_key)
    
    @staticmethod
    def outer_join(left_table, right_table, left_key, right_key):
        """
        Full outer join: Return all rows from both tables
        
        Args:
            left_table: Left table
            right_table: Right table
            left_key: Column name in left table
            right_key: Column name in right table
            
        Returns:
            Table: Joined table
        """
        # Perform left join
        left_result = Join.left_join(left_table, right_table, left_key, right_key)
        
        # Find unmatched right rows
        from ..core.table import Table
        
        left_key_idx = left_table.headers.index(left_key)
        right_key_idx = right_table.headers.index(right_key)
        
        # Get all left key values
        left_keys = set(row[left_key_idx] for row in left_table.data 
                       if left_key_idx < len(row))
        
        # Find unmatched right rows
        unmatched_right = []
        for right_row in right_table.data:
            key_val = right_row[right_key_idx] if right_key_idx < len(right_row) else None
            if key_val not in left_keys:
                unmatched_right.append(right_row)
        
        # Add unmatched right rows with null left values
        null_left_row = [None] * len(left_table.headers)
        for right_row in unmatched_right:
            filtered_right = [right_row[i] for i, h in enumerate(right_table.headers) 
                             if h != right_key and i < len(right_row)]
            combined = null_left_row + filtered_right
            left_result.data.append(combined)
        
        return left_result


# TODO: Add cross join
# TODO: Add self join support
# TODO: Optimize join algorithms for large datasets
'''
            },
            'core': {
                '__init__.py': '''"""
Core Data Structures Module

Contains fundamental data structures for SQL operations
"""

from .table import Table
from .query_engine import QueryEngine

__all__ = ['Table', 'QueryEngine']
''',
                'table.py': '''"""
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
        
        return "\\n".join(lines)


# TODO: Add index support for faster lookups
# TODO: Add data validation
# TODO: Add column statistics methods
''',
                'query_engine.py': '''"""
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
'''
            },
            'utils': {
                '__init__.py': '''"""
Utility Functions Module
"""

from .validators import Validator

__all__ = ['Validator']
''',
                'validators.py': '''"""
Input Validation Utilities
"""

class Validator:
    """Input validation helper functions"""
    
    @staticmethod
    def validate_file_path(filepath):
        """
        Validate file path exists and is readable
        
        Args:
            filepath (str): Path to file
            
        Returns:
            bool: True if valid
            
        Raises:
            FileNotFoundError: If file doesn't exist
            PermissionError: If file isn't readable
        """
        import os
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
        
        if not os.path.isfile(filepath):
            raise ValueError(f"Path is not a file: {filepath}")
        
        if not os.access(filepath, os.R_OK):
            raise PermissionError(f"File not readable: {filepath}")
        
        return True
    
    @staticmethod
    def validate_columns(columns, available_columns):
        """
        Validate that requested columns exist
        
        Args:
            columns: List of column names
            available_columns: List of available columns
            
        Returns:
            bool: True if valid
            
        Raises:
            ValueError: If any column not found
        """
        if isinstance(columns, str):
            columns = [columns]
        
        for col in columns:
            if col not in available_columns:
                raise ValueError(
                    f"Column '{col}' not found. "
                    f"Available columns: {available_columns}"
                )
        
        return True
    
    @staticmethod
    def validate_positive_int(value, name="value"):
        """Validate positive integer"""
        if not isinstance(value, int):
            raise TypeError(f"{name} must be an integer")
        
        if value <= 0:
            raise ValueError(f"{name} must be positive")
        
        return True
    
    @staticmethod
    def validate_condition_function(func):
        """Validate that object is a callable function"""
        if not callable(func):
            raise TypeError("Condition must be a callable function")
        
        return True


# TODO: Add more specific validators
# TODO: Add data type validators
'''
            },
            'tests': {
                'test_parser.py': '''"""
Unit Tests for CSV Parser
"""

def test_basic_parsing():
    """Test basic CSV parsing functionality"""
    from sql_component.parsers.csv_parser import CSVParser
    
    parser = CSVParser()
    
    # Test line parsing
    line = "Alice,25,Engineer"
    result = parser.parse_line(line)
    assert result == ["Alice", "25", "Engineer"], f"Expected ['Alice', '25', 'Engineer'], got {result}"
    print("✓ Basic line parsing test passed")

def test_quoted_fields():
    """Test parsing with quoted fields"""
    from sql_component.parsers.csv_parser import CSVParser
    
    parser = CSVParser()
    
    # Test quoted field with comma
    line = '\\"Smith, John\\",30,Manager'
    result = parser.parse_line(line)
    assert result[0] == "Smith, John", f"Expected 'Smith, John', got {result[0]}"
    print("✓ Quoted fields test passed")

def test_data_type_inference():
    """Test data type inference"""
    from sql_component.parsers.data_types import DataTypeInferrer
    
    inferrer = DataTypeInferrer()
    
    # Test integer
    val, typ = inferrer.infer_type("123")
    assert typ == "integer", f"Expected 'integer', got {typ}"
    
    # Test float
    val, typ = inferrer.infer_type("123.45")
    assert typ == "float", f"Expected 'float', got {typ}"
    
    # Test string
    val, typ = inferrer.infer_type("hello")
    assert typ == "string", f"Expected 'string', got {typ}"
    
    print("✓ Data type inference test passed")

def run_all_tests():
    """Run all parser tests"""
    print("\\nRunning CSV Parser Tests...")
    print("=" * 50)
    
    test_basic_parsing()
    test_quoted_fields()
    test_data_type_inference()
    
    print("=" * 50)
    print("All parser tests passed!\\n")

if __name__ == "__main__":
    run_all_tests()
''',
                'test_operations.py': '''"""
Unit Tests for SQL Operations
"""

def test_filter_operation():
    """Test filter operation"""
    from sql_component.core.table import Table
    from sql_component.operations.filter import Filter
    
    # Create sample table
    headers = ['name', 'age', 'salary']
    data = [
        ['Alice', 25, 50000],
        ['Bob', 30, 60000],
        ['Carol', 35, 70000],
    ]
    table = Table(headers, data, 'employees')
    
    # Filter age > 25
    result = Filter.apply(table, lambda row: row['age'] > 25)
    
    assert result.row_count() == 2, f"Expected 2 rows, got {result.row_count()}"
    print("✓ Filter operation test passed")

def test_projection_operation():
    """Test projection operation"""
    from sql_component.core.table import Table
    from sql_component.operations.projection import Projection
    
    # Create sample table
    headers = ['name', 'age', 'salary']
    data = [
        ['Alice', 25, 50000],
        ['Bob', 30, 60000],
    ]
    table = Table(headers, data, 'employees')
    
    # Select only name and age
    result = Projection.apply(table, ['name', 'age'])
    
    assert result.col_count() == 2, f"Expected 2 columns, got {result.col_count()}"
    assert 'salary' not in result.headers, "Salary should not be in result"
    print("✓ Projection operation test passed")

def test_aggregation():
    """Test aggregation functions"""
    from sql_component.operations.aggregation import Aggregation
    
    values = [10, 20, 30, 40, 50]
    
    # Test sum
    result = Aggregation.sum(values)
    assert result == 150, f"Expected 150, got {result}"
    
    # Test average
    result = Aggregation.avg(values)
    assert result == 30, f"Expected 30, got {result}"
    
    # Test count
    result = Aggregation.count(values)
    assert result == 5, f"Expected 5, got {result}"
    
    print("✓ Aggregation functions test passed")

def run_all_tests():
    """Run all operation tests"""
    print("\\nRunning SQL Operations Tests...")
    print("=" * 50)
    
    test_filter_operation()
    test_projection_operation()
    test_aggregation()
    
    print("=" * 50)
    print("All operations tests passed!\\n")

if __name__ == "__main__":
    run_all_tests()
''',
                'sample_data': {}
            }
        }
    }
    
    # Create the structure
    def create_structure(base_path, structure):
        for name, content in structure.items():
            path = os.path.join(base_path, name)
            
            if isinstance(content, dict):
                # It's a directory
                os.makedirs(path, exist_ok=True)
                print(f"Created directory: {path}")
                create_structure(path, content)
            else:
                # It's a file
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"Created file: {path}")
    
    # Start creation
    print("Creating SQL Component file structure...\n")
    create_structure('.', structure)
    print("\n✅ File structure created successfully!")
    print("\nNext steps:")
    print("1. Review the generated code in each file")
    print("2. Run the tests: python sql_component/tests/test_parser.py")
    print("3. Run the tests: python sql_component/tests/test_operations.py")
    print("4. Start implementing the TODOs marked in each file")
    print("5. Create sample CSV files in sql_component/tests/sample_data/")

if __name__ == "__main__":
    create_file_structure()