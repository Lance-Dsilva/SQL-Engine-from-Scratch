"""
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
