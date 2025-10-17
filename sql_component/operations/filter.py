"""
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
