"""
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
