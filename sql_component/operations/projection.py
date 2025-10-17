"""
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
