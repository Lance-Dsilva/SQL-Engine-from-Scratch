"""
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
