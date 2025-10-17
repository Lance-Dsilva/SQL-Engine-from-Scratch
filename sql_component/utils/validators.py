"""
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
