"""
Core CSV Parser Implementation

Handles CSV file parsing with support for:
- Configurable delimiters
- Quoted fields
- Escaped characters
- Header detection
"""

class CSVParser:
    def __init__(self, delimiter=',', quote_char='"', escape_char='\\'):
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
