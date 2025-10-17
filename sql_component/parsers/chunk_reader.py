"""
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
