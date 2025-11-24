class CSVParser:
    def __init__(self, delimiter=',', quote_char='"'):
        self.delimiter = delimiter
        self.quote_char = quote_char
    
    def parse_file(self, file_obj):
        """Parse entire CSV file"""
        content = file_obj.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8')
        lines = content.strip().split('\n')
        
        if not lines:
            return {'headers': [], 'data': []}
        
        headers = self._parse_line(lines[0]) 
        data = []
        
        for line in lines[1:]:
            if line.strip():
                row_data = self._parse_line(line)
                row_dict = {headers[i]: row_data[i] if i < len(row_data) else None 
                           for i in range(len(headers))}
                data.append(row_dict)
        
        return {'headers': headers, 'data': data}
    
    def get_file_info(self, filepath):
        """Get headers without loading full file"""
        with open(filepath, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            headers = self._parse_line(first_line)
            
            import os
            file_size = os.path.getsize(filepath)
            estimated_rows = file_size // 100
        
        return headers, estimated_rows
    
    def parse_file_in_chunks(self, filepath, chunk_size=10000):
        """Parse file in chunks (generator)"""
        with open(filepath, 'r', encoding='utf-8', buffering=8192*1024) as f:
            header_line = f.readline()
            headers = self._parse_line(header_line)
            
            chunk = []
            for line in f:
                if not line.strip():
                    continue
                
                try:
                    row_data = self._parse_line(line)
                    row_dict = {headers[i]: row_data[i] if i < len(row_data) else None 
                               for i in range(len(headers))}
                    chunk.append(row_dict)
                    
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                except:
                    continue
            
            if chunk:
                yield chunk
    
    def _parse_line(self, line):
        """Parse single CSV line"""
        line = line.strip()
        values = []
        current_value = ""
        in_quotes = False
        
        i = 0
        while i < len(line):
            char = line[i]
            
            if char == self.quote_char:
                if in_quotes and i + 1 < len(line) and line[i + 1] == self.quote_char:
                    current_value += self.quote_char
                    i += 1
                else:
                    in_quotes = not in_quotes
            elif char == self.delimiter and not in_quotes:
                values.append(current_value.strip())
                current_value = ""
            else:
                current_value += char
            
            i += 1
        
        values.append(current_value.strip())
        return values
