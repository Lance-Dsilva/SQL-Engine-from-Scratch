"""
SQL Engine Component
Handles CSV parsing and SQL-like operations
"""

class SQLEngine:
    def __init__(self):
        self.data = None
        self.headers = None
        self.filename = None
        self.chunked = False
        self.chunk_size = None
        self.file_path = None
        self.operations = []
    
    def load_data(self, file_obj, chunk_size=None):
        """Load CSV file"""
        try:
            from parsers.csv_parser import CSVParser
            import tempfile
            
            parser = CSVParser()
            
            # Save to temp file if chunking
            if chunk_size:
                with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as tmp:
                    tmp.write(file_obj.getvalue())
                    self.file_path = tmp.name
                
                self.headers, estimated_rows = parser.get_file_info(self.file_path)
                self.chunked = True
                self.chunk_size = chunk_size
                self.data = None  # Data loaded on demand
            else:
                result = parser.parse_file(file_obj)
                self.headers = result['headers']
                self.data = result['data']
                self.chunked = False
            
            self.filename = file_obj.name
            self.operations = []
            return True, f"Loaded {file_obj.name} successfully"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def is_loaded(self):
        return self.headers is not None
    
    def get_info(self):
        if self.chunked:
            # Estimate rows
            import os
            file_size = os.path.getsize(self.file_path)
            estimated_rows = file_size // 100
            return {
                'filename': self.filename,
                'rows': estimated_rows,
                'columns': len(self.headers),
                'chunked': True,
                'chunk_size': self.chunk_size
            }
        else:
            return {
                'filename': self.filename,
                'rows': len(self.data),
                'columns': len(self.headers),
                'chunked': False,
                'chunk_size': None
            }
    
    def get_columns(self):
        return self.headers if self.headers else []
    
    def preview(self, n=10):
        if self.chunked:
            from parsers.csv_parser import CSVParser
            parser = CSVParser()
            for chunk in parser.parse_file_in_chunks(self.file_path, self.chunk_size):
                return chunk[:n]
        else:
            return self.data[:n] if self.data else []
    
    def add_filter(self, column, operator, value):
        self.operations.append({
            'type': 'filter',
            'column': column,
            'operator': operator,
            'value': value,
            'description': f"{column} {operator} {value}"
        })
    
    def add_select(self, columns):
        if columns:
            self.operations.append({
                'type': 'select',
                'columns': columns,
                'description': f"SELECT {', '.join(columns)}"
            })
    
    def add_groupby(self, column, agg_func=None, agg_column=None):
        desc = f"GROUP BY {column}"
        if agg_func:
            desc += f" {agg_func}({agg_column})"
        self.operations.append({
            'type': 'groupby',
            'column': column,
            'agg_func': agg_func,
            'agg_column': agg_column,
            'description': desc
        })
    
    def add_limit(self, n):
        self.operations.append({
            'type': 'limit',
            'value': n,
            'description': f"LIMIT {n}"
        })
    
    def get_operations(self):
        return self.operations
    
    def remove_operation(self, index):
        if 0 <= index < len(self.operations):
            self.operations.pop(index)
    
    def clear_operations(self):
        self.operations = []
    
    def execute(self):
        """Execute query operations"""
        try:
            from operations.query_executor import QueryExecutor
            
            executor = QueryExecutor(self.data, self.headers, self.file_path, 
                                    self.chunked, self.chunk_size)
            return executor.execute(self.operations)
        except Exception as e:
            print(f"Execution error: {e}")
            return None
