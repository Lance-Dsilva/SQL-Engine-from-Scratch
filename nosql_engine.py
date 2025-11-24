class JSONParser:
    """Custom JSON Parser - No external libraries"""
    def __init__(self):
        self.text = ""
        self.index = 0
    
    def parse_file(self, file_obj):
        """Parse JSON file from scratch"""
        content = file_obj.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8')
        
        self.text = content.strip()
        self.index = 0
        
        return self._parse_value()
    
    def _parse_value(self):
        """Parse any JSON value"""
        self._skip_whitespace()
        
        if self.index >= len(self.text):
            return None
        
        char = self.text[self.index]
        
        if char == '{':
            return self._parse_object()
        elif char == '[':
            return self._parse_array()
        elif char == '"':
            return self._parse_string()
        elif char == '-' or char.isdigit():
            return self._parse_number()
        elif self.text[self.index:self.index+4] == 'true':
            self.index += 4
            return True
        elif self.text[self.index:self.index+5] == 'false':
            self.index += 5
            return False
        elif self.text[self.index:self.index+4] == 'null':
            self.index += 4
            return None
        else:
            raise ValueError(f"Unexpected character: {char}")
    
    def _parse_object(self):
        """Parse JSON object {...}"""
        obj = {}
        self.index += 1
        self._skip_whitespace()
        
        if self.index < len(self.text) and self.text[self.index] == '}':
            self.index += 1
            return obj
        
        while self.index < len(self.text):
            self._skip_whitespace()
            
            if self.text[self.index] != '"':
                raise ValueError("Expected string key")
            
            key = self._parse_string()
            self._skip_whitespace()
            
            if self.index >= len(self.text) or self.text[self.index] != ':':
                raise ValueError("Expected ':'")
            self.index += 1
            
            value = self._parse_value()
            obj[key] = value
            
            self._skip_whitespace()
            
            if self.index >= len(self.text):
                break
            
            if self.text[self.index] == ',':
                self.index += 1
            elif self.text[self.index] == '}':
                self.index += 1
                break
        
        return obj
    
    def _parse_array(self):
        """Parse JSON array [...]"""
        arr = []
        self.index += 1
        self._skip_whitespace()
        
        if self.index < len(self.text) and self.text[self.index] == ']':
            self.index += 1
            return arr
        
        while self.index < len(self.text):
            value = self._parse_value()
            arr.append(value)
            
            self._skip_whitespace()
            
            if self.index >= len(self.text):
                break
            
            if self.text[self.index] == ',':
                self.index += 1
            elif self.text[self.index] == ']':
                self.index += 1
                break
        
        return arr
    
    def _parse_string(self):
        """Parse JSON string"""
        self.index += 1
        result = ""
        
        while self.index < len(self.text):
            char = self.text[self.index]
            
            if char == '"':
                self.index += 1
                return result
            elif char == '\\':
                self.index += 1
                if self.index < len(self.text):
                    next_char = self.text[self.index]
                    if next_char == 'n':
                        result += '\n'
                    elif next_char == 't':
                        result += '\t'
                    elif next_char == 'r':
                        result += '\r'
                    elif next_char == '"':
                        result += '"'
                    elif next_char == '\\':
                        result += '\\'
                    elif next_char == '/':
                        result += '/'
                    else:
                        result += next_char
                    self.index += 1
            else:
                result += char
                self.index += 1
        
        raise ValueError("Unterminated string")
    
    def _parse_number(self):
        """Parse JSON number"""
        start = self.index
        
        if self.text[self.index] == '-':
            self.index += 1
        
        while self.index < len(self.text) and self.text[self.index].isdigit():
            self.index += 1
        
        if self.index < len(self.text) and self.text[self.index] == '.':
            self.index += 1
            while self.index < len(self.text) and self.text[self.index].isdigit():
                self.index += 1
        
        if self.index < len(self.text) and self.text[self.index] in ['e', 'E']:
            self.index += 1
            if self.index < len(self.text) and self.text[self.index] in ['+', '-']:
                self.index += 1
            while self.index < len(self.text) and self.text[self.index].isdigit():
                self.index += 1
        
        num_str = self.text[start:self.index]
        
        if '.' in num_str or 'e' in num_str or 'E' in num_str:
            return float(num_str)
        else:
            return int(num_str)
    
    def _skip_whitespace(self):
        """Skip whitespace"""
        while self.index < len(self.text) and self.text[self.index] in ' \t\n\r':
            self.index += 1


class NoSQLEngine:
    def __init__(self):
        self.data = None
        self.filename = None
        self.join_data = None
        self.join_filename = None
        self.operations = []
    
    def load_data(self, file_obj):
        """Load JSON file using custom parser"""
        try:
            parser = JSONParser()
            self.data = parser.parse_file(file_obj)
            self.filename = file_obj.name
            self.operations = []
            return True, f"Loaded {self.filename}"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def load_join_collection(self, file_obj):
        """Load second JSON collection for JOIN/Lookup"""
        try:
            parser = JSONParser()
            self.join_data = parser.parse_file(file_obj)
            self.join_filename = file_obj.name
            return True, f"Loaded join collection: {self.join_filename}"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def has_join_collection(self):
        """Check if join collection is loaded"""
        return self.join_data is not None
    
    def get_join_collection_info(self):
        """Get join collection information"""
        if isinstance(self.join_data, list):
            count = len(self.join_data)
        else:
            count = 1
        
        return {
            'filename': self.join_filename,
            'count': count
        }
    
    def preview_join_collection(self, n=3):
        """Preview join collection"""
        if isinstance(self.join_data, list):
            return self.join_data[:n]
        else:
            return [self.join_data]
    
    def preview_join_result(self, n=3):
        """Preview what the join will look like"""
        if not self.join_data:
            return None
        
        # Find join operation
        join_ops = [op for op in self.operations if op['type'] == 'join']
        if not join_ops:
            return None
        
        join_op = join_ops[0]
        
        # Get preview data
        if isinstance(self.data, list):
            preview_data = self.data[:n]
        else:
            preview_data = [self.data]
        
        # Perform join on preview
        from nosql_executor import NoSQLExecutor
        executor = NoSQLExecutor(preview_data, self.join_data)
        return executor._apply_join(preview_data, join_op)
    
    def clear_join_collection(self):
        """Remove join collection"""
        self.join_data = None
        self.join_filename = None
        # Remove join operations
        self.operations = [op for op in self.operations if op['type'] != 'join']
    
    def get_join_fields(self):
        """Get field names from join collection"""
        if not self.join_data:
            return []
        
        sample = self.join_data[0] if isinstance(self.join_data, list) else self.join_data
        return self._extract_fields(sample)
    
    def is_loaded(self):
        return self.data is not None
    
    def get_info(self):
        if isinstance(self.data, list):
            return {
                'filename': self.filename,
                'count': len(self.data),
                'type': 'Array'
            }
        else:
            return {
                'filename': self.filename,
                'count': 1,
                'type': 'Object'
            }
    
    def preview(self, n=5):
        if isinstance(self.data, list):
            return self.data[:n]
        else:
            return [self.data]
    
    def get_fields(self):
        """Get field names from first document"""
        if not self.data:
            return []
        
        sample = self.data[0] if isinstance(self.data, list) else self.data
        return self._extract_fields(sample)
    
    def _extract_fields(self, obj, prefix=''):
        """Extract field names recursively"""
        fields = []
        if isinstance(obj, dict):
            for key, value in obj.items():
                field_name = f"{prefix}.{key}" if prefix else key
                fields.append(field_name)
                if isinstance(value, dict):
                    fields.extend(self._extract_fields(value, field_name))
        return fields
    
    def add_filter(self, field, operator, value):
        self.operations.append({
            'type': 'filter',
            'field': field,
            'operator': operator,
            'value': value,
            'description': f"Filter: {field} {operator} {value}"
        })
    
    def add_join(self, main_field, join_field):
        """Add JOIN/Lookup operation"""
        self.operations.append({
            'type': 'join',
            'main_field': main_field,
            'join_field': join_field,
            'description': f"Join on {main_field} = {join_field}"
        })
    
    def add_projection(self, fields):
        self.operations.append({
            'type': 'project',
            'fields': fields,
            'description': f"Project: {', '.join(fields)}"
        })
    
    def add_groupby(self, field):
        self.operations.append({
            'type': 'groupby',
            'field': field,
            'description': f"Group By: {field}"
        })
    
    def add_limit(self, n):
        self.operations.append({
            'type': 'limit',
            'value': n,
            'description': f"Limit: {n}"
        })
    
    def get_operations(self):
        return self.operations
    
    def remove_operation(self, index):
        if 0 <= index < len(self.operations):
            self.operations.pop(index)
    
    def clear_operations(self):
        self.operations = []
    
    def execute(self):
        """Execute operations"""
        try:
            from nosql_executor import NoSQLExecutor
            executor = NoSQLExecutor(self.data, self.join_data)
            return executor.execute(self.operations)
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return None