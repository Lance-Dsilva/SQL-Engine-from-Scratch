"""
NoSQL Engine Component
Handles JSON parsing and document operations
"""
import json

class NoSQLEngine:
    def __init__(self):
        self.data = None
        self.filename = None
        self.operations = []
    
    def load_data(self, file_obj):
        """Load JSON file"""
        try:
            from parsers.json_parser import JSONParser
            
            parser = JSONParser()
            self.data = parser.parse_file(file_obj)
            self.filename = file_obj.name
            self.operations = []
            return True, f"Loaded {file_obj.name} successfully"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def is_loaded(self):
        return self.data is not None
    
    def get_info(self):
        if isinstance(self.data, list):
            return {
                'filename': self.filename,
                'count': len(self.data),
                'type': 'Array of documents'
            }
        else:
            return {
                'filename': self.filename,
                'count': 1,
                'type': 'Single document'
            }
    
    def preview(self, n=5):
        if isinstance(self.data, list):
            return self.data[:n]
        else:
            return [self.data]
    
    def get_fields(self):
        """Extract field names from data"""
        if not self.data:
            return []
        
        sample = self.data[0] if isinstance(self.data, list) else self.data
        return self._extract_fields(sample)
    
    def _extract_fields(self, obj, prefix=''):
        """Recursively extract field names"""
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
            'description': f"{field} {operator} {value}"
        })
    
    def add_projection(self, fields):
        if fields:
            self.operations.append({
                'type': 'project',
                'fields': fields,
                'description': f"PROJECT {', '.join(fields)}"
            })
    
    def add_groupby(self, field):
        self.operations.append({
            'type': 'groupby',
            'field': field,
            'description': f"GROUP BY {field}"
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
            from operations.nosql_executor import NoSQLExecutor
            
            executor = NoSQLExecutor(self.data)
            return executor.execute(self.operations)
        except Exception as e:
            print(f"Execution error: {e}")
            return None
