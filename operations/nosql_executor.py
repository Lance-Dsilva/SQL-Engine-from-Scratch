"""
NoSQL Query Executor
"""

class NoSQLExecutor:
    def __init__(self, data):
        self.data = data
    
    def execute(self, operations):
        """Execute query operations"""
        if isinstance(self.data, list):
            result = self.data[:]
        else:
            result = [self.data]
        
        for op in operations:
            if op['type'] == 'filter':
                result = self._apply_filter(result, op)
            elif op['type'] == 'project':
                result = self._apply_projection(result, op)
            elif op['type'] == 'groupby':
                result = self._apply_groupby(result, op)
            elif op['type'] == 'limit':
                result = result[:op['value']]
        
        return result
    
    def _apply_filter(self, data, op):
        """Apply filter operation"""
        field = op['field']
        operator = op['operator']
        value = op['value']
        
        filtered = []
        for doc in data:
            doc_val = self._get_nested_value(doc, field)
            
            if operator == '==':
                if doc_val == value:
                    filtered.append(doc)
            elif operator == '!=':
                if doc_val != value:
                    filtered.append(doc)
            elif operator == 'contains':
                if value.lower() in str(doc_val).lower():
                    filtered.append(doc)
            elif operator == '>':
                try:
                    if float(doc_val) > float(value):
                        filtered.append(doc)
                except:
                    pass
            elif operator == '<':
                try:
                    if float(doc_val) < float(value):
                        filtered.append(doc)
                except:
                    pass
        
        return filtered
    
    def _apply_projection(self, data, op):
        """Apply projection operation"""
        fields = op['fields']
        projected = []
        
        for doc in data:
            new_doc = {}
            for field in fields:
                value = self._get_nested_value(doc, field)
                self._set_nested_value(new_doc, field, value)
            projected.append(new_doc)
        
        return projected
    
    def _apply_groupby(self, data, op):
        """Apply group by operation"""
        field = op['field']
        groups = {}
        
        for doc in data:
            key = self._get_nested_value(doc, field)
            key = str(key)
            if key not in groups:
                groups[key] = []
            groups[key].append(doc)
        
        return [{field: k, 'count': len(v), 'documents': v} for k, v in groups.items()]
    
    def _get_nested_value(self, obj, path):
        """Get value from nested dict using dot notation"""
        keys = path.split('.')
        value = obj
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value
    
    def _set_nested_value(self, obj, path, value):
        """Set value in nested dict using dot notation"""
        keys = path.split('.')
        for key in keys[:-1]:
            if key not in obj:
                obj[key] = {}
            obj = obj[key]
        obj[keys[-1]] = value
