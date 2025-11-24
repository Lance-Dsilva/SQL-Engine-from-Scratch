"""
NoSQL Query Executor with JOIN/Lookup support
"""

class NoSQLExecutor:
    def __init__(self, data, join_data=None):
        self.data = data
        self.join_data = join_data
    
    def execute(self, operations):
        if isinstance(self.data, list):
            result = self.data[:]
        else:
            result = [self.data]
        
        for op in operations:
            if op['type'] == 'filter':
                result = self._apply_filter(result, op)
            elif op['type'] == 'join':
                result = self._apply_join(result, op)
            elif op['type'] == 'project':
                result = self._apply_projection(result, op)
            elif op['type'] == 'groupby':
                result = self._apply_groupby(result, op)
            elif op['type'] == 'limit':
                result = result[:op['value']]
        
        return result
    
    def _apply_filter(self, data, op):
        field = op['field']
        operator = op['operator']
        value = op['value']
        
        filtered = []
        for doc in data:
            doc_val = self._get_nested_value(doc, field)
            
            match = False
            if operator == '==':
                match = str(doc_val) == str(value)
            elif operator == '!=':
                match = str(doc_val) != str(value)
            elif operator == 'contains':
                match = value.lower() in str(doc_val).lower()
            elif operator in ['>', '<']:
                try:
                    if operator == '>':
                        match = float(doc_val) > float(value)
                    else:
                        match = float(doc_val) < float(value)
                except:
                    pass
            
            if match:
                filtered.append(doc)
        
        return filtered
    
    def _apply_join(self, data, op):
        """JOIN/Lookup operation - similar to MongoDB $lookup"""
        if not self.join_data:
            return data
        
        main_field = op['main_field']
        join_field = op['join_field']
        
        # Create index on join collection
        join_index = {}
        join_collection = self.join_data if isinstance(self.join_data, list) else [self.join_data]
        
        for doc in join_collection:
            key = str(self._get_nested_value(doc, join_field))
            if key not in join_index:
                join_index[key] = []
            join_index[key].append(doc)
        
        # Perform lookup
        result = []
        for doc in data:
            key = str(self._get_nested_value(doc, main_field))
            
            # Create new document with joined data
            new_doc = doc.copy()
            
            if key in join_index:
                # Add matched documents as array (like MongoDB $lookup)
                new_doc['joined_data'] = join_index[key]
            else:
                new_doc['joined_data'] = []
            
            result.append(new_doc)
        
        return result
    
    def _apply_projection(self, data, op):
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
        field = op['field']
        groups = {}
        
        for doc in data:
            key = str(self._get_nested_value(doc, field))
            if key not in groups:
                groups[key] = []
            groups[key].append(doc)
        
        return [{field: k, 'count': len(v)} for k, v in groups.items()]
    
    def _get_nested_value(self, obj, path):
        keys = path.split('.')
        value = obj
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value
    
    def _set_nested_value(self, obj, path, value):
        keys = path.split('.')
        for key in keys[:-1]:
            if key not in obj:
                obj[key] = {}
            obj = obj[key]
        obj[keys[-1]] = value
