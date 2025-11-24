"""
SQL Query Executor
"""

class QueryExecutor:
    def __init__(self, data, headers, file_path=None, chunked=False, chunk_size=None):
        self.data = data
        self.headers = headers
        self.file_path = file_path
        self.chunked = chunked
        self.chunk_size = chunk_size
    
    def execute(self, operations):
        """Execute query operations"""
        if self.chunked:
            return self._execute_chunked(operations)
        else:
            return self._execute_normal(operations)
    
    def _execute_normal(self, operations):
        """Execute on full dataset"""
        result = self.data
        
        for op in operations:
            if op['type'] == 'filter':
                result = self._apply_filter(result, op)
            elif op['type'] == 'select':
                result = self._apply_select(result, op)
            elif op['type'] == 'groupby':
                result = self._apply_groupby(result, op)
            elif op['type'] == 'limit':
                result = result[:op['value']]
        
        return result
    
    def _execute_chunked(self, operations):
        """Execute with chunking"""
        from parsers.csv_parser import CSVParser
        
        parser = CSVParser()
        results = []
        limit_val = None
        
        # Check for limit
        for op in operations:
            if op['type'] == 'limit':
                limit_val = op['value']
        
        for chunk in parser.parse_file_in_chunks(self.file_path, self.chunk_size):
            chunk_result = chunk
            
            for op in operations:
                if op['type'] == 'filter':
                    chunk_result = self._apply_filter(chunk_result, op)
                elif op['type'] == 'select':
                    chunk_result = self._apply_select(chunk_result, op)
            
            results.extend(chunk_result)
            
            if limit_val and len(results) >= limit_val:
                results = results[:limit_val]
                break
        
        # Apply groupby if present
        for op in operations:
            if op['type'] == 'groupby':
                results = self._apply_groupby(results, op)
        
        return results
    
    def _apply_filter(self, data, op):
        """Apply filter operation"""
        column = op['column']
        operator = op['operator']
        value = op['value']
        
        # Try to convert value to number
        try:
            value = float(value)
        except:
            pass
        
        filtered = []
        for row in data:
            row_val = row.get(column)
            try:
                row_val = float(row_val)
            except:
                pass
            
            if operator == '>':
                if row_val > value:
                    filtered.append(row)
            elif operator == '<':
                if row_val < value:
                    filtered.append(row)
            elif operator == '>=':
                if row_val >= value:
                    filtered.append(row)
            elif operator == '<=':
                if row_val <= value:
                    filtered.append(row)
            elif operator == '==':
                if row_val == value:
                    filtered.append(row)
            elif operator == '!=':
                if row_val != value:
                    filtered.append(row)
        
        return filtered
    
    def _apply_select(self, data, op):
        """Apply select operation"""
        columns = op['columns']
        return [{col: row.get(col) for col in columns} for row in data]
    
    def _apply_groupby(self, data, op):
        """Apply group by operation"""
        column = op['column']
        groups = {}
        
        for row in data:
            key = row.get(column)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        
        # If aggregation specified
        if op.get('agg_func'):
            agg_func = op['agg_func']
            agg_column = op['agg_column']
            result = []
            
            for key, rows in groups.items():
                if agg_func == 'COUNT':
                    result.append({column: key, 'count': len(rows)})
                elif agg_func in ['SUM', 'AVG', 'MIN', 'MAX']:
                    values = [float(row.get(agg_column, 0)) for row in rows]
                    if agg_func == 'SUM':
                        result.append({column: key, 'sum': sum(values)})
                    elif agg_func == 'AVG':
                        result.append({column: key, 'avg': sum(values)/len(values) if values else 0})
                    elif agg_func == 'MIN':
                        result.append({column: key, 'min': min(values)})
                    elif agg_func == 'MAX':
                        result.append({column: key, 'max': max(values)})
            
            return result
        else:
            return [{'group': k, 'count': len(v)} for k, v in groups.items()]
