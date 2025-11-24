from csv_parser import CSVParser
from merge_sort import _execute_chunked_external_orderby

class QueryExecutor:
    def __init__(self, data, headers, file_path=None, chunked=False, chunk_size=None, 
                 join_data=None, join_headers=None):
        self.data = data
        self.headers = headers
        self.file_path = file_path
        self.chunked = chunked
        self.chunk_size = chunk_size
        self.join_data = join_data
        self.join_headers = join_headers
    
    def execute(self, operations):
        """Execute all operations"""
        if self.chunked:
            return self._execute_chunked(operations)
        else:
            return self._execute_normal(operations)
    
    def _execute_normal(self, operations):
        """Execute on full dataset"""
        result = self.data
        
        for op in operations:
            if op['type'] == 'select':
                result = self._apply_select(result, op)
            elif op['type'] == 'join':
                result = self._apply_join(result, op)
            elif op['type'] == 'filter':
                result = self._apply_filter(result, op)
            elif op['type'] == 'groupby':
                result = self._apply_groupby(result, op)
            elif op['type'] == 'having':
                result = self._apply_having(result, op)
            elif op['type'] == 'orderby':
                result = self._apply_orderby(result, op)
            elif op['type'] == 'limit':
                result = result[:op['value']]
        
        return result
    
    def _execute_chunked(self, operations):
        """Execute operations in chunked mode"""
        # Detect expensive operations
        has_join = any(op['type'] == 'join' for op in operations)
        has_orderby = any(op['type'] == 'orderby' for op in operations)
        has_group_ops = any(op['type'] in ('groupby', 'having') for op in operations)

        # JOIN, no streaming load data
        if has_join:
            parser = CSVParser()
            all_data = []
            for chunk in parser.parse_file_in_chunks(self.file_path, self.chunk_size):
                all_data.extend(chunk)
            self.data = all_data
            self.chunked = False
            return self._execute_normal(operations)

        # ORDER BY without GROUP BY → external merge sort
        if has_orderby and not has_group_ops:
            return self._execute_chunked_external_orderby(operations)

        # Otherwise → simple chunk-based execution
        return self._execute_chunked_simple(operations)
    
    def _execute_chunked_simple(self, operations):
        """Simple chunked execution for filters, selects, group by, limit"""
        parser = CSVParser()
        results = []

        # Extract operations
        limit_val = None
        groupby_ops = []
        simple_ops = []

        for op in operations:
            if op['type'] == 'limit' and limit_val is None:
                limit_val = op['value']
            elif op['type'] == 'groupby':
                groupby_ops.append(op)
            elif op['type'] in ('filter', 'select'):
                simple_ops.append(op)

        # Stream chunks
        for chunk in parser.parse_file_in_chunks(self.file_path, self.chunk_size):
            if not chunk:
                continue

            # Apply simple ops (filter/select)
            chunk_result = self._process_simple_ops_on_chunk(chunk, simple_ops)

            if not chunk_result:
                continue

            # LIMIT handling — stop early
            if limit_val is not None:
                remaining = max(0, limit_val - len(results))
                if remaining <= 0:
                    break
                results.extend(chunk_result[:remaining])
                if len(results) >= limit_val:
                    break
            else:
                results.extend(chunk_result)

        # Apply GROUP BY at the very end
        for op in groupby_ops:
            results = self._apply_groupby(results, op)

        return results


    
    def _apply_select(self, data, op):
        """SELECT columns"""
        columns = op['columns']
        return [{col: row.get(col) for col in columns} for row in data]
    
    def _apply_join(self, data, op):
        """JOIN with another table"""
        if not self.join_data:
            return data
        
        join_type = op['join_type']
        main_key = op['main_key']
        join_key = op['join_key']
        
        # Create index on join table
        join_index = {}
        for row in self.join_data:
            key = row.get(join_key)
            if key not in join_index:
                join_index[key] = []
            join_index[key].append(row)
        
        result = []
        
        if join_type == "INNER":
            for row in data:
                key = row.get(main_key)
                if key in join_index:
                    for join_row in join_index[key]:
                        merged = {**row, **join_row}
                        result.append(merged)
        
        elif join_type == "LEFT":
            for row in data:
                key = row.get(main_key)
                if key in join_index:
                    for join_row in join_index[key]:
                        merged = {**row, **join_row}
                        result.append(merged)
                else:
                    result.append(row)
        
        elif join_type == "RIGHT":
            # First add all join table rows
            for key, join_rows in join_index.items():
                matched = False
                for row in data:
                    if row.get(main_key) == key:
                        matched = True
                        for join_row in join_rows:
                            merged = {**row, **join_row}
                            result.append(merged)
                
                if not matched:
                    for join_row in join_rows:
                        result.append(join_row)
        
        return result
    
    def _apply_filter(self, data, op):
        """WHERE filter"""
        column = op['column']
        operator = op['operator']
        value = op['value']
        
        # Try to convert to number
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
            
            match = False
            if operator == '>':
                match = row_val > value
            elif operator == '<':
                match = row_val < value
            elif operator == '>=':
                match = row_val >= value
            elif operator == '<=':
                match = row_val <= value
            elif operator == '==':
                match = row_val == value
            elif operator == '!=':
                match = row_val != value
            
            if match:
                filtered.append(row)
        
        return filtered
    
    def _apply_groupby(self, data, op):
        """GROUP BY"""
        column = op['column']
        groups = {}
        
        for row in data:
            key = str(row.get(column))
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        
        # If aggregation
        if op.get('agg_func'):
            agg_func = op['agg_func']
            agg_column = op['agg_column']
            result = []
            
            for key, rows in groups.items():
                agg_result = {column: key}
                
                if agg_func == 'COUNT':
                    agg_result['count'] = len(rows)
                elif agg_func in ['SUM', 'AVG', 'MIN', 'MAX']:
                    values = []
                    for row in rows:
                        try:
                            values.append(float(row.get(agg_column, 0)))
                        except:
                            pass
                    
                    if values:
                        if agg_func == 'SUM':
                            agg_result['sum'] = sum(values)
                        elif agg_func == 'AVG':
                            agg_result['avg'] = sum(values) / len(values)
                        elif agg_func == 'MIN':
                            agg_result['min'] = min(values)
                        elif agg_func == 'MAX':
                            agg_result['max'] = max(values)
                
                result.append(agg_result)
            
            return result
        else:
            return [{'group': k, 'count': len(v)} for k, v in groups.items()]
    
    def _apply_orderby(self, data, op):
        """ORDER BY"""
        column = op['column']
        direction = op['direction']
        reverse = (direction == 'DESC')
        
        def get_sort_key(row):
            val = row.get(column)
            try:
                return float(val)
            except:
                return str(val)
        
        return sorted(data, key=get_sort_key, reverse=reverse)
    
    def _apply_having(self, data, op):
        """HAVING - filters grouped results"""
        
        if not isinstance(data, list) or len(data) == 0:
            return data
        
        function = op['function']
        operator = op['operator']
        value = op['value']
        
        try:
            value = float(value)
        except:
            pass
        
        filtered = []
        for row in data:
            # Find the aggregated value in the row
            agg_val = None
            
            # Look for common aggregate result keys
            if function == 'COUNT':
                agg_val = row.get('count')
            elif function == 'SUM':
                agg_val = row.get('sum')
            elif function == 'AVG':
                agg_val = row.get('avg')
            elif function == 'MIN':
                agg_val = row.get('min')
            elif function == 'MAX':
                agg_val = row.get('max')
            
            if agg_val is None:
                continue
            
            # Apply condition
            try:
                agg_val = float(agg_val)
                match = False
                
                if operator == '>':
                    match = agg_val > value
                elif operator == '<':
                    match = agg_val < value
                elif operator == '>=':
                    match = agg_val >= value
                elif operator == '<=':
                    match = agg_val <= value
                elif operator == '==':
                    match = agg_val == value
                
                if match:
                    filtered.append(row)
            except:
                continue
        
        return filtered

    def _process_simple_ops_on_chunk(self, chunk, simple_ops):
        result = chunk
        for op in simple_ops:
            if op['type'] == 'filter':
                result = self._apply_filter(result, op)
            elif op['type'] == 'select':
                result = self._apply_select(result, op)
        return result
