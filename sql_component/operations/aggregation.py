"""
Aggregation Operations

Compute aggregate statistics on grouped or full datasets
"""

class Aggregation:
    """Aggregate functions for data analysis"""
    
    @staticmethod
    def sum(values):
        """
        Calculate sum of values
        
        Args:
            values: List of numeric values
            
        Returns:
            float: Sum of values (excluding None)
        """
        total = 0
        count = 0
        
        for val in values:
            if val is not None:
                try:
                    total += float(val)
                    count += 1
                except (ValueError, TypeError):
                    continue
        
        return total if count > 0 else None
    
    @staticmethod
    def count(values):
        """Count non-null values"""
        return len([v for v in values if v is not None])
    
    @staticmethod
    def avg(values):
        """
        Calculate average of values
        
        Args:
            values: List of numeric values
            
        Returns:
            float: Average (mean) of values
        """
        non_null = []
        
        for val in values:
            if val is not None:
                try:
                    non_null.append(float(val))
                except (ValueError, TypeError):
                    continue
        
        if not non_null:
            return None
        
        return sum(non_null) / len(non_null)
    
    @staticmethod
    def max_val(values):
        """Find maximum value"""
        non_null = [v for v in values if v is not None]
        
        if not non_null:
            return None
        
        max_v = non_null[0]
        for val in non_null[1:]:
            try:
                if val > max_v:
                    max_v = val
            except TypeError:
                continue
        
        return max_v
    
    @staticmethod
    def min_val(values):
        """Find minimum value"""
        non_null = [v for v in values if v is not None]
        
        if not non_null:
            return None
        
        min_v = non_null[0]
        for val in non_null[1:]:
            try:
                if val < min_v:
                    min_v = val
            except TypeError:
                continue
        
        return min_v
    
    @staticmethod
    def median(values):
        """Calculate median value"""
        non_null = []
        
        for val in values:
            if val is not None:
                try:
                    non_null.append(float(val))
                except (ValueError, TypeError):
                    continue
        
        if not non_null:
            return None
        
        # Sort values
        sorted_vals = sorted(non_null)
        n = len(sorted_vals)
        
        if n % 2 == 0:
            return (sorted_vals[n//2 - 1] + sorted_vals[n//2]) / 2
        else:
            return sorted_vals[n//2]
    
    @staticmethod
    def apply_to_groups(groups, table_headers, agg_specs):
        """
        Apply aggregation functions to grouped data
        
        Args:
            groups: Output from GroupBy.apply()
            table_headers: Original table headers
            agg_specs: Dict like {'column_name': ['sum', 'avg', 'count']}
            
        Returns:
            list: List of aggregation results
        """
        results = []
        
        # Map function names to functions
        func_map = {
            'sum': Aggregation.sum,
            'count': Aggregation.count,
            'avg': Aggregation.avg,
            'average': Aggregation.avg,
            'mean': Aggregation.avg,
            'max': Aggregation.max_val,
            'min': Aggregation.min_val,
            'median': Aggregation.median,
        }
        
        for group_key, rows in groups.items():
            result_row = {'group_key': group_key}
            
            # Apply each aggregation
            for col_name, agg_funcs in agg_specs.items():
                if col_name not in table_headers:
                    continue
                
                col_idx = table_headers.index(col_name)
                
                # Extract column values
                values = [row[col_idx] if col_idx < len(row) else None 
                         for row in rows]
                
                # Apply each function
                if isinstance(agg_funcs, str):
                    agg_funcs = [agg_funcs]
                
                for func_name in agg_funcs:
                    func_name_lower = func_name.lower()
                    if func_name_lower in func_map:
                        func = func_map[func_name_lower]
                        result_row[f'{col_name}_{func_name}'] = func(values)
            
            results.append(result_row)
        
        return results


# TODO: Add standard deviation, variance
# TODO: Add percentile calculations
# TODO: Add custom aggregation functions
