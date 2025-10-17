"""
Unit Tests for SQL Operations
"""

def test_filter_operation():
    """Test filter operation"""
    from sql_component.core.table import Table
    from sql_component.operations.filter import Filter
    
    # Create sample table
    headers = ['name', 'age', 'salary']
    data = [
        ['Alice', 25, 50000],
        ['Bob', 30, 60000],
        ['Carol', 35, 70000],
    ]
    table = Table(headers, data, 'employees')
    
    # Filter age > 25
    result = Filter.apply(table, lambda row: row['age'] > 25)
    
    assert result.row_count() == 2, f"Expected 2 rows, got {result.row_count()}"
    print("✓ Filter operation test passed")

def test_projection_operation():
    """Test projection operation"""
    from sql_component.core.table import Table
    from sql_component.operations.projection import Projection
    
    # Create sample table
    headers = ['name', 'age', 'salary']
    data = [
        ['Alice', 25, 50000],
        ['Bob', 30, 60000],
    ]
    table = Table(headers, data, 'employees')
    
    # Select only name and age
    result = Projection.apply(table, ['name', 'age'])
    
    assert result.col_count() == 2, f"Expected 2 columns, got {result.col_count()}"
    assert 'salary' not in result.headers, "Salary should not be in result"
    print("✓ Projection operation test passed")

def test_aggregation():
    """Test aggregation functions"""
    from sql_component.operations.aggregation import Aggregation
    
    values = [10, 20, 30, 40, 50]
    
    # Test sum
    result = Aggregation.sum(values)
    assert result == 150, f"Expected 150, got {result}"
    
    # Test average
    result = Aggregation.avg(values)
    assert result == 30, f"Expected 30, got {result}"
    
    # Test count
    result = Aggregation.count(values)
    assert result == 5, f"Expected 5, got {result}"
    
    print("✓ Aggregation functions test passed")

def run_all_tests():
    """Run all operation tests"""
    print("\nRunning SQL Operations Tests...")
    print("=" * 50)
    
    test_filter_operation()
    test_projection_operation()
    test_aggregation()
    
    print("=" * 50)
    print("All operations tests passed!\n")

if __name__ == "__main__":
    run_all_tests()
