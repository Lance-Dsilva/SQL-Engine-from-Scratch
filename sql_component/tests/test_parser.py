"""
Unit Tests for CSV Parser
"""

def test_basic_parsing():
    """Test basic CSV parsing functionality"""
    from sql_component.parsers.csv_parser import CSVParser
    
    parser = CSVParser()
    
    # Test line parsing
    line = "Alice,25,Engineer"
    result = parser.parse_line(line)
    assert result == ["Alice", "25", "Engineer"], f"Expected ['Alice', '25', 'Engineer'], got {result}"
    print("✓ Basic line parsing test passed")

def test_quoted_fields():
    """Test parsing with quoted fields"""
    from sql_component.parsers.csv_parser import CSVParser
    
    parser = CSVParser()
    
    # Test quoted field with comma
    line = '\"Smith, John\",30,Manager'
    result = parser.parse_line(line)
    assert result[0] == "Smith, John", f"Expected 'Smith, John', got {result[0]}"
    print("✓ Quoted fields test passed")

def test_data_type_inference():
    """Test data type inference"""
    from sql_component.parsers.data_types import DataTypeInferrer
    
    inferrer = DataTypeInferrer()
    
    # Test integer
    val, typ = inferrer.infer_type("123")
    assert typ == "integer", f"Expected 'integer', got {typ}"
    
    # Test float
    val, typ = inferrer.infer_type("123.45")
    assert typ == "float", f"Expected 'float', got {typ}"
    
    # Test string
    val, typ = inferrer.infer_type("hello")
    assert typ == "string", f"Expected 'string', got {typ}"
    
    print("✓ Data type inference test passed")

def run_all_tests():
    """Run all parser tests"""
    print("\nRunning CSV Parser Tests...")
    print("=" * 50)
    
    test_basic_parsing()
    test_quoted_fields()
    test_data_type_inference()
    
    print("=" * 50)
    print("All parser tests passed!\n")

if __name__ == "__main__":
    run_all_tests()
