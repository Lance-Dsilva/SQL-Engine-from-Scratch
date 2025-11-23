import streamlit as st
import os
import sys

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sql_component.parsers.csv_parser import CSVParser
from sql_component.parsers.data_types import DataTypeInferrer
from sql_component.parsers.chunk_reader import ChunkReader
from sql_component.core.table import Table
from sql_component.core.query_engine import QueryEngine
from sql_component.operations.filter import Filter
from sql_component.operations.projection import Projection
from sql_component.operations.groupby import GroupBy
from sql_component.operations.aggregation import Aggregation
from sql_component.operations.join import Join

# Page configuration
st.set_page_config(
    page_title="SQL Component Tester",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #2c3e50;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .info-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        color: #0c5460;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize session state
if 'current_table' not in st.session_state:
    st.session_state.current_table = None
if 'tables' not in st.session_state:
    st.session_state.tables = {}
if 'operation_history' not in st.session_state:
    st.session_state.operation_history = []

def create_sample_data():
    """Create sample datasets for testing"""
    
    # Sample 1: Products
    products_headers = ['product_id', 'name', 'category', 'price', 'brand']
    products_data = [
        ['P001', 'Laptop', 'Electronics', '999.99', 'TechBrand'],
        ['P002', 'Mouse', 'Electronics', '29.99', 'TechBrand'],
        ['P003', 'Keyboard', 'Electronics', '79.99', 'TechBrand'],
        ['P004', 'T-Shirt', 'Clothing', '19.99', 'FashionCo'],
        ['P005', 'Jeans', 'Clothing', '49.99', 'FashionCo'],
        ['P006', 'Desk', 'Furniture', '299.99', 'HomePlus'],
        ['P007', 'Chair', 'Furniture', '149.99', 'HomePlus'],
        ['P008', 'Monitor', 'Electronics', '399.99', 'TechBrand'],
        ['P009', 'Shoes', 'Clothing', '79.99', 'FashionCo'],
        ['P010', 'Lamp', 'Furniture', '39.99', 'HomePlus'],
    ]
    
    # Sample 2: Sales
    sales_headers = ['sale_id', 'product_id', 'customer_id', 'quantity', 'amount', 'date']
    sales_data = [
        ['S001', 'P001', 'C001', '1', '999.99', '2024-01-15'],
        ['S002', 'P002', 'C002', '2', '59.98', '2024-01-16'],
        ['S003', 'P003', 'C001', '1', '79.99', '2024-01-17'],
        ['S004', 'P004', 'C003', '3', '59.97', '2024-01-18'],
        ['S005', 'P005', 'C002', '2', '99.98', '2024-01-19'],
        ['S006', 'P006', 'C004', '1', '299.99', '2024-01-20'],
        ['S007', 'P007', 'C003', '2', '299.98', '2024-01-21'],
        ['S008', 'P008', 'C001', '1', '399.99', '2024-01-22'],
        ['S009', 'P001', 'C005', '1', '999.99', '2024-01-23'],
        ['S010', 'P009', 'C002', '1', '79.99', '2024-01-24'],
    ]
    
    # Sample 3: Customers
    customers_headers = ['customer_id', 'name', 'age', 'location', 'segment']
    customers_data = [
        ['C001', 'Alice Johnson', '28', 'New York', 'Premium'],
        ['C002', 'Bob Smith', '35', 'Los Angeles', 'Regular'],
        ['C003', 'Carol White', '42', 'Chicago', 'Premium'],
        ['C004', 'David Brown', '31', 'Houston', 'Regular'],
        ['C005', 'Eve Davis', '26', 'Phoenix', 'Premium'],
    ]
    
    return {
        'products': Table(products_headers, products_data, 'products'),
        'sales': Table(sales_headers, sales_data, 'sales'),
        'customers': Table(customers_headers, customers_data, 'customers')
    }

def display_table(table, title="Table"):
    """Display table in a nice format"""
    if table is None:
        st.warning("No table to display")
        return
    
    st.markdown(f"**{title}**")
    st.markdown(f"*Shape: {table.shape()} (rows × columns)*")
    
    # Convert to display format
    if table.data:
        # Create a list of dictionaries for better display
        display_data = []
        for row in table.data:
            row_dict = {}
            for i, header in enumerate(table.headers):
                row_dict[header] = row[i] if i < len(row) else None
            display_data.append(row_dict)
        
        st.dataframe(display_data, use_container_width=True)
        
        # Show statistics
        st.markdown("**📊 Table Statistics:**")
        col1, col2, col3 = st.columns(3)
        col1.metric("Rows", table.row_count())
        col2.metric("Columns", table.col_count())
        col3.metric("Total Cells", table.row_count() * table.col_count())
            
        st.markdown("**Column Types:**")
        st.json(table.types)
    else:
        st.info("Table is empty")

def main():
    st.markdown('<h1 class="main-header">🗄️ SQL Component Tester</h1>', unsafe_allow_html=True)
    st.markdown("---")
    
    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/fluency/96/000000/database.png", width=80)
        st.title("Navigation")
        
        page = st.radio(
            "Select Operation:",
            [
                "🏠 Home",
                "📁 Load Data",
                "🔍 Filter Operation",
                "📋 Projection Operation",
                "📊 Group By & Aggregation",
                "🔗 Join Operations",
                "⚡ Query Engine",
                "📦 Chunk Processing",
                "🧪 All Operations Demo"
            ]
        )
        
        st.markdown("---")
        
        # Load sample data button
        if st.button("🎲 Load Sample Data", use_container_width=True):
            st.session_state.tables = create_sample_data()
            st.session_state.current_table = st.session_state.tables['products']
            st.success("Sample data loaded!")
            st.rerun()
        
        # Show loaded tables
        if st.session_state.tables:
            st.markdown("### Loaded Tables:")
            for name in st.session_state.tables.keys():
                if st.button(f"📄 {name}", key=f"nav_{name}", use_container_width=True):
                    st.session_state.current_table = st.session_state.tables[name]
                    st.rerun()
    
    # Main content area
    if page == "🏠 Home":
        show_home()
    elif page == "📁 Load Data":
        show_load_data()
    elif page == "🔍 Filter Operation":
        show_filter_operation()
    elif page == "📋 Projection Operation":
        show_projection_operation()
    elif page == "📊 Group By & Aggregation":
        show_groupby_aggregation()
    elif page == "🔗 Join Operations":
        show_join_operations()
    elif page == "⚡ Query Engine":
        show_query_engine()
    elif page == "📦 Chunk Processing":
        show_chunk_processing()
    elif page == "🧪 All Operations Demo":
        show_all_operations_demo()

def show_home():
    """Home page"""
    st.markdown("## Welcome to SQL Component Tester! 👋")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 🎯 Features
        
        This application allows you to test custom SQL operations implemented from scratch:
        
        - **📁 Load Data**: Upload CSV files or use sample data
        - **🔍 Filter**: Apply conditions to filter rows
        - **📋 Projection**: Select specific columns
        - **📊 Group By**: Group data and compute aggregations
        - **🔗 Join**: Combine multiple tables
        - **⚡ Query Engine**: Chain multiple operations
        - **📦 Chunk Processing**: Handle large files efficiently
        """)
    
    with col2:
        st.markdown("""
        ### 🚀 Quick Start
        
        1. Click **"Load Sample Data"** in the sidebar
        2. Explore different operations from the menu
        3. Upload your own CSV files to test
        4. Chain operations using the Query Engine
        
        ### 📊 Sample Datasets
        
        - **Products**: Product catalog with pricing
        - **Sales**: Transaction records
        - **Customers**: Customer information
        """)
    
    st.markdown("---")
    
    # Quick stats
    if st.session_state.tables:
        st.markdown("### 📈 Currently Loaded Tables")
        cols = st.columns(len(st.session_state.tables))
        for idx, (name, table) in enumerate(st.session_state.tables.items()):
            with cols[idx]:
                st.info(f"**{name}**\n\n{table.row_count()} rows × {table.col_count()} cols")
    else:
        st.info("👈 Load sample data or upload your own CSV files to get started!")

def show_load_data():
    """Load data page"""
    st.markdown("## 📁 Load Data")
    
    tab1, tab2, tab3 = st.tabs(["📤 Upload CSV", "🎲 Sample Data", "✍️ Manual Input"])
    
    with tab1:
        st.markdown("### Upload CSV File")
        
        uploaded_file = st.file_uploader("Choose a CSV file", type=['csv'])
        
        if uploaded_file is not None:
            col1, col2 = st.columns(2)
            
            with col1:
                delimiter = st.text_input("Delimiter", value=",")
                has_header = st.checkbox("Has Header", value=True)
            
            with col2:
                table_name = st.text_input("Table Name", value="uploaded_data")
            
            if st.button("Parse CSV", type="primary"):
                try:
                    # Save uploaded file temporarily
                    temp_path = f"temp_{uploaded_file.name}"
                    with open(temp_path, 'wb') as f:
                        f.write(uploaded_file.getbuffer())
                    
                    # Parse CSV
                    parser = CSVParser(delimiter=delimiter)
                    result = parser.parse_file(temp_path, has_header=has_header)
                    
                    # Create table
                    table = Table(result['headers'], result['data'], table_name)
                    st.session_state.tables[table_name] = table
                    st.session_state.current_table = table
                    
                    # Clean up
                    os.remove(temp_path)
                    
                    st.success(f"✅ Successfully loaded {table.row_count()} rows!")
                    display_table(table, f"Loaded: {table_name}")
                    
                except Exception as e:
                    st.error(f"Error parsing CSV: {str(e)}")
    
    with tab2:
        st.markdown("### Sample Datasets")
        
        if st.button("Load All Sample Data", type="primary"):
            st.session_state.tables = create_sample_data()
            st.session_state.current_table = st.session_state.tables['products']
            st.success("✅ Sample data loaded successfully!")
        
        if st.session_state.tables:
            st.markdown("---")
            for name, table in st.session_state.tables.items():
                with st.expander(f"📄 {name.upper()}"):
                    display_table(table, name)
    
    with tab3:
        st.markdown("### Create Table Manually")
        
        col1, col2 = st.columns(2)
        with col1:
            table_name = st.text_input("Table Name", value="custom_table", key="manual_name")
            num_cols = st.number_input("Number of Columns", min_value=1, max_value=10, value=3)
        
        with col2:
            num_rows = st.number_input("Number of Rows", min_value=1, max_value=20, value=5)
        
        # Create input fields
        headers = []
        st.markdown("**Column Names:**")
        cols = st.columns(num_cols)
        for i in range(num_cols):
            with cols[i]:
                header = st.text_input(f"Col {i+1}", value=f"column_{i+1}", key=f"header_{i}")
                headers.append(header)
        
        st.markdown("**Data Rows:**")
        data = []
        for row_idx in range(num_rows):
            cols = st.columns(num_cols)
            row = []
            for col_idx in range(num_cols):
                with cols[col_idx]:
                    val = st.text_input(f"R{row_idx+1}", value="", key=f"cell_{row_idx}_{col_idx}", label_visibility="collapsed")
                    row.append(val)
            data.append(row)
        
        if st.button("Create Table", type="primary"):
            table = Table(headers, data, table_name)
            st.session_state.tables[table_name] = table
            st.session_state.current_table = table
            st.success(f"✅ Table '{table_name}' created!")
            display_table(table, table_name)

def show_filter_operation():
    """Filter operation page"""
    st.markdown("## 🔍 Filter Operation")
    
    if not st.session_state.current_table:
        st.warning("⚠️ Please load data first!")
        return
    
    table = st.session_state.current_table
    
    st.markdown("### Original Table")
    display_table(table, table.name)
    
    st.markdown("---")
    st.markdown("### Apply Filter")
    
    # Filter configuration
    col1, col2, col3 = st.columns(3)
    
    with col1:
        filter_column = st.selectbox("Column", table.headers)
    
    with col2:
        operator = st.selectbox("Operator", ["==", "!=", ">", "<", ">=", "<=", "contains", "startswith", "endswith"])
    
    with col3:
        filter_value = st.text_input("Value")
    
    # Additional options
    col1, col2 = st.columns(2)
    with col1:
        case_sensitive = st.checkbox("Case Sensitive (for text)", value=False)
    with col2:
        convert_numbers = st.checkbox("Auto-convert to numbers", value=True)
    
    if st.button("Apply Filter", type="primary"):
        try:
            # Create condition function
            def condition(row):
                val = row.get(filter_column)
                
                # Handle None
                if val is None:
                    return False
                
                # Convert value if needed
                compare_val = filter_value
                if convert_numbers:
                    try:
                        val = float(val)
                        compare_val = float(filter_value)
                    except (ValueError, TypeError):
                        pass
                
                # String operations
                if not case_sensitive and isinstance(val, str) and isinstance(compare_val, str):
                    val = val.lower()
                    compare_val = compare_val.lower()
                
                # Apply operator
                if operator == "==":
                    return val == compare_val
                elif operator == "!=":
                    return val != compare_val
                elif operator == ">":
                    return val > compare_val
                elif operator == "<":
                    return val < compare_val
                elif operator == ">=":
                    return val >= compare_val
                elif operator == "<=":
                    return val <= compare_val
                elif operator == "contains":
                    return compare_val in str(val)
                elif operator == "startswith":
                    return str(val).startswith(str(compare_val))
                elif operator == "endswith":
                    return str(val).endswith(str(compare_val))
                
                return False
            
            # Apply filter
            result = Filter.apply(table, condition)
            
            st.success(f"✅ Filter applied! {result.row_count()} rows match the condition.")
            
            st.markdown("### Filtered Result")
            display_table(result, f"Filtered: {table.name}")
            
            # Option to save result
            if st.button("💾 Save Filtered Result"):
                result_name = f"{table.name}_filtered"
                st.session_state.tables[result_name] = result
                st.success(f"Saved as '{result_name}'")
        
        except Exception as e:
            st.error(f"Error applying filter: {str(e)}")
    
    # Predefined filter examples
    st.markdown("---")
    st.markdown("### 🎯 Quick Filter Examples")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Show Electronics"):
            if 'category' in table.headers:
                result = Filter.apply(table, lambda row: row.get('category') == 'Electronics')
                display_table(result, "Electronics Products")
    
    with col2:
        if st.button("Price > 100"):
            if 'price' in table.headers:
                result = Filter.apply(table, lambda row: float(row.get('price', 0)) > 100)
                display_table(result, "High-priced Items")
    
    with col3:
        if st.button("Premium Customers"):
            if 'segment' in table.headers:
                result = Filter.apply(table, lambda row: row.get('segment') == 'Premium')
                display_table(result, "Premium Segment")

def show_projection_operation():
    """Projection operation page"""
    st.markdown("## 📋 Projection Operation")
    
    if not st.session_state.current_table:
        st.warning("⚠️ Please load data first!")
        return
    
    table = st.session_state.current_table
    
    st.markdown("### Original Table")
    display_table(table, table.name)
    
    st.markdown("---")
    st.markdown("### Select Columns")
    
    # Column selection
    selected_columns = st.multiselect(
        "Choose columns to keep:",
        options=table.headers,
        default=table.headers[:3] if len(table.headers) >= 3 else table.headers
    )
    
    if selected_columns:
        if st.button("Apply Projection", type="primary"):
            try:
                result = Projection.apply(table, selected_columns)
                
                st.success(f"✅ Projection applied! Selected {len(selected_columns)} columns.")
                
                st.markdown("### Projected Result")
                display_table(result, f"Projected: {table.name}")
                
                # Save option
                if st.button("💾 Save Projected Result"):
                    result_name = f"{table.name}_projected"
                    st.session_state.tables[result_name] = result
                    st.success(f"Saved as '{result_name}'")
            
            except Exception as e:
                st.error(f"Error applying projection: {str(e)}")
    else:
        st.info("Please select at least one column")
    
    # Quick examples
    st.markdown("---")
    st.markdown("### 🎯 Quick Examples")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Select IDs and Names"):
            id_cols = [c for c in table.headers if 'id' in c.lower()]
            name_cols = [c for c in table.headers if 'name' in c.lower()]
            cols = list(set(id_cols + name_cols))
            if cols:
                result = Projection.apply(table, cols)
                display_table(result, "IDs and Names")
    
    with col2:
        if st.button("First 3 Columns"):
            cols = table.headers[:3]
            result = Projection.apply(table, cols)
            display_table(result, "First 3 Columns")

def show_groupby_aggregation():
    """Group By and Aggregation page"""
    st.markdown("## 📊 Group By & Aggregation")
    
    if not st.session_state.current_table:
        st.warning("⚠️ Please load data first!")
        return
    
    table = st.session_state.current_table
    
    st.markdown("### Original Table")
    display_table(table, table.name)
    
    st.markdown("---")
    st.markdown("### Configure Grouping")
    
    col1, col2 = st.columns(2)
    
    with col1:
        group_columns = st.multiselect(
            "Group By Columns:",
            options=table.headers,
            default=[table.headers[0]] if table.headers else []
        )
    
    with col2:
        agg_column = st.selectbox("Column to Aggregate:", table.headers)
    
    # Aggregation functions
    st.markdown("### Select Aggregation Functions")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        do_sum = st.checkbox("Sum", value=True)
    with col2:
        do_avg = st.checkbox("Average", value=True)
    with col3:
        do_count = st.checkbox("Count", value=True)
    with col4:
        do_minmax = st.checkbox("Min/Max", value=False)
    
    if group_columns and st.button("Apply Group By & Aggregation", type="primary"):
        try:
            # Group data
            groups = GroupBy.apply(table, group_columns)
            
            st.success(f"✅ Found {len(groups)} groups")
            
            # Build aggregation specs
            agg_funcs = []
            if do_sum:
                agg_funcs.append('sum')
            if do_avg:
                agg_funcs.append('avg')
            if do_count:
                agg_funcs.append('count')
            if do_minmax:
                agg_funcs.extend(['min', 'max'])
            
            agg_specs = {agg_column: agg_funcs}
            
            # Apply aggregations
            results = Aggregation.apply_to_groups(groups, table.headers, agg_specs)
            
            st.markdown("### Aggregation Results")
            
            # Display results
            if results:
                # Create display format
                display_data = []
                for result in results:
                    row_dict = {}
                    # Add group keys
                    for i, col in enumerate(group_columns):
                        row_dict[col] = result['group_key'][i]
                    
                    # Add aggregations
                    for key, val in result.items():
                        if key != 'group_key':
                            row_dict[key] = val
                    
                    display_data.append(row_dict)
                
                st.dataframe(display_data, use_container_width=True)
                
                # Visualization
                st.markdown("### 📈 Visualization")
                
                # Bar chart for counts or sums
                if do_count or do_sum:
                    import json
                    chart_data = {}
                    for item in display_data:
                        key = str(item.get(group_columns[0], 'Unknown'))
                        if do_count:
                            chart_data[key] = item.get(f'{agg_column}_count', 0)
                        elif do_sum:
                            chart_data[key] = item.get(f'{agg_column}_sum', 0)
                    
                    st.bar_chart(chart_data)
            
        except Exception as e:
            st.error(f"Error in grouping/aggregation: {str(e)}")
    
    # Examples
    st.markdown("---")
    st.markdown("### 🎯 Quick Examples")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Count by First Column"):
            if table.headers:
                groups = GroupBy.apply(table, [table.headers[0]])
                results = Aggregation.apply_to_groups(
                    groups, table.headers, 
                    {table.headers[0]: ['count']}
                )
                st.json(results)
    
    with col2:
        if st.button("Sum by Category"):
            if 'category' in table.headers and 'price' in table.headers:
                groups = GroupBy.apply(table, ['category'])
                results = Aggregation.apply_to_groups(
                    groups, table.headers,
                    {'price': ['sum', 'avg', 'count']}
                )
                st.json(results)

def show_join_operations():
    """Join operations page"""
    st.markdown("## 🔗 Join Operations")
    
    if len(st.session_state.tables) < 2:
        st.warning("⚠️ Please load at least 2 tables to perform joins!")
        if st.button("Load Sample Data"):
            st.session_state.tables = create_sample_data()
            st.rerun()
        return
    
    st.markdown("### Select Tables to Join")
    
    col1, col2 = st.columns(2)
    
    table_names = list(st.session_state.tables.keys())
    
    with col1:
        left_table_name = st.selectbox("Left Table:", table_names, index=0)
        left_table = st.session_state.tables[left_table_name]
        st.markdown(f"**Preview: {left_table_name}**")
        display_table(left_table.head(5), left_table_name)
    
    with col2:
        right_table_name = st.selectbox("Right Table:", table_names, index=min(1, len(table_names)-1))
        right_table = st.session_state.tables[right_table_name]
        st.markdown(f"**Preview: {right_table_name}**")
        display_table(right_table.head(5), right_table_name)
    
    st.markdown("---")
    st.markdown("### Join Configuration")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        left_key = st.selectbox("Left Join Key:", left_table.headers)
    
    with col2:
        right_key = st.selectbox("Right Join Key:", right_table.headers)
    
    with col3:
        join_type = st.selectbox("Join Type:", ["inner", "left", "right", "outer"])
    
    if st.button("Perform Join", type="primary"):
        try:
            if join_type == "inner":
                result = Join.inner_join(left_table, right_table, left_key, right_key)
            elif join_type == "left":
                result = Join.left_join(left_table, right_table, left_key, right_key)
            elif join_type == "right":
                result = Join.right_join(left_table, right_table, left_key, right_key)
            else:  # outer
                result = Join.outer_join(left_table, right_table, left_key, right_key)
            
            st.success(f"✅ {join_type.upper()} JOIN completed! Result has {result.row_count()} rows.")
            
            st.markdown("### Join Result")
            display_table(result, f"{left_table_name} {join_type.upper()} JOIN {right_table_name}")
            
            # Save option
            if st.button("💾 Save Join Result"):
                result_name = f"{left_table_name}_join_{right_table_name}"
                st.session_state.tables[result_name] = result
                st.success(f"Saved as '{result_name}'")
        
        except Exception as e:
            st.error(f"Error performing join: {str(e)}")
    
    # Join type explanation
    st.markdown("---")
    with st.expander("ℹ️ Join Types Explained"):
        st.markdown("""
        - **Inner Join**: Returns only matching rows from both tables
        - **Left Join**: Returns all rows from left table, with nulls for non-matches
        - **Right Join**: Returns all rows from right table, with nulls for non-matches
        - **Outer Join**: Returns all rows from both tables, with nulls for non-matches
        """)

def show_query_engine():
    """Query Engine page"""
    st.markdown("## ⚡ Query Engine - Chain Multiple Operations")
    
    if not st.session_state.current_table:
        st.warning("⚠️ Please load data first!")
        return
    
    table = st.session_state.current_table
    
    st.markdown("### Original Table")
    display_table(table, table.name)
    
    st.markdown("---")
    st.markdown("### Build Your Query")
    
    # Query builder
    operations = []
    
    # Operation 1: Filter
    st.markdown("#### Step 1: Filter (Optional)")
    use_filter = st.checkbox("Add Filter", value=False, key="qe_filter")
    
    if use_filter:
        col1, col2, col3 = st.columns(3)
        with col1:
            filter_col = st.selectbox("Column", table.headers, key="qe_filter_col")
        with col2:
            filter_op = st.selectbox("Operator", [">", "<", "==", "!="], key="qe_filter_op")
        with col3:
            filter_val = st.text_input("Value", key="qe_filter_val")
        
        if filter_col and filter_val:
            operations.append(('filter', filter_col, filter_op, filter_val))
    
    # Operation 2: Select
    st.markdown("#### Step 2: Select Columns (Optional)")
    use_select = st.checkbox("Add Projection", value=False, key="qe_select")
    
    if use_select:
        select_cols = st.multiselect("Select Columns", table.headers, key="qe_select_cols")
        if select_cols:
            operations.append(('select', select_cols))
    
    # Operation 3: Group By
    st.markdown("#### Step 3: Group By & Aggregate (Optional)")
    use_groupby = st.checkbox("Add Group By", value=False, key="qe_groupby")
    
    if use_groupby:
        col1, col2 = st.columns(2)
        with col1:
            group_cols = st.multiselect("Group By", table.headers, key="qe_group_cols")
        with col2:
            agg_col = st.selectbox("Aggregate Column", table.headers, key="qe_agg_col")
            agg_func = st.multiselect("Functions", ['sum', 'avg', 'count', 'min', 'max'], 
                                     default=['count'], key="qe_agg_func")
        
        if group_cols and agg_col:
            operations.append(('groupby', group_cols, agg_col, agg_func))
    
    # Operation 4: Order By
    st.markdown("#### Step 4: Order By (Optional)")
    use_order = st.checkbox("Add Order By", value=False, key="qe_order")
    
    if use_order:
        col1, col2 = st.columns(2)
        with col1:
            order_col = st.selectbox("Order By Column", table.headers, key="qe_order_col")
        with col2:
            order_asc = st.radio("Direction", ["Ascending", "Descending"], key="qe_order_dir")
        
        if order_col:
            operations.append(('order', order_col, order_asc == "Ascending"))
    
    # Operation 5: Limit
    st.markdown("#### Step 5: Limit Results (Optional)")
    use_limit = st.checkbox("Add Limit", value=False, key="qe_limit")
    
    if use_limit:
        limit_n = st.number_input("Number of Rows", min_value=1, value=10, key="qe_limit_n")
        operations.append(('limit', limit_n))
    
    # Execute query
    st.markdown("---")
    if st.button("🚀 Execute Query", type="primary"):
        try:
            # Start query engine
            query = QueryEngine(table)
            
            # Apply operations
            for op in operations:
                if op[0] == 'filter':
                    _, col, operator, val = op
                    # Create condition
                    if operator == '>':
                        query.filter(lambda row: float(row.get(col, 0)) > float(val))
                    elif operator == '<':
                        query.filter(lambda row: float(row.get(col, 0)) < float(val))
                    elif operator == '==':
                        query.filter(lambda row: str(row.get(col, '')) == str(val))
                    elif operator == '!=':
                        query.filter(lambda row: str(row.get(col, '')) != str(val))
                
                elif op[0] == 'select':
                    query.select(op[1])
                
                elif op[0] == 'groupby':
                    _, group_cols, agg_col, agg_funcs = op
                    query.group_by(group_cols)
                    query.aggregate({agg_col: agg_funcs})
                
                elif op[0] == 'order':
                    query.order_by(op[1], ascending=op[2])
                
                elif op[0] == 'limit':
                    query.limit(op[1])
            
            # Execute
            result = query.execute()
            
            st.success(f"✅ Query executed successfully! Result has {result.row_count()} rows.")
            
            st.markdown("### Query Result")
            display_table(result, "Query Result")
            
            # Save option
            if st.button("💾 Save Query Result"):
                result_name = f"{table.name}_query_result"
                st.session_state.tables[result_name] = result
                st.success(f"Saved as '{result_name}'")
        
        except Exception as e:
            st.error(f"Error executing query: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
    
    # Show query summary
    if operations:
        st.markdown("---")
        st.markdown("### 📋 Query Summary")
        for i, op in enumerate(operations, 1):
            st.text(f"{i}. {op[0].upper()}: {op[1:]}")

def show_chunk_processing():
    """Chunk processing page"""
    st.markdown("## 📦 Chunk Processing for Large Files")
    
    st.info("""
    🔍 **What is Chunk Processing?**
    
    Chunk processing allows you to handle CSV files that are too large to fit in memory.
    The file is read in configurable chunks (e.g., 10,000 rows at a time), processed incrementally,
    and then aggregated.
    """)
    
    st.markdown("---")
    
    tab1, tab2 = st.tabs(["📤 Upload Large File", "🧪 Demo with Sample"])
    
    with tab1:
        uploaded_file = st.file_uploader("Upload Large CSV File", type=['csv'])
        
        col1, col2 = st.columns(2)
        with col1:
            chunk_size = st.number_input("Chunk Size (rows)", min_value=100, value=1000, step=100)
        with col2:
            has_header = st.checkbox("Has Header", value=True, key="chunk_header")
        
        if uploaded_file and st.button("Process in Chunks", type="primary"):
            try:
                # Save file
                temp_path = f"temp_{uploaded_file.name}"
                with open(temp_path, 'wb') as f:
                    f.write(uploaded_file.getbuffer())
                
                # Create parser and chunk reader
                parser = CSVParser()
                reader = ChunkReader(temp_path, chunk_size, parser)
                
                # Process chunks
                st.markdown("### Processing Chunks...")
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                all_chunks = []
                total_rows = 0
                
                for chunk in reader.read_chunks(has_header):
                    chunk_num = chunk['chunk_number']
                    rows_in_chunk = chunk['rows_in_chunk']
                    total_rows = chunk['total_rows_so_far']
                    
                    status_text.text(f"Processing chunk {chunk_num + 1}... ({rows_in_chunk} rows)")
                    all_chunks.append(chunk)
                    
                    if chunk['is_last']:
                        progress_bar.progress(1.0)
                        break
                
                st.success(f"✅ Processed {len(all_chunks)} chunks with {total_rows} total rows!")
                
                # Show chunk statistics
                st.markdown("### 📊 Chunk Statistics")
                chunk_stats = []
                for chunk in all_chunks:
                    chunk_stats.append({
                        'Chunk #': chunk['chunk_number'] + 1,
                        'Rows': chunk['rows_in_chunk'],
                        'Cumulative Rows': chunk['total_rows_so_far'],
                        'Is Last': '✓' if chunk['is_last'] else ''
                    })
                
                st.dataframe(chunk_stats, use_container_width=True)
                
                # Option to load first chunk as table
                if st.button("Load First Chunk as Table"):
                    first_chunk = all_chunks[0]
                    table = Table(first_chunk['headers'], first_chunk['data'], 'first_chunk')
                    st.session_state.tables['first_chunk'] = table
                    st.session_state.current_table = table
                    st.success("First chunk loaded!")
                    display_table(table, "First Chunk")
                
                # Clean up
                os.remove(temp_path)
                
            except Exception as e:
                st.error(f"Error processing chunks: {str(e)}")
    
    with tab2:
        st.markdown("### Demo: Create and Process Sample Large File")
        
        num_rows = st.number_input("Number of Sample Rows", min_value=100, max_value=100000, 
                                   value=5000, step=1000)
        
        if st.button("Generate and Process Sample File", type="primary"):
            try:
                # Generate sample CSV
                temp_file = "temp_sample_large.csv"
                
                with open(temp_file, 'w') as f:
                    # Write header
                    f.write("id,name,category,value,date\n")
                    
                    # Write rows
                    categories = ['A', 'B', 'C', 'D', 'E']
                    for i in range(num_rows):
                        f.write(f"{i+1},Item_{i+1},{categories[i % 5]},{(i+1)*10},2024-01-{(i % 28)+1:02d}\n")
                
                st.success(f"✅ Generated {num_rows} rows")
                
                # Process in chunks
                parser = CSVParser()
                reader = ChunkReader(temp_file, chunk_size=1000, parser=parser)
                
                st.markdown("### Processing...")
                chunks_processed = 0
                total_rows = 0
                
                for chunk in reader.read_chunks(has_header=True):
                    chunks_processed += 1
                    total_rows = chunk['total_rows_so_far']
                    
                    if chunk['is_last']:
                        break
                
                st.success(f"✅ Processed {chunks_processed} chunks with {total_rows} total rows!")
                
                # Show statistics
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Rows", f"{total_rows:,}")
                col2.metric("Chunks", chunks_processed)
                col3.metric("Avg Rows/Chunk", f"{total_rows // chunks_processed:,}")
                
                # Clean up
                os.remove(temp_file)
                
            except Exception as e:
                st.error(f"Error: {str(e)}")

def show_all_operations_demo():
    """Demo page showing all operations in sequence"""
    st.markdown("## 🧪 Complete Operations Demo")
    
    st.info("""
    This demo showcases all SQL operations in a real-world business scenario.
    We'll analyze e-commerce data step by step.
    """)
    
    # Load sample data if not loaded
    if not st.session_state.tables or 'products' not in st.session_state.tables:
        if st.button("Load Sample E-Commerce Data", type="primary"):
            st.session_state.tables = create_sample_data()
            st.rerun()
        return
    
    products = st.session_state.tables['products']
    sales = st.session_state.tables['sales']
    customers = st.session_state.tables['customers']
    
    # Demo workflow
    demo_step = st.selectbox(
        "Select Demo Step:",
        [
            "1️⃣ View Raw Data",
            "2️⃣ Filter: High-Value Products",
            "3️⃣ Projection: Essential Columns",
            "4️⃣ Join: Sales with Products",
            "5️⃣ Group By: Sales by Category",
            "6️⃣ Complex Query: Best Customers",
            "7️⃣ Complete Analysis Pipeline"
        ]
    )
    
    if demo_step == "1️⃣ View Raw Data":
        st.markdown("### 📊 Raw Data Tables")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("**Products**")
            display_table(products, "Products")
        with col2:
            st.markdown("**Sales**")
            display_table(sales, "Sales")
        with col3:
            st.markdown("**Customers**")
            display_table(customers, "Customers")
    
    elif demo_step == "2️⃣ Filter: High-Value Products":
        st.markdown("### 🔍 Filter Products > $100")
        
        result = Filter.apply(products, lambda row: float(row['price']) > 100)
        
        col1, col2 = st.columns([1, 2])
        with col1:
            st.metric("Products Found", result.row_count())
            st.metric("Out of Total", products.row_count())
        with col2:
            display_table(result, "High-Value Products")
        
        st.code("""
# Code:
result = Filter.apply(
    products, 
    lambda row: float(row['price']) > 100
)
        """)
    
    elif demo_step == "3️⃣ Projection: Essential Columns":
        st.markdown("### 📋 Select Product Name, Category, and Price")
        
        result = Projection.apply(products, ['name', 'category', 'price'])
        
        display_table(result, "Essential Product Info")
        
        st.code("""
# Code:
result = Projection.apply(
    products, 
    ['name', 'category', 'price']
)
        """)
    
    elif demo_step == "4️⃣ Join: Sales with Products":
        st.markdown("### 🔗 Join Sales with Product Details")
        
        result = Join.inner_join(sales, products, 'product_id', 'product_id')
        
        st.metric("Combined Records", result.row_count())
        display_table(result, "Sales with Product Details")
        
        st.code("""
# Code:
result = Join.inner_join(
    sales, products, 
    'product_id', 'product_id'
)
        """)
    
    elif demo_step == "5️⃣ Group By: Sales by Category":
        st.markdown("### 📊 Aggregate Sales by Product Category")
        
        # First join sales with products
        joined = Join.inner_join(sales, products, 'product_id', 'product_id')
        
        # Group by category
        groups = GroupBy.apply(joined, ['category'])
        
        # Aggregate
        results = Aggregation.apply_to_groups(
            groups, joined.headers,
            {'amount': ['sum', 'avg', 'count']}
        )
        
        st.markdown("**Results:**")
        st.json(results)
        
        # Visualization
        st.markdown("**Visualization:**")
        chart_data = {r['group_key'][0]: r['amount_sum'] for r in results}
        st.bar_chart(chart_data)
        
        st.code("""
# Code:
joined = Join.inner_join(sales, products, 'product_id', 'product_id')
groups = GroupBy.apply(joined, ['category'])
results = Aggregation.apply_to_groups(
    groups, joined.headers,
    {'amount': ['sum', 'avg', 'count']}
)
        """)
    
    elif demo_step == "6️⃣ Complex Query: Best Customers":
        st.markdown("### ⭐ Find Premium Customers with High Spending")
        
        # Join sales with customers
        sales_customers = Join.inner_join(sales, customers, 'customer_id', 'customer_id')
        
        # Filter for premium customers
        premium_sales = Filter.apply(
            sales_customers,
            lambda row: row.get('segment') == 'Premium'
        )
        
        # Group by customer and calculate total spending
        groups = GroupBy.apply(premium_sales, ['name'])
        results = Aggregation.apply_to_groups(
            groups, premium_sales.headers,
            {'amount': ['sum', 'count']}
        )
        
        st.markdown("**Premium Customer Spending:**")
        st.json(results)
        
        st.code("""
# Code:
sales_customers = Join.inner_join(
    sales, customers, 
    'customer_id', 'customer_id'
)

premium_sales = Filter.apply(
    sales_customers,
    lambda row: row.get('segment') == 'Premium'
)

groups = GroupBy.apply(premium_sales, ['name'])
results = Aggregation.apply_to_groups(
    groups, premium_sales.headers,
    {'amount': ['sum', 'count']}
)
        """)
    
    elif demo_step == "7️⃣ Complete Analysis Pipeline":
        st.markdown("### 🚀 End-to-End Query Pipeline")
        
        st.markdown("""
        **Business Question:** 
        *"What are the top-selling product categories for premium customers, 
        and what's the average order value?"*
        """)
        
        with st.spinner("Running complete analysis..."):
            # Step 1: Join sales with products
            st.markdown("**Step 1:** Join sales with products")
            sales_products = Join.inner_join(sales, products, 'product_id', 'product_id')
            st.text(f"✓ Created {sales_products.row_count()} combined records")
            
            # Step 2: Join with customers
            st.markdown("**Step 2:** Join with customers")
            full_data = Join.inner_join(sales_products, customers, 'customer_id', 'customer_id')
            st.text(f"✓ Created {full_data.row_count()} complete records")
            
            # Step 3: Filter for premium customers
            st.markdown("**Step 3:** Filter for premium segment")
            premium_data = Filter.apply(
                full_data,
                lambda row: row.get('segment') == 'Premium'
            )
            st.text(f"✓ Found {premium_data.row_count()} premium customer transactions")
            
            # Step 4: Group by category
            st.markdown("**Step 4:** Group by category and aggregate")
            groups = GroupBy.apply(premium_data, ['category'])
            results = Aggregation.apply_to_groups(
                groups, premium_data.headers,
                {'amount': ['sum', 'avg', 'count']}
            )
            
            st.markdown("### 📊 Final Results")
            st.json(results)
            
            # Create visualization
            st.markdown("### 📈 Visualization")
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Total Sales by Category**")
                chart_data = {r['group_key'][0]: r['amount_sum'] for r in results}
                st.bar_chart(chart_data)
            
            with col2:
                st.markdown("**Average Order Value**")
                chart_data = {r['group_key'][0]: r['amount_avg'] for r in results}
                st.bar_chart(chart_data)
            
            st.success("✅ Complete analysis finished!")
            
            # Show complete code
            with st.expander("📝 View Complete Code"):
                st.code("""
# Complete Analysis Pipeline

# Step 1: Join sales with products
sales_products = Join.inner_join(
    sales, products, 
    'product_id', 'product_id'
)

# Step 2: Join with customers
full_data = Join.inner_join(
    sales_products, customers, 
    'customer_id', 'customer_id'
)

# Step 3: Filter for premium customers
premium_data = Filter.apply(
    full_data,
    lambda row: row.get('segment') == 'Premium'
)

# Step 4: Group and aggregate
groups = GroupBy.apply(premium_data, ['category'])
results = Aggregation.apply_to_groups(
    groups, premium_data.headers,
    {'amount': ['sum', 'avg', 'count']}
)

# Results contain sales analysis for premium customers by category
                """)

if __name__ == "__main__":
    main()