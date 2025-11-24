#!/usr/bin/env python3
"""
Final Update Script - SQL + NoSQL System with JOIN support
Simple, clean structure for easy professor explanation

Authors: Lance Dsilva, Chelroy Limas, Rafayel Mirijanyan
Course: DSCI 551 - Fall 2025
"""

import os
import shutil
from datetime import datetime
from pathlib import Path

class FinalProjectUpdater:
    def __init__(self):
        self.project_root = Path.cwd()
        self.backup_dir = self.project_root / "backups_final"
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
    def create_backup_dir(self):
        if not self.backup_dir.exists():
            self.backup_dir.mkdir(parents=True)
            print(f"Created backup directory: {self.backup_dir}")
    
    def backup_file(self, filepath):
        file_path = Path(filepath)
        if file_path.exists():
            backup_name = f"{file_path.name}.backup_{self.timestamp}"
            backup_path = self.backup_dir / backup_name
            shutil.copy2(file_path, backup_path)
            print(f"Backed up: {filepath}")
            return True
        return False
    
    # ============= STREAMLIT CONFIG =============
    def get_streamlit_config(self):
        return '''[server]
maxUploadSize = 500
maxMessageSize = 500
enableCORS = false

[browser]
gatherUsageStats = false
'''

    # ============= MAIN APP - WITH FIXES =============
    def get_app_content(self):
        return '''import streamlit as st
from sql_engine import SQLEngine
from nosql_engine import NoSQLEngine

st.set_page_config(page_title="Data Processing System", layout="wide")

st.title("E-Commerce Intelligence System")
st.write("DSCI 551 Fall 2025 - SQL and NoSQL Data Processing")

# Initialize engines
if 'sql_engine' not in st.session_state:
    st.session_state.sql_engine = SQLEngine()
if 'nosql_engine' not in st.session_state:
    st.session_state.nosql_engine = NoSQLEngine()

# Create tabs
tab1, tab2 = st.tabs(["SQL Engine (CSV)", "NoSQL Engine (JSON)"])

# ============= SQL TAB =============
with tab1:
    st.header("SQL Engine - CSV Processing")
    
    # Step 1: Load Main Table
    st.subheader("Step 1: Load Main Table")
    col1, col2 = st.columns([1, 1])
    
    with col1:
        uploaded_file = st.file_uploader("Upload CSV file", type=['csv'], key="sql_upload")
        use_chunking = st.checkbox("Enable chunking for large files", value=False)
        if use_chunking:
            chunk_size = st.number_input("Chunk size (rows)", min_value=1000, value=10000, step=1000)
        else:
            chunk_size = None
        
        if st.button("Load Main Table", type="primary"):
            if uploaded_file:
                with st.spinner("Loading..."):
                    success, message = st.session_state.sql_engine.load_data(uploaded_file, chunk_size)
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
    
    with col2:
        if st.session_state.sql_engine.is_loaded():
            info = st.session_state.sql_engine.get_info()
            st.write(f"File: {info['filename']}")
            st.write(f"Rows: {info['rows']:,}")
            st.write(f"Columns: {info['columns']}")
            st.write(f"Mode: {info['mode']}")
    
    # Show preview if loaded
    if st.session_state.sql_engine.is_loaded():
        with st.expander("View Main Table Preview", expanded=False):
            st.dataframe(st.session_state.sql_engine.preview(10), use_container_width=True)
    
    st.divider()
    
    # Step 2: Build Query
    if st.session_state.sql_engine.is_loaded():
        st.subheader("Step 2: Build Query (SQL Order)")
        
        # Get all available columns (main + join if exists)
        columns = st.session_state.sql_engine.get_all_columns()
        main_columns = st.session_state.sql_engine.get_columns()
        
        # 1. SELECT
        with st.expander("1. SELECT (Choose Columns)", expanded=False):
            st.caption("SELECT column1, column2, ...")
            select_cols = st.multiselect("Select columns to display", columns, key="sql_select")
            if st.button("Add SELECT", key="add_select"):
                if select_cols:
                    st.session_state.sql_engine.add_select(select_cols)
                    st.success("SELECT added")
                else:
                    st.warning("Please select at least one column")
        
        # 2. JOIN
        with st.expander("2. JOIN (Combine with Another Table)", expanded=False):
            st.caption("JOIN table2 ON table1.key = table2.key")
            
            if st.session_state.sql_engine.has_join_table():
                join_info = st.session_state.sql_engine.get_join_table_info()
                st.success(f"Join table loaded: {join_info['filename']} ({join_info['rows']} rows)")
                
                # Show join table preview (INSIDE the JOIN expander)
                st.write("Preview Join Table:")
                join_preview = st.session_state.sql_engine.preview_join_table(10)
                st.dataframe(join_preview, use_container_width=True)
                
                st.divider()
                
                # Join configuration - ALWAYS visible when join table loaded
                st.write("**Configure Join:**")
                
                col1, col2 = st.columns(2)
                with col1:
                    join_type = st.selectbox(
                        "Join Type", 
                        ["INNER", "LEFT", "RIGHT"], 
                        key="join_type_select",
                        help="Select the type of join"
                    )
                with col2:
                    # Placeholder to balance columns
                    st.write("")
                
                col1, col2 = st.columns(2)
                with col1:
                    main_key = st.selectbox(
                        "Main table key", 
                        main_columns, 
                        key="main_join_key",
                        help="Column from main table"
                    )
                with col2:
                    join_key = st.selectbox(
                        "Join table key", 
                        join_info['columns'], 
                        key="join_join_key",
                        help="Column from join table"
                    )
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Add JOIN", key="add_join", type="primary", use_container_width=True):
                        st.session_state.sql_engine.add_join(join_type, main_key, join_key)
                        st.success(f"✓ {join_type} JOIN added on {main_key} = {join_key}")
                
                with col2:
                    if st.button("Remove Join Table", key="remove_join_table", use_container_width=True):
                        st.session_state.sql_engine.clear_join_table()
                        st.info("Join table removed")
                
                # Show join preview if JOIN operation exists
                join_ops = [op for op in st.session_state.sql_engine.get_operations() if op['type'] == 'join']
                if join_ops:
                    st.divider()
                    st.write("**Preview Join Result (First 10 rows):**")
                    join_preview_result = st.session_state.sql_engine.preview_join_result(10)
                    if join_preview_result:
                        st.dataframe(join_preview_result, use_container_width=True)
                    else:
                        st.info("Add join keys and click 'Add JOIN' to see preview")
                
            else:
                st.info("📤 Upload a second CSV file to perform JOIN operations")
                join_file = st.file_uploader("Upload second CSV for JOIN", type=['csv'], key="join_upload")
                
                if join_file:
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        if st.button("Load Join Table", key="load_join", type="primary"):
                            with st.spinner("Loading join table..."):
                                success, message = st.session_state.sql_engine.load_join_table(join_file)
                                if success:
                                    st.success(message)
                                    st.balloons()  # Visual feedback
                                    st.rerun()  # Force refresh to show join options immediately
                                else:
                                    st.error(message)
        
        # 3. WHERE
        with st.expander("3. WHERE (Filter Rows)", expanded=False):
            st.caption("WHERE column > value")
            col1, col2, col3 = st.columns(3)
            with col1:
                filter_col = st.selectbox("Column", main_columns, key="filter_col")
            with col2:
                filter_op = st.selectbox("Operator", [">", "<", ">=", "<=", "==", "!="], key="filter_op")
            with col3:
                filter_val = st.text_input("Value", key="filter_val")
            
            if st.button("Add WHERE", key="add_filter"):
                if filter_val:
                    st.session_state.sql_engine.add_filter(filter_col, filter_op, filter_val)
                    st.success("WHERE condition added")
                else:
                    st.warning("Please enter a value")
        
        # 4. GROUP BY
        with st.expander("4. GROUP BY (Aggregate Data)", expanded=False):
            st.caption("GROUP BY column")
            col1, col2 = st.columns(2)
            with col1:
                group_col = st.selectbox("Group by column", main_columns, key="group_col")
            with col2:
                include_agg = st.checkbox("Add aggregate function", key="include_agg")
            
            if include_agg:
                col1, col2 = st.columns(2)
                with col1:
                    agg_func = st.selectbox("Function", ["COUNT", "SUM", "AVG", "MIN", "MAX"], key="agg_func")
                with col2:
                    agg_col = st.selectbox("Column", main_columns, key="agg_col")
                
                if st.button("Add GROUP BY + Aggregate", key="add_groupby_agg"):
                    st.session_state.sql_engine.add_groupby(group_col, agg_func, agg_col)
                    st.success("GROUP BY with aggregation added")
            else:
                if st.button("Add GROUP BY", key="add_groupby"):
                    st.session_state.sql_engine.add_groupby(group_col)
                    st.success("GROUP BY added")
        
        # 5. HAVING
        with st.expander("5. HAVING (Filter Groups)", expanded=False):
            st.caption("HAVING aggregate_function > value")
            col1, col2, col3 = st.columns(3)
            with col1:
                having_func = st.selectbox("Function", ["COUNT", "SUM", "AVG", "MIN", "MAX"], key="having_func")
            with col2:
                having_op = st.selectbox("Operator", [">", "<", ">=", "<=", "=="], key="having_op")
            with col3:
                having_val = st.text_input("Value", key="having_val")
            
            if st.button("Add HAVING", key="add_having"):
                if having_val:
                    st.session_state.sql_engine.add_having(having_func, having_op, having_val)
                    st.success("HAVING condition added")
                else:
                    st.warning("Please enter a value")
        
        # 6. ORDER BY
        with st.expander("6. ORDER BY (Sort Results)", expanded=False):
            st.caption("ORDER BY column ASC/DESC")
            col1, col2 = st.columns(2)
            with col1:
                order_col = st.selectbox("Sort by column", main_columns, key="order_col")
            with col2:
                order_dir = st.selectbox("Direction", ["ASC", "DESC"], key="order_dir")
            
            if st.button("Add ORDER BY", key="add_order"):
                st.session_state.sql_engine.add_orderby(order_col, order_dir)
                st.success("ORDER BY added")
        
        # 7. LIMIT
        with st.expander("7. LIMIT (Limit Results)", expanded=False):
            st.caption("LIMIT n")
            limit_val = st.number_input("Number of rows", min_value=1, value=100, key="limit_val")
            if st.button("Add LIMIT", key="add_limit"):
                st.session_state.sql_engine.add_limit(limit_val)
                st.success("LIMIT added")
        
        st.divider()
        
        # Show Current Query
        operations = st.session_state.sql_engine.get_operations()
        if operations:
            st.subheader("Step 3: Current Query")
            
            # Show as SQL-like text
            query_text = st.session_state.sql_engine.get_query_text()
            st.code(query_text, language="sql")
            
            # Show operations list
            st.write("Operations:")
            for i, op in enumerate(operations):
                col1, col2 = st.columns([5, 1])
                with col1:
                    st.text(f"{i+1}. {op['description']}")
                with col2:
                    if st.button("X", key=f"remove_{i}"):
                        st.session_state.sql_engine.remove_operation(i)
                        st.success("Operation removed")
            
            # Execute buttons
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Execute Query", type="primary", use_container_width=True):
                    with st.spinner("Executing..."):
                        result = st.session_state.sql_engine.execute()
                        if result is not None:
                            st.success(f"Returned {len(result) if isinstance(result, list) else 1} rows")
                            st.dataframe(result, use_container_width=True)
                        else:
                            st.error("Query failed")
            with col2:
                if st.button("Clear All", use_container_width=True):
                    st.session_state.sql_engine.clear_operations()
                    st.success("All operations cleared")

# ============= NOSQL TAB =============
with tab2:
    st.header("NoSQL Engine - JSON Processing")
    
    st.subheader("Step 1: Load Main Collection")
    col1, col2 = st.columns([1, 1])
    
    with col1:
        uploaded_json = st.file_uploader("Upload JSON file", type=['json'], key="nosql_upload")
        if st.button("Load JSON", type="primary"):
            if uploaded_json:
                with st.spinner("Loading..."):
                    success, message = st.session_state.nosql_engine.load_data(uploaded_json)
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
    
    with col2:
        if st.session_state.nosql_engine.is_loaded():
            info = st.session_state.nosql_engine.get_info()
            st.write(f"File: {info['filename']}")
            st.write(f"Documents: {info['count']:,}")
            st.write(f"Type: {info['type']}")
    
    if st.session_state.nosql_engine.is_loaded():
        with st.expander("View Main Collection Preview", expanded=False):
            st.json(st.session_state.nosql_engine.preview(3))
        
        st.divider()
        
        st.subheader("Step 2: Build Query")
        fields = st.session_state.nosql_engine.get_fields()
        
        # 1. Filter
        with st.expander("1. Filter Documents", expanded=False):
            col1, col2, col3 = st.columns(3)
            with col1:
                filter_field = st.selectbox("Field", fields, key="nosql_filter_field")
            with col2:
                filter_op = st.selectbox("Operator", ["==", "!=", ">", "<", "contains"], key="nosql_filter_op")
            with col3:
                filter_val = st.text_input("Value", key="nosql_filter_val")
            
            if st.button("Add Filter", key="nosql_add_filter"):
                if filter_val:
                    st.session_state.nosql_engine.add_filter(filter_field, filter_op, filter_val)
                    st.success("Filter added")
                else:
                    st.warning("Please enter a value")
        
        # 2. Join (Lookup)
        with st.expander("2. Join (Lookup Another Collection)", expanded=False):
            st.caption("Join/Lookup with another JSON collection")
            
            if st.session_state.nosql_engine.has_join_collection():
                join_info = st.session_state.nosql_engine.get_join_collection_info()
                st.success(f"Join collection loaded: {join_info['filename']} ({join_info['count']} documents)")
                
                # Show join collection preview (INSIDE the JOIN expander)
                st.write("**Preview Join Collection:**")
                join_preview = st.session_state.nosql_engine.preview_join_collection(3)
                st.json(join_preview)
                
                st.divider()
                
                # Join configuration - ALWAYS visible
                st.write("**Configure Join:**")
                
                join_fields = st.session_state.nosql_engine.get_join_fields()
                
                col1, col2 = st.columns(2)
                with col1:
                    main_field = st.selectbox(
                        "Main collection field", 
                        fields, 
                        key="nosql_main_field",
                        help="Field from main collection"
                    )
                with col2:
                    join_field = st.selectbox(
                        "Join collection field", 
                        join_fields, 
                        key="nosql_join_field",
                        help="Field from join collection"
                    )
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Add Join (Lookup)", key="nosql_add_join", type="primary", use_container_width=True):
                        st.session_state.nosql_engine.add_join(main_field, join_field)
                        st.success(f"✓ Join added on {main_field} = {join_field}")
                
                with col2:
                    if st.button("Remove Join Collection", key="nosql_remove_join", use_container_width=True):
                        st.session_state.nosql_engine.clear_join_collection()
                        st.info("Join collection removed")
                
                # Show join preview
                join_ops = [op for op in st.session_state.nosql_engine.get_operations() if op['type'] == 'join']
                if join_ops:
                    st.divider()
                    st.write("**Preview Join Result (First 3 documents):**")
                    join_result = st.session_state.nosql_engine.preview_join_result(3)
                    if join_result:
                        st.json(join_result)
                    else:
                        st.info("Add join fields and click 'Add Join' to see preview")
                
            else:
                st.info("📤 Upload a second JSON file to perform JOIN/Lookup operations")
                join_json = st.file_uploader("Upload second JSON for Join/Lookup", type=['json'], key="nosql_join_upload")
                
                if join_json:
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        if st.button("Load Join Collection", key="nosql_load_join", type="primary"):
                            with st.spinner("Loading join collection..."):
                                success, message = st.session_state.nosql_engine.load_join_collection(join_json)
                                if success:
                                    st.success(message)
                                    st.balloons()  # Visual feedback
                                    st.rerun()  # Force refresh to show join options
                                else:
                                    st.error(message)
        
        # 3. Project
        with st.expander("3. Project Fields", expanded=False):
            project_fields = st.multiselect("Select fields", fields, key="nosql_project")
            if st.button("Add Projection", key="nosql_add_project"):
                if project_fields:
                    st.session_state.nosql_engine.add_projection(project_fields)
                    st.success("Projection added")
                else:
                    st.warning("Please select at least one field")
        
        # 4. Group By
        with st.expander("4. Group By", expanded=False):
            group_field = st.selectbox("Group by field", fields, key="nosql_group")
            if st.button("Add Group By", key="nosql_add_group"):
                st.session_state.nosql_engine.add_groupby(group_field)
                st.success("Group By added")
        
        # 5. Limit
        with st.expander("5. Limit", expanded=False):
            limit_val = st.number_input("Number of documents", min_value=1, value=100, key="nosql_limit")
            if st.button("Add Limit", key="nosql_add_limit"):
                st.session_state.nosql_engine.add_limit(limit_val)
                st.success("Limit added")
        
        # Show operations
        operations = st.session_state.nosql_engine.get_operations()
        if operations:
            st.subheader("Step 3: Current Query")
            st.write("Operations:")
            for i, op in enumerate(operations):
                col1, col2 = st.columns([5, 1])
                with col1:
                    st.text(f"{i+1}. {op['description']}")
                with col2:
                    if st.button("X", key=f"nosql_remove_{i}"):
                        st.session_state.nosql_engine.remove_operation(i)
                        st.success("Operation removed")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Execute Query", type="primary", key="nosql_exec", use_container_width=True):
                    with st.spinner("Executing..."):
                        result = st.session_state.nosql_engine.execute()
                        if result is not None:
                            st.success(f"Returned {len(result)} documents")
                            st.json(result[:10])
                        else:
                            st.error("Query failed")
            with col2:
                if st.button("Clear All", key="nosql_clear", use_container_width=True):
                    st.session_state.nosql_engine.clear_operations()
                    st.success("All operations cleared")

st.divider()
st.caption("DSCI 551 Fall 2025 | Lance Dsilva, Chelroy Limas, Rafayel Mirijanyan")
'''

    # ============= SQL ENGINE - WITH PREVIEW AND HAVING =============
    def get_sql_engine_content(self):
        return '''"""
SQL Engine - Simple and Easy to Explain

Structure:
1. Load data (main table)
2. Optionally load join table
3. Build query operations
4. Execute query
"""

from csv_parser import CSVParser
from query_executor import QueryExecutor
import tempfile

class SQLEngine:
    def __init__(self):
        # Main table
        self.data = None
        self.headers = None
        self.filename = None
        self.chunked = False
        self.chunk_size = None
        self.file_path = None
        
        # Join table
        self.join_data = None
        self.join_headers = None
        self.join_filename = None
        
        # Operations
        self.operations = []
    
    def load_data(self, file_obj, chunk_size=None):
        """Load main CSV file"""
        try:
            parser = CSVParser()
            
            if chunk_size:
                # Save to temp file for chunking
                with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as tmp:
                    tmp.write(file_obj.getvalue())
                    self.file_path = tmp.name
                
                self.headers, estimated_rows = parser.get_file_info(self.file_path)
                self.chunked = True
                self.chunk_size = chunk_size
                self.data = None
            else:
                result = parser.parse_file(file_obj)
                self.headers = result['headers']
                self.data = result['data']
                self.chunked = False
            
            self.filename = file_obj.name
            self.operations = []
            return True, f"Loaded {self.filename}"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def load_join_table(self, file_obj):
        """Load second table for JOIN operation"""
        try:
            parser = CSVParser()
            result = parser.parse_file(file_obj)
            
            self.join_data = result['data']
            self.join_headers = result['headers']
            self.join_filename = file_obj.name
            
            return True, f"Loaded join table: {self.join_filename}"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def has_join_table(self):
        """Check if join table is loaded"""
        return self.join_data is not None
    
    def get_join_table_info(self):
        """Get join table information"""
        return {
            'filename': self.join_filename,
            'columns': self.join_headers,
            'rows': len(self.join_data)
        }
    
    def preview_join_table(self, n=10):
        """Preview join table data"""
        return self.join_data[:n] if self.join_data else []
    
    def preview_join_result(self, n=10):
        """Preview what the join will look like"""
        if not self.join_data:
            return None
        
        # Find join operation
        join_ops = [op for op in self.operations if op['type'] == 'join']
        if not join_ops:
            return None
        
        join_op = join_ops[0]
        
        # Get preview data
        if self.chunked:
            parser = CSVParser()
            for chunk in parser.parse_file_in_chunks(self.file_path, self.chunk_size):
                preview_data = chunk[:n]
                break
        else:
            preview_data = self.data[:n]
        
        # Perform join on preview
        executor = QueryExecutor(
            preview_data, self.headers, None,
            False, None,
            self.join_data, self.join_headers
        )
        
        return executor._apply_join(preview_data, join_op)
    
    def clear_join_table(self):
        """Remove join table"""
        self.join_data = None
        self.join_headers = None
        self.join_filename = None
        # Remove join operations
        self.operations = [op for op in self.operations if op['type'] != 'join']
    
    def is_loaded(self):
        """Check if main table is loaded"""
        return self.headers is not None
    
    def get_info(self):
        """Get main table information"""
        if self.chunked:
            import os
            file_size = os.path.getsize(self.file_path)
            estimated_rows = file_size // 100
            mode = f"Chunked ({self.chunk_size:,} rows/batch)"
        else:
            estimated_rows = len(self.data)
            mode = "Normal (all in memory)"
        
        return {
            'filename': self.filename,
            'rows': estimated_rows,
            'columns': len(self.headers),
            'mode': mode
        }
    
    def get_columns(self):
        """Get column names from main table"""
        return self.headers if self.headers else []
    
    def get_all_columns(self):
        """Get all columns (main + join tables)"""
        all_cols = self.headers[:] if self.headers else []
        if self.join_headers:
            # Add join table columns with prefix to avoid confusion
            all_cols.extend([f"{self.join_filename.split('.')[0]}.{col}" for col in self.join_headers])
        return all_cols
    
    def preview(self, n=10):
        """Preview data"""
        if self.chunked:
            parser = CSVParser()
            for chunk in parser.parse_file_in_chunks(self.file_path, self.chunk_size):
                return chunk[:n]
        else:
            return self.data[:n] if self.data else []
    
    def add_select(self, columns):
        """Add SELECT operation"""
        self.operations.append({
            'type': 'select',
            'columns': columns,
            'description': f"SELECT {', '.join(columns)}"
        })
    
    def add_join(self, join_type, main_key, join_key):
        """Add JOIN operation"""
        self.operations.append({
            'type': 'join',
            'join_type': join_type,
            'main_key': main_key,
            'join_key': join_key,
            'description': f"{join_type} JOIN ON {main_key} = {join_key}"
        })
    
    def add_filter(self, column, operator, value):
        """Add WHERE filter"""
        self.operations.append({
            'type': 'filter',
            'column': column,
            'operator': operator,
            'value': value,
            'description': f"WHERE {column} {operator} {value}"
        })
    
    def add_groupby(self, column, agg_func=None, agg_column=None):
        """Add GROUP BY operation"""
        desc = f"GROUP BY {column}"
        if agg_func:
            desc += f", {agg_func}({agg_column})"
        
        self.operations.append({
            'type': 'groupby',
            'column': column,
            'agg_func': agg_func,
            'agg_column': agg_column,
            'description': desc
        })
    
    def add_having(self, function, operator, value):
        """Add HAVING operation"""
        self.operations.append({
            'type': 'having',
            'function': function,
            'operator': operator,
            'value': value,
            'description': f"HAVING {function} {operator} {value}"
        })
    
    def add_having(self, function, operator, value):
        """Add HAVING operation"""
        self.operations.append({
            'type': 'having',
            'function': function,
            'operator': operator,
            'value': value,
            'description': f"HAVING {function} {operator} {value}"
        })
    
    def add_orderby(self, column, direction):
        """Add ORDER BY operation"""
        self.operations.append({
            'type': 'orderby',
            'column': column,
            'direction': direction,
            'description': f"ORDER BY {column} {direction}"
        })
    
    def add_limit(self, n):
        """Add LIMIT operation"""
        self.operations.append({
            'type': 'limit',
            'value': n,
            'description': f"LIMIT {n}"
        })
    
    def get_operations(self):
        """Get all operations"""
        return self.operations
    
    def remove_operation(self, index):
        """Remove operation at index"""
        if 0 <= index < len(self.operations):
            self.operations.pop(index)
    
    def clear_operations(self):
        """Clear all operations"""
        self.operations = []
    
    def get_query_text(self):
        """Generate SQL-like query text"""
        query_parts = []
        
        # SELECT
        select_ops = [op for op in self.operations if op['type'] == 'select']
        if select_ops:
            cols = select_ops[0]['columns']
            query_parts.append(f"SELECT {', '.join(cols)}")
        else:
            query_parts.append("SELECT *")
        
        # FROM
        query_parts.append(f"FROM {self.filename}")
        
        # JOIN
        join_ops = [op for op in self.operations if op['type'] == 'join']
        if join_ops:
            for jop in join_ops:
                query_parts.append(f"{jop['join_type']} JOIN {self.join_filename}")
                query_parts.append(f"  ON {jop['main_key']} = {jop['join_key']}")
        
        # WHERE
        filter_ops = [op for op in self.operations if op['type'] == 'filter']
        if filter_ops:
            conditions = [f"{op['column']} {op['operator']} {op['value']}" for op in filter_ops]
            query_parts.append(f"WHERE {' AND '.join(conditions)}")
        
        # GROUP BY
        group_ops = [op for op in self.operations if op['type'] == 'groupby']
        if group_ops:
            for gop in group_ops:
                query_parts.append(f"GROUP BY {gop['column']}")
                if gop.get('agg_func'):
                    query_parts.append(f"  {gop['agg_func']}({gop['agg_column']})")
        
        # HAVING
        having_ops = [op for op in self.operations if op['type'] == 'having']
        if having_ops:
            conditions = [f"{op['function']} {op['operator']} {op['value']}" for op in having_ops]
            query_parts.append(f"HAVING {' AND '.join(conditions)}")
        
        # ORDER BY
        order_ops = [op for op in self.operations if op['type'] == 'orderby']
        if order_ops:
            orders = [f"{op['column']} {op['direction']}" for op in order_ops]
            query_parts.append(f"ORDER BY {', '.join(orders)}")
        
        # LIMIT
        limit_ops = [op for op in self.operations if op['type'] == 'limit']
        if limit_ops:
            query_parts.append(f"LIMIT {limit_ops[0]['value']}")
        
        return "\\n".join(query_parts)
    
    def execute(self):
        """Execute query"""
        try:
            executor = QueryExecutor(
                self.data, self.headers, self.file_path,
                self.chunked, self.chunk_size,
                self.join_data, self.join_headers
            )
            return executor.execute(self.operations)
        except Exception as e:
            print(f"Execution error: {e}")
            import traceback
            traceback.print_exc()
            return None
'''

    # ============= QUERY EXECUTOR - WITH JOIN =============
    def get_query_executor_content(self):
        return '''"""
Query Executor - Executes SQL-like operations

Simple logic:
1. Start with data
2. Apply each operation in order
3. Return result
"""

from csv_parser import CSVParser

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
        """Execute with chunking (simplified - loads all for join/order)"""
        # For simplicity, if JOIN or ORDER BY present, load all data
        has_expensive = any(op['type'] in ['join', 'orderby'] for op in operations)
        
        if has_expensive:
            # Load all data first
            parser = CSVParser()
            all_data = []
            for chunk in parser.parse_file_in_chunks(self.file_path, self.chunk_size):
                all_data.extend(chunk)
            return self._execute_normal(operations)
        
        # Otherwise process in chunks
        parser = CSVParser()
        results = []
        limit_val = None
        
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
                return results[:limit_val]
        
        # Apply groupby if present
        for op in operations:
            if op['type'] == 'groupby':
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
        # HAVING only makes sense after GROUP BY
        # Data should be grouped result like [{'category': 'A', 'count': 10}, ...]
        
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
'''

    # ============= CSV PARSER (SAME) =============
    def get_csv_parser_content(self):
        return '''"""
CSV Parser - No external libraries

Simple structure:
1. Read file line by line
2. Handle quotes and delimiters
3. Convert to list of dicts
"""

class CSVParser:
    def __init__(self, delimiter=',', quote_char='"'):
        self.delimiter = delimiter
        self.quote_char = quote_char
    
    def parse_file(self, file_obj):
        """Parse entire CSV file"""
        content = file_obj.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8')
        lines = content.strip().split('\\n')
        
        if not lines:
            return {'headers': [], 'data': []}
        
        headers = self._parse_line(lines[0])
        data = []
        
        for line in lines[1:]:
            if line.strip():
                row_data = self._parse_line(line)
                row_dict = {headers[i]: row_data[i] if i < len(row_data) else None 
                           for i in range(len(headers))}
                data.append(row_dict)
        
        return {'headers': headers, 'data': data}
    
    def get_file_info(self, filepath):
        """Get headers without loading full file"""
        with open(filepath, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            headers = self._parse_line(first_line)
            
            import os
            file_size = os.path.getsize(filepath)
            estimated_rows = file_size // 100
        
        return headers, estimated_rows
    
    def parse_file_in_chunks(self, filepath, chunk_size=10000):
        """Parse file in chunks (generator)"""
        with open(filepath, 'r', encoding='utf-8', buffering=8192*1024) as f:
            header_line = f.readline()
            headers = self._parse_line(header_line)
            
            chunk = []
            for line in f:
                if not line.strip():
                    continue
                
                try:
                    row_data = self._parse_line(line)
                    row_dict = {headers[i]: row_data[i] if i < len(row_data) else None 
                               for i in range(len(headers))}
                    chunk.append(row_dict)
                    
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                except:
                    continue
            
            if chunk:
                yield chunk
    
    def _parse_line(self, line):
        """Parse single CSV line"""
        line = line.strip()
        values = []
        current_value = ""
        in_quotes = False
        
        i = 0
        while i < len(line):
            char = line[i]
            
            if char == self.quote_char:
                if in_quotes and i + 1 < len(line) and line[i + 1] == self.quote_char:
                    current_value += self.quote_char
                    i += 1
                else:
                    in_quotes = not in_quotes
            elif char == self.delimiter and not in_quotes:
                values.append(current_value.strip())
                current_value = ""
            else:
                current_value += char
            
            i += 1
        
        values.append(current_value.strip())
        return values
'''

    # ============= NOSQL ENGINE - WITH CUSTOM JSON PARSER =============
    def get_nosql_engine_content(self):
        return '''"""
NoSQL Engine - JSON processing with custom parser (no json library)
"""

from json_parser import JSONParser

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
'''

    # ============= NOSQL EXECUTOR - WITH JOIN =============
    def get_nosql_executor_content(self):
        return '''"""
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
'''

    # ============= README =============
    def get_readme_content(self):
        return '''# E-Commerce Intelligence System

DSCI 551 Fall 2025
Authors: Lance Dsilva, Chelroy Limas, Rafayel Mirijanyan

## Project Structure (Simple & Easy to Explain)

```
project/
├── app.py                  # Main Streamlit application
├── sql_engine.py           # SQL engine (handles CSV)
├── nosql_engine.py         # NoSQL engine (handles JSON)
├── csv_parser.py           # Custom CSV parser (no pandas)
├── query_executor.py       # Executes SQL operations
├── nosql_executor.py       # Executes NoSQL operations
└── .streamlit/
    └── config.toml         # Configuration (500MB upload limit)
```

## How It Works

### SQL Engine (CSV Processing)
1. **Load Data**: Upload CSV file (normal or chunked mode)
2. **Build Query**: Add operations in SQL order
   - SELECT (choose columns)
   - JOIN (combine tables)
   - WHERE (filter rows)
   - GROUP BY (aggregate)
   - ORDER BY (sort)
   - LIMIT (limit results)
3. **Execute**: Run query and see results

### NoSQL Engine (JSON Processing)
1. **Load Data**: Upload JSON file
2. **Build Query**: Add operations
   - Filter (find documents)
   - Project (select fields)
   - Group By (aggregate)
   - Limit (limit results)
3. **Execute**: Run query and see results

## Features

- Custom parsers (no pandas, no csv library)
- Chunking support for large files (up to 500MB)
- JOIN operations with multiple tables
- SQL-like query building interface
- NoSQL document operations

## Installation

```bash
pip install streamlit
```

## Usage

```bash
streamlit run app.py
```

## Code Explanation (For Professors)

### Architecture
```
User Interface (app.py)
    ↓
Engine Layer (sql_engine.py, nosql_engine.py)
    ↓
Parser Layer (csv_parser.py)
    ↓
Executor Layer (query_executor.py, nosql_executor.py)
```

### Key Components

1. **app.py**: User interface with Streamlit
2. **sql_engine.py**: Manages SQL operations and state
3. **csv_parser.py**: Parses CSV without external libraries
4. **query_executor.py**: Executes operations on data
5. **nosql_engine.py**: Manages NoSQL operations
6. **nosql_executor.py**: Executes NoSQL operations

### Why This Design?

- **Simple**: Each file has one clear purpose
- **Maintainable**: Easy to modify individual components
- **Explainable**: Clear flow of data through layers
- **Testable**: Each component can be tested separately

## Demo Workflow

1. Start application
2. Load CSV file
3. Show data preview
4. Build query step by step
5. Show generated SQL-like query
6. Execute and show results
7. Repeat for NoSQL with JSON
'''

    # ============= MAIN EXECUTION =============
    def run(self):
        """Execute the update"""
        print("=" * 70)
        print(" FINAL PROJECT UPDATE")
        print(" SQL + NoSQL System with JOIN Support")
        print(" Simplified Structure for Easy Explanation")
        print("=" * 70)
        print()
        
        # All files in root directory for simplicity
        files = {
            '.streamlit/config.toml': self.get_streamlit_config(),
            'app.py': self.get_app_content(),
            'sql_engine.py': self.get_sql_engine_content(),
            'nosql_engine.py': self.get_nosql_engine_content(),
            'csv_parser.py': self.get_csv_parser_content(),
            'query_executor.py': self.get_query_executor_content(),
            'nosql_executor.py': self.get_nosql_executor_content(),
            'README.md': self.get_readme_content(),
            '.gitignore': '__pycache__/\\n*.pyc\\nbackups*/\\n.streamlit/secrets.toml\\n'
        }
        
        print("File Structure (FLAT - Easy to Understand):")
        for filepath in files.keys():
            print(f"   {filepath}")
        print()
        print("Benefits of Flat Structure:")
        print("   - All main files in root directory")
        print("   - No deep folder nesting")
        print("   - Easy to navigate and explain")
        print("   - Clear separation of concerns")
        print()
        
        try:
            response = input("Proceed? (yes/no): ").strip().lower()
        except KeyboardInterrupt:
            print("\\nCancelled")
            return
        
        if response not in ['yes', 'y']:
            print("Cancelled")
            return
        
        print()
        self.create_backup_dir()
        print()
        
        for filepath, content in files.items():
            file_path = Path(filepath)
            
            if file_path.exists():
                self.backup_file(filepath)
            
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f"Created: {filepath}")
        
        print()
        print("=" * 70)
        print(" UPDATE COMPLETE!")
        print("=" * 70)
        print()
        print("Structure:")
        print("   app.py              - Main application")
        print("   sql_engine.py       - SQL operations")
        print("   nosql_engine.py     - NoSQL operations")
        print("   csv_parser.py       - CSV parsing")
        print("   query_executor.py   - SQL execution")
        print("   nosql_executor.py   - NoSQL execution")
        print()
        print("NEW Features:")
        print("   ✓ JOIN support (upload second CSV)")
        print("   ✓ SQL-order query building")
        print("   ✓ Simplified file structure")
        print("   ✓ Easy to explain to professors")
        print()
        print("To run:")
        print("   streamlit run app.py")
        print()

def main():
    try:
        updater = FinalProjectUpdater()
        updater.run()
    except KeyboardInterrupt:
        print("\\n\\nCancelled")
    except Exception as e:
        print(f"\\n\\nError: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()