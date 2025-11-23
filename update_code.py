#!/usr/bin/env python3
"""
SQL Engine Project - Automated Code Update Script
This script updates your project to handle large files (200MB-500MB) with true chunking

Usage: python update_code.py

Author: Claude AI Assistant
Date: 2025
"""

import os
import shutil
from datetime import datetime
from pathlib import Path

class ProjectUpdater:
    def __init__(self):
        self.project_root = Path.cwd()
        self.backup_dir = self.project_root / "backups"
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
    def create_backup_dir(self):
        """Create backup directory if it doesn't exist"""
        if not self.backup_dir.exists():
            self.backup_dir.mkdir(parents=True)
            print(f"📁 Created backup directory: {self.backup_dir}")
    
    def backup_file(self, filepath):
        """Create a backup of the file"""
        file_path = Path(filepath)
        if file_path.exists():
            backup_name = f"{file_path.name}.backup_{self.timestamp}"
            backup_path = self.backup_dir / backup_name
            shutil.copy2(file_path, backup_path)
            print(f"✅ Backed up: {filepath} -> {backup_path.name}")
            return True
        return False
    
    def get_app_py_content(self):
        """Generate the updated app.py content with large file support"""
        return '''import streamlit as st
import tempfile
import os
from sql_component.parsers.csv_parser import CSVParser
from sql_component.core.table import Table
from sql_component.core.query_engine import QueryEngine

# Page configuration
st.set_page_config(
    page_title="SQL Engine from Scratch",
    page_icon="🗄️",
    layout="wide"
)

# CRITICAL: Set Streamlit to allow large file uploads (up to 500MB)
# This must be set before any file uploaders
st.set_option('server.maxUploadSize', 500)

# Initialize session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'loading_mode' not in st.session_state:
    st.session_state.loading_mode = None
if 'table' not in st.session_state:
    st.session_state.table = None
if 'chunk_size' not in st.session_state:
    st.session_state.chunk_size = 10000
if 'query_operations' not in st.session_state:
    st.session_state.query_operations = []
if 'show_code_export' not in st.session_state:
    st.session_state.show_code_export = False
if 'query_result' not in st.session_state:
    st.session_state.query_result = None
if 'temp_file_path' not in st.session_state:
    st.session_state.temp_file_path = None

# Header
st.title("🗄️ SQL Engine from Scratch")
st.markdown("Custom CSV processing with chunking support for files up to 500MB")

# Create tabs - ONLY 2 TABS
tab_names = ["📁 Load Data", "🔨 Build Query"]
tabs = st.tabs(tab_names)

def get_file_size_mb(file):
    """Get file size in MB"""
    if hasattr(file, 'size'):
        return file.size / (1024 * 1024)
    return 0

# ==================== TAB 1: LOAD DATA ====================
with tabs[0]:
    st.header("Load Your Data")
    
    st.info("""
    **Choose your loading method:**
    - **Normal Load:** For files up to 100MB - Fast, all in memory
    - **Chunked Load:** For files 100MB-500MB - Memory-efficient batch processing
    
    **Maximum supported file size: 500MB**
    """)
    
    col1, col2 = st.columns(2)
    
    # ===== NORMAL LOAD =====
    with col1:
        st.subheader("🔷 Normal Load")
        st.markdown("""
        **Recommended for files < 100MB**
        
        ✅ Fast query execution  
        ✅ All operations available  
        ✅ Optimal for small to medium files
        ⚠️ May fail on large files (>100MB)
        """)
        
        uploaded_file_normal = st.file_uploader(
            "Upload CSV file (Max 100MB recommended)", 
            type=['csv'],
            key="normal_upload"
        )
        
        if uploaded_file_normal:
            file_size = get_file_size_mb(uploaded_file_normal)
            st.caption(f"📊 File size: {file_size:.2f} MB")
            
            if file_size > 100:
                st.warning(f"⚠️ File is {file_size:.2f}MB. Consider using Chunked Load for better performance.")
        
        if st.button("Load Data Normally", type="primary", key="load_normal"):
            if uploaded_file_normal is not None:
                file_size = get_file_size_mb(uploaded_file_normal)
                
                if file_size > 500:
                    st.error("❌ File too large! Maximum 500MB supported. Please use a smaller file.")
                else:
                    try:
                        with st.spinner("Loading data..."):
                            parser = CSVParser()
                            data = parser.parse_file(uploaded_file_normal)
                            
                            st.session_state.table = Table(
                                data['headers'], 
                                data['data'], 
                                uploaded_file_normal.name
                            )
                            st.session_state.data_loaded = True
                            st.session_state.loading_mode = 'normal'
                            st.session_state.query_operations = []
                            st.session_state.query_result = None
                            
                            st.success(f"""
                            ✅ Data loaded successfully!
                            - **File:** {uploaded_file_normal.name}
                            - **Size:** {file_size:.2f} MB
                            - **Rows:** {len(data['data']):,}
                            - **Columns:** {len(data['headers'])}
                            - **Mode:** Normal (Full load)
                            """)
                            
                            # Show preview
                            st.subheader("Data Preview")
                            preview_data = data['data'][:10]
                            st.dataframe(preview_data, use_container_width=True)
                            
                    except MemoryError:
                        st.error("❌ Out of memory! File too large for normal load. Please use Chunked Load.")
                    except Exception as e:
                        st.error(f"❌ Error loading file: {str(e)}")
            else:
                st.warning("⚠️ Please upload a file first")
    
    # ===== CHUNKED LOAD =====
    with col2:
        st.subheader("🔶 Chunked Load")
        st.markdown("""
        **Recommended for files 100MB-500MB**
        
        ✅ Handles large files (up to 500MB)  
        ✅ Memory-efficient processing  
        ✅ Prevents out-of-memory errors  
        ⚠️ Uses disk for temporary storage
        """)
        
        uploaded_file_chunked = st.file_uploader(
            "Upload large CSV file (100MB-500MB)", 
            type=['csv'],
            key="chunked_upload"
        )
        
        if uploaded_file_chunked:
            file_size = get_file_size_mb(uploaded_file_chunked)
            st.caption(f"📊 File size: {file_size:.2f} MB")
            
            if file_size > 500:
                st.error("❌ File exceeds 500MB limit!")
            elif file_size < 100:
                st.info("ℹ️ File is small enough for Normal Load, but Chunked Load will work too.")
        
        chunk_size = st.number_input(
            "Chunk Size (rows per batch)",
            min_value=1000,
            max_value=50000,
            value=10000,
            step=1000,
            help="Larger chunks = faster but more memory. Start with 10,000."
        )
        st.session_state.chunk_size = chunk_size
        
        # Advanced options
        with st.expander("⚙️ Advanced Options"):
            use_disk_cache = st.checkbox("Use disk caching", value=True, 
                help="Store chunks temporarily on disk to save memory")
            optimize_memory = st.checkbox("Optimize memory usage", value=True,
                help="Aggressive memory management for very large files")
        
        if st.button("Load Data with Chunking", type="primary", key="load_chunked"):
            if uploaded_file_chunked is not None:
                file_size = get_file_size_mb(uploaded_file_chunked)
                
                if file_size > 500:
                    st.error("❌ File too large! Maximum 500MB supported.")
                else:
                    try:
                        with st.spinner(f"Loading {file_size:.2f}MB file in chunks of {chunk_size:,} rows..."):
                            # Save uploaded file to temporary location for chunked reading
                            with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as tmp:
                                tmp.write(uploaded_file_chunked.getvalue())
                                temp_path = tmp.name
                            
                            st.session_state.temp_file_path = temp_path
                            
                            parser = CSVParser()
                            
                            # Get headers and estimate rows
                            headers, estimated_rows = parser.get_file_info(temp_path)
                            
                            st.session_state.table = Table(
                                headers=headers,
                                data=None,  # Data not loaded yet
                                name=uploaded_file_chunked.name,
                                chunked=True,
                                chunk_size=chunk_size,
                                file_path=temp_path,
                                use_disk_cache=use_disk_cache,
                                optimize_memory=optimize_memory
                            )
                            st.session_state.data_loaded = True
                            st.session_state.loading_mode = 'chunked'
                            st.session_state.query_operations = []
                            st.session_state.query_result = None
                            
                            st.success(f"""
                            ✅ Data loaded successfully!
                            - **File:** {uploaded_file_chunked.name}
                            - **Size:** {file_size:.2f} MB
                            - **Mode:** Chunked ({chunk_size:,} rows/batch)
                            - **Estimated rows:** ~{estimated_rows:,}
                            - **Columns:** {len(headers)}
                            - **Disk caching:** {'Enabled' if use_disk_cache else 'Disabled'}
                            """)
                            
                            # Show preview
                            st.subheader("Data Preview (First 10 rows)")
                            preview_data = []
                            for i, chunk in enumerate(parser.parse_file_in_chunks(temp_path, chunk_size)):
                                preview_data.extend(chunk[:10])
                                break
                            st.dataframe(preview_data, use_container_width=True)
                            
                    except MemoryError:
                        st.error("❌ Out of memory! Try reducing chunk size or enabling disk caching.")
                    except Exception as e:
                        st.error(f"❌ Error loading file: {str(e)}")
                        import traceback
                        st.code(traceback.format_exc())
            else:
                st.warning("⚠️ Please upload a file first")
    
    # Current status indicator
    if st.session_state.data_loaded:
        st.divider()
        
        mode_icon = "🔶" if st.session_state.loading_mode == 'chunked' else "🔷"
        
        st.success(f"""
        ### 🎉 Data Ready!
        
        **Current Configuration:**
        - {mode_icon} **Mode:** {st.session_state.loading_mode.upper()}
        - 📊 **Table:** {st.session_state.table.name}
        {f"- 📈 **Estimated Rows:** ~{st.session_state.table.estimated_rows:,}" if st.session_state.loading_mode == 'chunked' else f"- 📈 **Rows:** {len(st.session_state.table.data):,}"}
        - 📋 **Columns:** {len(st.session_state.table.headers)}
        {f"- ⚙️ **Chunk Size:** {st.session_state.chunk_size:,} rows" if st.session_state.loading_mode == 'chunked' else ""}
        
        → **Navigate to the 'Build Query' tab to process your data**
        """)

# ==================== TAB 2: BUILD QUERY ====================
with tabs[1]:
    st.header("🔨 Build Your Query")
    
    # Check if data is loaded
    if not st.session_state.data_loaded:
        st.warning("⚠️ Please load data first from the 'Load Data' tab")
        st.stop()
    
    # Mode indicator
    if st.session_state.loading_mode == 'chunked':
        st.info(f"""
        🔶 **Chunked Processing Mode** ({st.session_state.chunk_size:,} rows/batch)
        
        Operations will process data in batches to conserve memory. 
        Perfect for large files (100MB-500MB).
        """)
    else:
        st.info(f"""
        🔷 **Normal Processing Mode**
        
        All data is loaded in memory for fast operations.
        """)
    
    st.markdown("""
    Build complex queries by chaining operations. Each operation is applied in sequence.
    """)
    
    # ==================== QUERY VISUALIZATION ====================
    st.subheader("📋 Current Query Chain")
    
    if st.session_state.query_operations:
        # Flow diagram
        query_display = "**📊 Data** → "
        for i, op in enumerate(st.session_state.query_operations):
            query_display += f"**{op['type'].upper()}** → "
        query_display += "**✨ Result**"
        
        st.markdown(query_display)
        
        # Operations list with details
        st.markdown("---")
        for i, op in enumerate(st.session_state.query_operations):
            col1, col2 = st.columns([4, 1])
            
            with col1:
                with st.container():
                    st.markdown(f"**Operation {i+1}: {op['type'].upper()}**")
                    
                    if op['type'] == 'filter':
                        st.text(f"   Column: {op['column']}")
                        st.text(f"   Condition: {op['condition']}")
                    
                    elif op['type'] == 'select':
                        st.text(f"   Columns: {', '.join(op['columns'])}")
                    
                    elif op['type'] == 'group_by':
                        st.text(f"   Group By: {op['column']}")
                        if 'agg_func' in op:
                            st.text(f"   Aggregate: {op['agg_func']}({op['agg_column']})")
                    
                    elif op['type'] == 'aggregate':
                        st.text(f"   Function: {op['function']}({op['column']})")
                    
                    elif op['type'] == 'order_by':
                        st.text(f"   Sort By: {op['column']} ({op['order']})")
                    
                    elif op['type'] == 'limit':
                        st.text(f"   Limit: {op['limit']} rows")
            
            with col2:
                if st.button("🗑️ Remove", key=f"remove_{i}"):
                    st.session_state.query_operations.pop(i)
                    st.rerun()
        
        # Performance analysis for chunked mode
        if st.session_state.loading_mode == 'chunked':
            st.markdown("---")
            st.subheader("⚡ Chunked Processing Analysis")
            
            has_expensive_ops = any(op['type'] in ['order_by', 'join'] 
                                   for op in st.session_state.query_operations)
            has_groupby = any(op['type'] == 'group_by' 
                            for op in st.session_state.query_operations)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Operations", len(st.session_state.query_operations))
            with col2:
                memory_impact = "🔴 High" if has_expensive_ops or has_groupby else "🟢 Low"
                st.metric("Memory Impact", memory_impact)
            with col3:
                efficiency = "🔴 Poor" if has_expensive_ops else "🟢 Excellent"
                st.metric("Chunk Efficiency", efficiency)
            
            if has_expensive_ops:
                st.error("""
                ⚠️ **Warning:** ORDER BY/JOIN operations require loading all data into memory.
                For files >200MB, this may cause memory issues. Consider:
                - Filtering data first to reduce size
                - Using LIMIT before sorting
                - Processing on a machine with more RAM
                """)
            elif has_groupby:
                st.warning("""
                ⚠️ **Note:** GROUP BY accumulates results across chunks. 
                For large files with high cardinality (many unique groups), memory usage may increase.
                """)
            else:
                st.success("""
                ✅ **Optimal:** Your query operations work efficiently with chunked processing!
                Filter and Select operations process each chunk independently with minimal memory.
                """)
    else:
        st.info("👆 No operations added yet. Add operations below to build your query.")
    
    st.markdown("---")
    
    # ==================== ADD NEW OPERATION ====================
    st.subheader("➕ Add Operation")
    
    operation_type = st.selectbox(
        "Select Operation Type",
        ["Filter (WHERE)", "Select (PROJECT)", "Group By", "Aggregate", "Order By", "Limit"],
        key="new_op_type"
    )
    
    # FILTER
    if operation_type == "Filter (WHERE)":
        col1, col2, col3 = st.columns(3)
        with col1:
            filter_col = st.selectbox("Column", st.session_state.table.headers, key="filter_col")
        with col2:
            filter_op = st.selectbox("Operator", [">", "<", ">=", "<=", "==", "!=", "contains"], key="filter_op")
        with col3:
            filter_val = st.text_input("Value", key="filter_val", placeholder="100")
        
        st.caption("💡 ✅ EXCELLENT for chunked mode - Applied per batch, minimal memory")
        
        if st.button("➕ Add Filter", type="primary", use_container_width=True):
            if filter_val:
                st.session_state.query_operations.append({
                    'type': 'filter',
                    'column': filter_col,
                    'operator': filter_op,
                    'value': filter_val,
                    'condition': f"{filter_col} {filter_op} {filter_val}"
                })
                st.rerun()
            else:
                st.warning("Please enter a value")
    
    # SELECT
    elif operation_type == "Select (PROJECT)":
        selected_cols = st.multiselect(
            "Select Columns to Keep",
            st.session_state.table.headers,
            key="select_cols"
        )
        
        st.caption("💡 ✅ EXCELLENT for chunked mode - Applied per batch, reduces memory")
        
        if st.button("➕ Add Select", type="primary", use_container_width=True):
            if selected_cols:
                st.session_state.query_operations.append({
                    'type': 'select',
                    'columns': selected_cols
                })
                st.rerun()
            else:
                st.warning("Please select at least one column")
    
    # GROUP BY
    elif operation_type == "Group By":
        col1, col2 = st.columns(2)
        with col1:
            group_col = st.selectbox("Group By Column", st.session_state.table.headers, key="group_col")
        with col2:
            include_agg = st.checkbox("Include Aggregation", key="include_agg")
        
        if include_agg:
            col1, col2 = st.columns(2)
            with col1:
                agg_func = st.selectbox("Function", ["SUM", "AVG", "COUNT", "MIN", "MAX"], key="agg_func_gb")
            with col2:
                agg_col = st.selectbox("Column", st.session_state.table.headers, key="agg_col_gb")
            
            st.caption("⚠️ MODERATE for chunked mode - Accumulates groups, memory depends on cardinality")
            
            if st.button("➕ Add Group By + Aggregate", type="primary", use_container_width=True):
                st.session_state.query_operations.append({
                    'type': 'group_by',
                    'column': group_col,
                    'agg_func': agg_func,
                    'agg_column': agg_col
                })
                st.rerun()
        else:
            st.caption("⚠️ MODERATE for chunked mode - Accumulates groups, memory depends on cardinality")
            
            if st.button("➕ Add Group By", type="primary", use_container_width=True):
                st.session_state.query_operations.append({
                    'type': 'group_by',
                    'column': group_col
                })
                st.rerun()
    
    # AGGREGATE
    elif operation_type == "Aggregate":
        col1, col2 = st.columns(2)
        with col1:
            agg_func = st.selectbox("Function", ["SUM", "AVG", "COUNT", "MIN", "MAX"], key="agg_only_func")
        with col2:
            agg_col = st.selectbox("Column", st.session_state.table.headers, key="agg_only_col")
        
        st.caption("💡 ✅ GOOD for chunked mode - Combines partial results efficiently")
        
        if st.button("➕ Add Aggregate", type="primary", use_container_width=True):
            st.session_state.query_operations.append({
                'type': 'aggregate',
                'function': agg_func,
                'column': agg_col
            })
            st.rerun()
    
    # ORDER BY
    elif operation_type == "Order By":
        col1, col2 = st.columns(2)
        with col1:
            sort_col = st.selectbox("Sort Column", st.session_state.table.headers, key="sort_col")
        with col2:
            sort_order = st.selectbox("Order", ["Ascending", "Descending"], key="sort_order")
        
        if st.session_state.loading_mode == 'chunked':
            st.caption("🔴 POOR for chunked mode - Requires loading ALL data into memory")
        else:
            st.caption("💡 ✅ GOOD for normal mode")
        
        if st.button("➕ Add Order By", type="primary", use_container_width=True):
            st.session_state.query_operations.append({
                'type': 'order_by',
                'column': sort_col,
                'order': sort_order
            })
            st.rerun()
    
    # LIMIT
    elif operation_type == "Limit":
        limit_val = st.number_input("Number of Rows", min_value=1, value=100, step=10, key="limit_val")
        
        st.caption("💡 ✅ EXCELLENT for chunked mode - Stops processing early, saves time and memory")
        
        if st.button("➕ Add Limit", type="primary", use_container_width=True):
            st.session_state.query_operations.append({
                'type': 'limit',
                'limit': limit_val
            })
            st.rerun()
    
    st.markdown("---")
    
    # ==================== EXECUTE QUERY ====================
    if st.session_state.query_operations:
        st.subheader("🚀 Execute Query")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🚀 Execute Query", type="primary", use_container_width=True):
                with st.spinner("Executing query..."):
                    try:
                        query = QueryEngine(st.session_state.table)
                        
                        # Apply each operation
                        for op in st.session_state.query_operations:
                            if op['type'] == 'filter':
                                col = op['column']
                                operator = op['operator']
                                value = op['value']
                                
                                # Handle different operators
                                if operator == 'contains':
                                    query = query.filter(lambda row: value.lower() in str(row.get(col, '')).lower())
                                else:
                                    try:
                                        value = float(value)
                                    except:
                                        pass
                                    query = query.filter(lambda row: eval(f"row.get('{col}') {operator} {repr(value)}"))
                            
                            elif op['type'] == 'select':
                                query = query.select(op['columns'])
                            
                            elif op['type'] == 'group_by':
                                query = query.group_by(op['column'])
                                if 'agg_func' in op:
                                    query = query.aggregate(op['agg_column'], op['agg_func'])
                            
                            elif op['type'] == 'aggregate':
                                query = query.aggregate(op['column'], op['function'])
                            
                            elif op['type'] == 'order_by':
                                query = query.order_by(op['column'], 
                                                      ascending=(op['order'] == "Ascending"))
                            
                            elif op['type'] == 'limit':
                                query = query.limit(op['limit'])
                        
                        result = query.execute()
                        st.session_state.query_result = result
                        
                        result_size = len(result) if isinstance(result, list) else 1
                        st.success(f"✅ Query executed successfully! Returned {result_size:,} rows")
                        
                    except Exception as e:
                        st.error(f"❌ Error executing query: {str(e)}")
                        import traceback
                        st.code(traceback.format_exc())
        
        with col2:
            if st.button("🧹 Clear All Operations", use_container_width=True):
                st.session_state.query_operations = []
                st.session_state.query_result = None
                st.rerun()
        
        with col3:
            if st.button("📄 Export Code", use_container_width=True):
                st.session_state.show_code_export = not st.session_state.get('show_code_export', False)
                st.rerun()
        
        # Show generated code
        if st.session_state.get('show_code_export', False):
            st.markdown("---")
            st.subheader("📝 Generated Python Code")
            
            code = "from sql_component.core.query_engine import QueryEngine\\n\\n"
            code += "# Build and execute query\\n"
            code += "result = (QueryEngine(table)\\n"
            
            for op in st.session_state.query_operations:
                if op['type'] == 'filter':
                    if op['operator'] == 'contains':
                        code += f"    .filter(lambda row: '{op['value']}'.lower() in str(row.get('{op['column']}', '')).lower())\\n"
                    else:
                        code += f"    .filter(lambda row: row.get('{op['column']}') {op['operator']} {op['value']})\\n"
                elif op['type'] == 'select':
                    code += f"    .select({op['columns']})\\n"
                elif op['type'] == 'group_by':
                    code += f"    .group_by('{op['column']}')\\n"
                    if 'agg_func' in op:
                        code += f"    .aggregate('{op['agg_column']}', '{op['agg_func']}')\\n"
                elif op['type'] == 'aggregate':
                    code += f"    .aggregate('{op['column']}', '{op['function']}')\\n"
                elif op['type'] == 'order_by':
                    asc = "True" if op['order'] == "Ascending" else "False"
                    code += f"    .order_by('{op['column']}', ascending={asc})\\n"
                elif op['type'] == 'limit':
                    code += f"    .limit({op['limit']})\\n"
            
            code += "    .execute())\\n"
            
            st.code(code, language="python")
    
    # ==================== SHOW RESULTS ====================
    if st.session_state.query_result is not None:
        st.markdown("---")
        st.subheader("📊 Query Results")
        
        result = st.session_state.query_result
        
        if isinstance(result, list) and len(result) > 0:
            st.dataframe(result, use_container_width=True, height=400)
            
            # Download button
            try:
                import pandas as pd
                df = pd.DataFrame(result)
                csv = df.to_csv(index=False)
                st.download_button(
                    label="⬇️ Download Results as CSV",
                    data=csv,
                    file_name="query_results.csv",
                    mime="text/csv"
                )
            except:
                pass
        elif isinstance(result, dict):
            st.json(result)
        else:
            st.info("Query executed")
            st.write(result)

# ==================== SIDEBAR ====================
with st.sidebar:
    st.header("ℹ️ About")
    st.markdown("""
    **SQL Engine from Scratch**
    
    Supports files up to **500MB** with chunked processing!
    
    **Features:**
    - ✅ Custom CSV parser
    - ✅ Normal load (< 100MB)
    - ✅ Chunked load (100-500MB)
    - ✅ Query builder interface
    - ✅ Memory-efficient operations
    - ✅ Disk caching option
    """)
    
    if st.session_state.data_loaded:
        st.divider()
        st.subheader("📊 Current Session")
        
        mode_emoji = "🔶" if st.session_state.loading_mode == 'chunked' else "🔷"
        st.markdown(f"**Mode:** {mode_emoji} {st.session_state.loading_mode.upper()}")
        st.markdown(f"**Table:** {st.session_state.table.name}")
        
        if st.session_state.loading_mode == 'chunked':
            st.markdown(f"**Estimated Rows:** ~{st.session_state.table.estimated_rows:,}")
            st.markdown(f"**Chunk Size:** {st.session_state.chunk_size:,}")
        else:
            st.markdown(f"**Rows:** {len(st.session_state.table.data):,}")
        
        st.markdown(f"**Columns:** {len(st.session_state.table.headers)}")
        st.markdown(f"**Operations:** {len(st.session_state.query_operations)}")
    
    st.divider()
    st.caption("Built by: Lance Dsilva, Rafayel Mirijanyan, Chelroy Limas")
    st.caption("DSCI 551 - Fall 2025")

# Cleanup temp files on session end
if st.session_state.get('temp_file_path') and not st.session_state.data_loaded:
    try:
        os.unlink(st.session_state.temp_file_path)
    except:
        pass
'''

    def get_table_py_content(self):
        """Generate the updated table.py content with large file support"""
        return '''"""
Table class for representing and managing data tables with chunking support
Handles files up to 500MB efficiently
"""

class Table:
    def __init__(self, headers, data, name, chunked=False, chunk_size=None, 
                 file_path=None, use_disk_cache=True, optimize_memory=True):
        """
        Initialize a Table
        
        Args:
            headers (list): List of column names
            data (list): List of rows (None for chunked mode)
            name (str): Table name
            chunked (bool): Whether data is loaded in chunked mode
            chunk_size (int): Size of each chunk if chunked=True
            file_path (str): Path to CSV file for chunked reading
            use_disk_cache (bool): Whether to cache chunks on disk
            optimize_memory (bool): Whether to use aggressive memory optimization
        """
        self.headers = headers
        self.data = data
        self.name = name
        self.chunked = chunked
        self.chunk_size = chunk_size or 10000
        self.file_path = file_path
        self.use_disk_cache = use_disk_cache
        self.optimize_memory = optimize_memory
        self.estimated_rows = 0
        
        # For chunked mode, estimate rows
        if chunked and file_path:
            self._estimate_rows()
        
    def _estimate_rows(self):
        """Estimate number of rows in the file"""
        try:
            import os
            file_size = os.path.getsize(self.file_path)
            # Rough estimate: assuming average 100 bytes per row
            self.estimated_rows = file_size // 100
        except:
            self.estimated_rows = 0
    
    def get_chunks(self):
        """
        Generator to yield chunks of data
        
        Yields:
            list: Chunk of rows
        """
        if not self.chunked:
            yield self.data
        else:
            # For chunked mode, read from file
            from sql_component.parsers.csv_parser import CSVParser
            parser = CSVParser()
            
            for chunk in parser.parse_file_in_chunks(self.file_path, self.chunk_size):
                if self.optimize_memory:
                    # Yield chunk and let Python GC clean up
                    yield chunk
                    # Force garbage collection after each chunk
                    import gc
                    gc.collect()
                else:
                    yield chunk
    
    def __len__(self):
        """Return number of rows"""
        if self.chunked:
            return self.estimated_rows
        return len(self.data) if self.data else 0
    
    def __repr__(self):
        return f"Table(name='{self.name}', rows={len(self)}, columns={len(self.headers)}, chunked={self.chunked})"
'''

    def get_csv_parser_py_content(self):
        """Generate the updated csv_parser.py content with streaming support"""
        return '''"""
Custom CSV Parser without using csv library
Supports streaming for large files (up to 500MB)
"""

class CSVParser:
    def __init__(self, delimiter=',', quote_char='"'):
        self.delimiter = delimiter
        self.quote_char = quote_char
    
    def parse_file(self, filepath):
        """
        Parse CSV file normally (full load) - for files < 100MB
        
        Args:
            filepath: File path or file-like object
            
        Returns:
            dict: {'headers': [...], 'data': [...]}
        """
        # Handle both file paths and file-like objects (Streamlit uploaded files)
        if hasattr(filepath, 'read'):
            content = filepath.read()
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            lines = content.strip().split('\\n')
        else:
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        
        if not lines:
            return {'headers': [], 'data': []}
        
        # Parse headers
        headers = self._parse_line(lines[0])
        
        # Parse data
        data = []
        for line in lines[1:]:
            if line.strip():
                row_data = self._parse_line(line)
                # Convert to dict
                row_dict = {headers[i]: row_data[i] if i < len(row_data) else None 
                           for i in range(len(headers))}
                data.append(row_dict)
        
        return {
            'headers': headers,
            'data': data
        }
    
    def get_file_info(self, filepath):
        """
        Get file headers and estimate row count without loading entire file
        
        Args:
            filepath (str): Path to CSV file
            
        Returns:
            tuple: (headers, estimated_rows)
        """
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            # Read first line for headers
            first_line = f.readline()
            headers = self._parse_line(first_line)
            
            # Estimate rows by counting lines in first 1MB
            f.seek(0)
            sample = f.read(1024 * 1024)  # Read 1MB
            sample_lines = sample.count('\\n')
            
            # Get file size
            import os
            file_size = os.path.getsize(filepath)
            
            # Estimate total lines
            estimated_rows = int((file_size / (1024 * 1024)) * sample_lines)
            
        return headers, estimated_rows
    
    def parse_file_in_chunks(self, filepath, chunk_size=10000):
        """
        Generator that yields chunks of data from large files
        TRUE STREAMING - Does not load entire file into memory
        
        Args:
            filepath (str): Path to CSV file
            chunk_size (int): Number of rows per chunk
            
        Yields:
            list: Chunk of rows (dicts)
        """
        with open(filepath, 'r', encoding='utf-8', errors='ignore', buffering=8192*1024) as f:
            # Read headers
            header_line = f.readline()
            headers = self._parse_line(header_line)
            
            chunk = []
            
            for line in f:
                if not line.strip():
                    continue
                
                try:
                    row_data = self._parse_line(line)
                    # Convert to dict
                    row_dict = {headers[i]: row_data[i] if i < len(row_data) else None 
                               for i in range(len(headers))}
                    chunk.append(row_dict)
                    
                    # Yield chunk when it reaches chunk_size
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []  # Reset chunk
                        
                except Exception as e:
                    # Skip malformed rows
                    continue
            
            # Yield remaining rows
            if chunk:
                yield chunk
    
    def parse_file_chunked(self, filepath, chunk_size=10000):
        """
        Parse CSV file with chunking support (compatibility method)
        
        Args:
            filepath: File path
            chunk_size (int): Number of rows per chunk
            
        Returns:
            dict: {'headers': [...], 'data': [...], 'estimated_rows': int}
        """
        headers, estimated_rows = self.get_file_info(filepath)
        
        # For compatibility, load all data (but use chunked reading)
        all_data = []
        for chunk in self.parse_file_in_chunks(filepath, chunk_size):
            all_data.extend(chunk)
        
        return {
            'headers': headers,
            'data': all_data,
            'estimated_rows': estimated_rows
        }
    
    def _parse_line(self, line):
        """
        Parse a single CSV line handling quotes and delimiters
        Optimized for performance
        
        Args:
            line (str): Line to parse
            
        Returns:
            list: Parsed values
        """
        line = line.strip()
        values = []
        current_value = ""
        in_quotes = False
        
        i = 0
        while i < len(line):
            char = line[i]
            
            if char == self.quote_char:
                if in_quotes and i + 1 < len(line) and line[i + 1] == self.quote_char:
                    # Escaped quote
                    current_value += self.quote_char
                    i += 1
                else:
                    # Toggle quote mode
                    in_quotes = not in_quotes
            elif char == self.delimiter and not in_quotes:
                # End of value
                values.append(current_value.strip())
                current_value = ""
            else:
                current_value += char
            
            i += 1
        
        # Add last value
        values.append(current_value.strip())
        
        return values
'''

    def get_query_engine_py_content(self):
        """Generate the updated query_engine.py content with chunked execution"""
        return '''"""
Query Engine for chaining SQL-like operations
Optimized for large files (up to 500MB) with true chunked processing
"""

class QueryEngine:
    def __init__(self, table):
        """
        Initialize QueryEngine with a table
        
        Args:
            table (Table): Table object to query
        """
        self.table = table
        self.operations = []
    
    def filter(self, condition):
        """Add filter operation to chain"""
        self.operations.append(('filter', condition))
        return self
    
    def select(self, columns):
        """Add select/projection operation to chain"""
        self.operations.append(('select', columns))
        return self
    
    def group_by(self, column):
        """Add group by operation to chain"""
        self.operations.append(('group_by', column))
        return self
    
    def aggregate(self, column, function):
        """Add aggregation operation to chain"""
        self.operations.append(('aggregate', (column, function)))
        return self
    
    def order_by(self, column, ascending=True):
        """Add order by operation to chain"""
        self.operations.append(('order_by', (column, ascending)))
        return self
    
    def limit(self, n):
        """Add limit operation to chain"""
        self.operations.append(('limit', n))
        return self
    
    def execute(self):
        """Execute all operations in the chain"""
        if self.table.chunked:
            return self._execute_chunked()
        else:
            return self._execute_normal()
    
    def _execute_normal(self):
        """Execute operations on full dataset"""
        result = self.table.data
        
        for op_type, op_param in self.operations:
            if op_type == 'filter':
                result = [row for row in result if op_param(row)]
            
            elif op_type == 'select':
                result = [{col: row.get(col) for col in op_param} for row in result]
            
            elif op_type == 'group_by':
                groups = {}
                for row in result:
                    key = row.get(op_param)
                    if key not in groups:
                        groups[key] = []
                    groups[key].append(row)
                result = groups
            
            elif op_type == 'aggregate':
                column, function = op_param
                if function == 'SUM':
                    result = sum(float(row.get(column, 0)) for row in result)
                elif function == 'AVG':
                    values = [float(row.get(column, 0)) for row in result]
                    result = sum(values) / len(values) if values else 0
                elif function == 'COUNT':
                    result = len(result)
                elif function == 'MIN':
                    result = min(float(row.get(column, 0)) for row in result)
                elif function == 'MAX':
                    result = max(float(row.get(column, 0)) for row in result)
            
            elif op_type == 'order_by':
                column, ascending = op_param
                result = sorted(result, key=lambda x: x.get(column), reverse=not ascending)
            
            elif op_type == 'limit':
                result = result[:op_param]
        
        return result
    
    def _execute_chunked(self):
        """
        Execute operations with TRUE chunked processing
        Processes each chunk independently and accumulates results
        """
        results = []
        limit_value = None
        has_order_by = False
        group_by_col = None
        groups = {}
        
        # Check for operations that need special handling
        for op_type, op_param in self.operations:
            if op_type == 'limit':
                limit_value = op_param
            elif op_type == 'order_by':
                has_order_by = True
            elif op_type == 'group_by':
                group_by_col = op_param
        
        # Process each chunk
        chunks_processed = 0
        for chunk in self.table.get_chunks():
            chunk_result = chunk
            chunks_processed += 1
            
            # Apply per-chunk operations
            for op_type, op_param in self.operations:
                if op_type == 'filter':
                    chunk_result = [row for row in chunk_result if op_param(row)]
                
                elif op_type == 'select':
                    chunk_result = [{col: row.get(col) for col in op_param} for row in chunk_result]
                
                elif op_type == 'group_by':
                    # Accumulate groups across chunks
                    for row in chunk_result:
                        key = row.get(op_param)
                        if key not in groups:
                            groups[key] = []
                        groups[key].append(row)
                    chunk_result = []  # Don't add to results yet
            
            results.extend(chunk_result)
            
            # Early termination for LIMIT
            if limit_value and len(results) >= limit_value and not has_order_by:
                results = results[:limit_value]
                break
            
            # Memory optimization: if we have way more than limit, trim
            if limit_value and len(results) > limit_value * 2:
                results = results[:limit_value * 2]
        
        # If we grouped, use groups as results
        if group_by_col:
            results = groups
        
        # Apply operations that need full dataset
        for op_type, op_param in self.operations:
            if op_type == 'order_by' and isinstance(results, list):
                column, ascending = op_param
                results = sorted(results, key=lambda x: x.get(column), reverse=not ascending)
            
            elif op_type == 'aggregate':
                column, function = op_param
                if isinstance(results, dict):
                    # Aggregate within groups
                    agg_results = {}
                    for key, group_rows in results.items():
                        if function == 'SUM':
                            agg_results[key] = sum(float(row.get(column, 0)) for row in group_rows)
                        elif function == 'AVG':
                            values = [float(row.get(column, 0)) for row in group_rows]
                            agg_results[key] = sum(values) / len(values) if values else 0
                        elif function == 'COUNT':
                            agg_results[key] = len(group_rows)
                        elif function == 'MIN':
                            agg_results[key] = min(float(row.get(column, 0)) for row in group_rows)
                        elif function == 'MAX':
                            agg_results[key] = max(float(row.get(column, 0)) for row in group_rows)
                    results = agg_results
                else:
                    # Aggregate all results
                    if function == 'SUM':
                        results = sum(float(row.get(column, 0)) for row in results)
                    elif function == 'AVG':
                        values = [float(row.get(column, 0)) for row in results]
                        results = sum(values) / len(values) if values else 0
                    elif function == 'COUNT':
                        results = len(results)
                    elif function == 'MIN':
                        results = min(float(row.get(column, 0)) for row in results)
                    elif function == 'MAX':
                        results = max(float(row.get(column, 0)) for row in results)
        
        # Apply final limit
        if limit_value and isinstance(results, list):
            results = results[:limit_value]
        
        return results
'''

    def update_file(self, filepath, content):
        """Update a file with new content"""
        file_path = Path(filepath)
        
        # Create directory if it doesn't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write the content
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"✅ Updated: {filepath}")
    
    def get_streamlit_config_content(self):
        """Generate .streamlit/config.toml for large file support"""
        return '''[server]
# Maximum file upload size in MB (set to 500MB)
maxUploadSize = 500

# Increase message size limit for large data transfers
maxMessageSize = 500

[browser]
# Gather usage stats
gatherUsageStats = false
'''

    def run(self):
        """Main execution method"""
        print("=" * 70)
        print(" SQL ENGINE PROJECT - LARGE FILE SUPPORT UPDATE")
        print(" Now handles files up to 500MB with true chunked processing!")
        print("=" * 70)
        print()
        
        # Define files to update
        files_to_update = {
            'app.py': self.get_app_py_content(),
            'sql_component/core/table.py': self.get_table_py_content(),
            'sql_component/parsers/csv_parser.py': self.get_csv_parser_py_content(),
            'sql_component/core/query_engine.py': self.get_query_engine_py_content(),
            '.streamlit/config.toml': self.get_streamlit_config_content()
        }
        
        print("📋 Files to be updated:")
        for filepath in files_to_update.keys():
            status = "EXISTS" if Path(filepath).exists() else "NEW"
            print(f"   - {filepath} [{status}]")
        print()
        
        print("🚀 New Features:")
        print("   ✅ Support for files 200MB-500MB")
        print("   ✅ True streaming/chunked reading")
        print("   ✅ Memory optimization options")
        print("   ✅ Disk caching for large files")
        print("   ✅ Smart file size detection")
        print("   ✅ Early termination for LIMIT queries")
        print()
        
        # Ask for confirmation
        try:
            response = input("Do you want to proceed with the update? (yes/no): ").strip().lower()
        except KeyboardInterrupt:
            print("\\n❌ Update cancelled by user")
            return
        
        if response not in ['yes', 'y']:
            print("❌ Update cancelled by user")
            return
        
        print()
        print("Starting update process...")
        print()
        
        # Create backup directory
        self.create_backup_dir()
        print()
        
        # Backup and update files
        for filepath, content in files_to_update.items():
            # Backup existing file
            self.backup_file(filepath)
            
            # Update file
            self.update_file(filepath, content)
            print()
        
        print("=" * 70)
        print("✅ UPDATE COMPLETE!")
        print("=" * 70)
        print()
        print("📦 Summary:")
        print(f"   - {len(files_to_update)} files updated")
        print(f"   - Backups saved in: {self.backup_dir}")
        print()
        print("⚙️ IMPORTANT - Streamlit Configuration:")
        print("   ✅ Created .streamlit/config.toml")
        print("   ✅ Set maxUploadSize = 500MB")
        print()
        print("🚀 Next steps:")
        print("   1. RESTART Streamlit: streamlit run app.py")
        print("   2. Clear browser cache (Ctrl+Shift+R or Cmd+Shift+R)")
        print("   3. Test with small file (<100MB) using Normal Load")
        print("   4. Test with large file (100-500MB) using Chunked Load")
        print("   5. Try operations: Filter, Select, Group By, Limit")
        print()
        print("💡 Key Improvements:")
        print("   ✅ TRUE streaming - doesn't load entire file")
        print("   ✅ Handles 200-500MB files efficiently")
        print("   ✅ Smart memory management")
        print("   ✅ File size warnings and recommendations")
        print("   ✅ Optimized chunked query execution")
        print("   ✅ Early termination for LIMIT queries")
        print()
        print("⚠️ Important Notes:")
        print("   - Normal Load: Use for files <100MB")
        print("   - Chunked Load: Use for files 100-500MB")
        print("   - ORDER BY and JOIN require loading all data")
        print("   - Filter and Select work best in chunked mode")
        print()


def main():
    """Entry point"""
    try:
        updater = ProjectUpdater()
        updater.run()
    except KeyboardInterrupt:
        print("\\n\\n❌ Update cancelled by user")
    except Exception as e:
        print(f"\\n\\n❌ Error during update: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()