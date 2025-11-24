# Data query Intelligence System

DSCI 551 Fall 2025
Authors: Lance Dsilva, Chelroy Limas, Rafayel Mirijanyan

## Project Structure

```
project/
├── app.py                  # Main Streamlit application
├── sql_engine.py           # SQL engine (handles CSV)
├── nosql_engine.py         # NoSQL engine (handles JSON)
├── csv_parser.py           # Custom CSV parser (no pandas)
├── query_executor.py       # Executes SQL operations
├── nosql_executor.py       # Executes NoSQL operations
├── merge_sort.py           # Executes Mergesort operations
└── .streamlit/
    └── config.toml         # Configuration (500MB upload limit)
```
## Setup

### 1. Create a virtual environment

```bash
python3 -m venv .venv
```

Activate it:

- macOS/Linux: `source .venv/bin/activate`
- Windows PowerShell: `.venv\Scripts\Activate.ps1`

### 2. Install requirements

```bash
pip install -r requirements.txt
```

### 3. Run the Streamlit app

```bash
streamlit run app.py
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

## Implementation Details

- **Custom CSV parser (`csv_parser.py`)**  
  Parses CSV files without relying on `csv`/`pandas`, supports chunked streaming via `parse_file_in_chunks`, and estimates file stats for progress indicators.

- **SQL engine (`sql_engine.py` + `query_executor.py`)**  
  Maintains query-building state, supports SELECT/JOIN/FILTER/GROUP BY/ORDER BY/LIMIT operations, and can switch between in-memory mode and chunked processing for large files.

- **Chunked ORDER BY with merge sort (`merge_sort.py`)**  
  Implements an external merge sort: each streamed chunk is filtered, sorted, spilled to a temp file, then merged with a k-way heap. Serialization uses the custom JSON parser from `nosql_engine.py` so no stdlib `json` dependency is required.

- **JOIN handling**  
  JOIN operations build an index on the join table for INNER/LEFT/RIGHT joins. When chunking is enabled, the system detects JOIN usage and reloads all data in-memory to keep semantics correct.

- **NoSQL engine (`nosql_engine.py`, `nosql_executor.py`)**  
  Includes a hand-written JSON parser/executor that loads documents, applies filter/project/group/limit operations, and mirrors the SQL interface for JSON datasets.

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

## Demo Workflow

1. Start application
2. Load CSV file
3. Show data preview
4. Build query step by step
5. Show generated SQL-like query
6. Execute and show results
7. Repeat for NoSQL with JSON
