# SQL Engine from Scratch

A comprehensive SQL-like data processing engine built entirely from scratch in Python, without using pandas, csv module, or any external data processing libraries.

## 🎯 Project Overview

This project implements a complete SQL engine capable of parsing CSV files, executing SQL operations, and processing large datasets through chunked reading - all using only built-in Python features.

## 👥 Team Members

- **Lance Dsilva** 
- **Chelroy Limas** 
- **Rafayel Mirijanyan** 

**Course**: DSCI 551 - Fall 2025  
**Section**: Tuesday

## ✨ Key Features

- **Custom CSV Parser**: Handles delimiters, quotes, escape characters, and edge cases
- **Chunked Processing**: Process files larger than memory in configurable batches
- **SQL Operations**:
  - Filter (WHERE clauses)
  - Projection (SELECT columns)
  - Group By & Aggregation (SUM, AVG, COUNT, MIN, MAX)
  - Joins (INNER, LEFT, RIGHT, OUTER)
  - Order By & Limit
- **Query Engine**: Chain multiple operations seamlessly
- **Interactive UI**: Streamlit-based web application for testing

## 🚀 Quick Start
```bash
# Clone the repository
git clone [your-repo-url]
cd SQL-Engine-from-Scratch

# Install Streamlit (only UI dependency)
pip install streamlit

# Run the application
streamlit run app.py
```

## 📁 Project Structure
```
sql_component/
├── parsers/          # CSV parsing & data type inference
├── operations/       # SQL operations (filter, join, groupby, etc.)
├── core/            # Table structure & query engine
├── utils/           # Validation helpers
└── tests/           # Unit tests
```

## 🧪 Testing
```bash
# Run parser tests
python sql_component/tests/test_parser.py

# Run operations tests
python sql_component/tests/test_operations.py
```

## 📊 Sample Usage
```python
from sql_component.parsers.csv_parser import CSVParser
from sql_component.core.table import Table
from sql_component.core.query_engine import QueryEngine

# Parse CSV
parser = CSVParser()
data = parser.parse_file('products.csv')
table = Table(data['headers'], data['data'], 'products')

# Execute query
result = (QueryEngine(table)
    .filter(lambda row: row['price'] > 100)
    .select(['name', 'category', 'price'])
    .order_by('price', ascending=False)
    .limit(10)
    .execute())
```

## 🎓 Learning Outcomes

- Deep understanding of data parsing and processing
- Memory management for large datasets
- SQL query execution fundamentals
- Software architecture for data systems
- Team collaboration on complex projects

## 📝 Note

This implementation uses **zero external data processing libraries** - only Python built-ins like `open()`, `list`, `dict`, and basic string operations. This provides hands-on experience with low-level data processing concepts.

---

**Academic Project** | DSCI 551 Fall 2025 | University of Southern California
