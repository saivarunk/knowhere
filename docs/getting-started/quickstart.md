# Quick Start

## GUI

1. **Open the app** - Launch Knowhere from your Applications folder or menu
2. **Load data** - Click "Open File" or "Open Folder" to load CSV, Parquet, or Delta files
3. **Browse tables** - Expand tables in the sidebar to see columns
4. **Write SQL** - Type your query in the editor with intellisense support
5. **Execute** - Press `⌘+Enter` (Mac) or `Ctrl+Enter` (Linux) to run

## TUI

```bash
# Query a CSV file
knowhere data.csv

# Query a Parquet file
knowhere sales.parquet

# Query all files in a folder
knowhere ./data-folder/

# Query a SQLite database
knowhere database.db
```

## Your First Query

Once your data is loaded, try:

```sql
-- See all columns and rows
SELECT * FROM your_table LIMIT 10

-- Count rows
SELECT COUNT(*) FROM your_table

-- Filter and sort
SELECT * FROM users WHERE age > 30 ORDER BY name
```

## Multi-Table Queries

When loading a folder, each file becomes a table:

```bash
knowhere ./data/
```

```sql
SELECT 
    users.name,
    orders.amount
FROM users
JOIN orders ON users.id = orders.user_id
```
