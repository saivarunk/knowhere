# Knowhere

A lightweight SQL engine for querying CSV and Parquet files via an interactive TUI or command line.

## Features

- **SQL Engine from Scratch** - Full ANSI SQL-compatible query engine built without external SQL libraries
- **CSV & Parquet Support** - Query CSV and Parquet files directly with automatic type inference
- **Interactive TUI** - Vim-style terminal interface with syntax highlighting
- **Multi-Table Queries** - Point to a folder and JOIN across multiple files
- **Rich SQL Support** - SELECT, JOIN, WHERE, GROUP BY, ORDER BY, aggregations, and more

## Installation

### Using curl (Linux/macOS)

```bash
curl -fsSL https://raw.githubusercontent.com/saivarunk/knowhere/main/install.sh | bash
```

### Using Homebrew

```bash
brew tap saivarunk/knowhere
brew install knowhere
```

### Building from Source

```bash
git clone https://github.com/saivarunk/knowhere.git
cd knowhere
cargo build --release
```

The binary will be available at `./target/release/knowhere`.

## Usage

### Interactive TUI Mode

Launch the interactive query interface:

```bash
# Query a single CSV file
knowhere data.csv

# Query a single Parquet file
knowhere data.parquet

# Query all files in a folder (each file becomes a table)
knowhere ./data-folder/
```

**TUI Keybindings:**
- `i` - Enter insert mode (type your query)
- `Esc` - Return to normal mode
- `Enter` - Execute query
- `j/k` - Scroll results up/down
- `h/l` - Scroll results left/right
- `Tab` - Switch focus between query editor and results
- `:q` - Quit

### Non-Interactive Mode

Run queries directly from the command line:

```bash
knowhere --query "SELECT * FROM data" data.csv
```

## SQL Examples

### Basic Queries

```sql
-- Select all columns
SELECT * FROM users

-- Select specific columns
SELECT name, email, age FROM users

-- Filter with WHERE
SELECT * FROM users WHERE age > 30

-- Sort results
SELECT * FROM users ORDER BY age DESC

-- Limit results
SELECT * FROM users LIMIT 10
```

### Filtering

```sql
-- Multiple conditions
SELECT * FROM users WHERE age > 25 AND department = 'Engineering'

-- IN clause
SELECT * FROM users WHERE status IN ('active', 'pending')

-- LIKE pattern matching
SELECT * FROM users WHERE name LIKE 'John%'

-- BETWEEN range
SELECT * FROM users WHERE age BETWEEN 25 AND 40

-- NULL checking
SELECT * FROM users WHERE email IS NOT NULL
```

### Aggregations

```sql
-- Count rows
SELECT COUNT(*) FROM orders

-- Sum values
SELECT SUM(amount) FROM orders

-- Average
SELECT AVG(price) FROM products

-- Min/Max
SELECT MIN(age), MAX(age) FROM users

-- Group by with aggregation
SELECT department, COUNT(*), AVG(salary)
FROM employees
GROUP BY department

-- Having clause
SELECT department, COUNT(*) as cnt
FROM employees
GROUP BY department
HAVING COUNT(*) > 5
```

### Joins (Multi-File Queries)

When pointing to a folder, each file becomes a table named after the filename (without extension):

```bash
knowhere ./data/
```

```sql
-- Inner join
SELECT users.name, orders.amount
FROM users
JOIN orders ON users.id = orders.user_id

-- Left join
SELECT users.name, orders.amount
FROM users
LEFT JOIN orders ON users.id = orders.user_id

-- Complex join with aggregation
SELECT
    users.name,
    COUNT(orders.id) AS order_count,
    SUM(orders.amount) AS total_spent
FROM users
LEFT JOIN orders ON users.id = orders.user_id
GROUP BY users.name
ORDER BY total_spent DESC
LIMIT 10
```

### Output Formats

```bash
# Table format (default)
knowhere --query "SELECT * FROM data" --format table data.csv

# CSV format
knowhere --query "SELECT * FROM data" --format csv data.csv

# JSON format
knowhere --query "SELECT * FROM data" --format json data.csv
```

### Additional Options

```bash
# Custom CSV delimiter
knowhere --delimiter ';' data.csv

# CSV without header
knowhere --no-header data.csv
```

## Supported SQL Features

| Feature | Status |
|---------|--------|
| SELECT columns, *, aliases | ✅ |
| DISTINCT | ✅ |
| FROM | ✅ |
| JOIN (INNER, LEFT, RIGHT, CROSS) | ✅ |
| WHERE | ✅ |
| AND, OR, NOT | ✅ |
| Comparison operators (=, !=, <, >, <=, >=) | ✅ |
| LIKE | ✅ |
| IN | ✅ |
| BETWEEN | ✅ |
| IS NULL / IS NOT NULL | ✅ |
| GROUP BY | ✅ |
| HAVING | ✅ |
| ORDER BY (ASC, DESC) | ✅ |
| LIMIT | ✅ |
| OFFSET | ✅ |
| COUNT, SUM, AVG, MIN, MAX | ✅ |
| Arithmetic expressions | ✅ |
| String concatenation (\|\|) | ✅ |
| CASE expressions | ✅ |
| Subqueries | ❌ |

## Data Types

Knowhere automatically infers column types from your data:

- **INTEGER** - Whole numbers
- **FLOAT** - Decimal numbers
- **STRING** - Text values
- **BOOLEAN** - true/false values
- **NULL** - Missing values (empty cells, "null", "NA", "N/A")

## License

MIT
