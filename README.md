# Knowhere

A powerful SQL engine for querying CSV, Parquet, Delta Lake, and SQLite files via an interactive TUI or command line. Built on Apache DataFusion for production-grade query performance.

![Knowhere GUI](docs/assets/screenshots/gui-screenshot-1.png)

## Features

- **Apache DataFusion Engine** - Production-grade SQL engine with query optimization and full ANSI SQL support
- **Multiple Format Support** - Query CSV, Parquet, Delta Lake, and SQLite files
- **Desktop GUI** - Modern IDE-like interface with Monaco editor, intellisense, and resizable panes
- **Interactive TUI** - Vim-style terminal interface for exploring your data
- **Multi-Table Queries** - Point to a folder and JOIN across different file formats
- **Advanced SQL** - CTEs, window functions, subqueries, UNION, and 100+ built-in functions
- **Query Persistence** - Save and load SQL queries, with recent queries tracking
- **Zero Configuration** - Automatic schema inference and format detection

## Installation

### Using curl (Linux/macOS)

```bash
curl -fsSL https://raw.githubusercontent.com/saivarunk/knowhere/main/install.sh | bash
```

### Using Homebrew

```bash
# Add the tap
brew tap saivarunk/knowhere

# Install CLI (terminal interface)
brew install knowhere

# Install GUI (desktop app)
brew install --cask knowhere
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

# Query a Delta Lake table directory
knowhere ./delta_table/

# Query SQLite database (all tables loaded automatically)
knowhere database.db

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

### Common Table Expressions (CTEs)

```sql
-- Simple CTE
WITH high_earners AS (
    SELECT name, salary FROM users WHERE salary > 80000
)
SELECT * FROM high_earners ORDER BY salary DESC

-- Multiple CTEs
WITH
    dept_avg AS (
        SELECT department, AVG(salary) as avg_sal FROM users GROUP BY department
    ),
    high_depts AS (
        SELECT * FROM dept_avg WHERE avg_sal > 70000
    )
SELECT * FROM high_depts

-- Recursive CTE
WITH RECURSIVE numbers(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 10
)
SELECT * FROM numbers
```

### Window Functions

```sql
-- Row number
SELECT name, salary,
       ROW_NUMBER() OVER (ORDER BY salary DESC) as rank
FROM users

-- Partition by department
SELECT department, name, salary,
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
FROM users

-- Running totals
SELECT name, salary,
       SUM(salary) OVER (ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
FROM users

-- Lag and Lead
SELECT name, salary,
       LAG(salary, 1) OVER (ORDER BY salary) as prev_salary,
       LEAD(salary, 1) OVER (ORDER BY salary) as next_salary
FROM users
```

### Subqueries

```sql
-- Scalar subquery in WHERE
SELECT name, salary
FROM users
WHERE salary > (SELECT AVG(salary) FROM users)

-- Correlated subquery
SELECT u.name, u.salary
FROM users u
WHERE u.salary > (
    SELECT AVG(u2.salary)
    FROM users u2
    WHERE u2.department = u.department
)

-- EXISTS subquery
SELECT u.name
FROM users u
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.user_id = u.id
)

-- Subquery in FROM
SELECT dept_avg.department, dept_avg.avg_sal
FROM (
    SELECT department, AVG(salary) as avg_sal
    FROM users
    GROUP BY department
) as dept_avg
WHERE dept_avg.avg_sal > 70000
```

### Set Operations

```sql
-- Union
SELECT name FROM users WHERE department = 'Engineering'
UNION
SELECT name FROM users WHERE department = 'Marketing'

-- Intersect
SELECT user_id FROM orders WHERE status = 'completed'
INTERSECT
SELECT user_id FROM orders WHERE status = 'shipped'

-- Except (difference)
SELECT id FROM users
EXCEPT
SELECT user_id FROM orders
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

-- Advanced aggregates
SELECT
    department,
    STDDEV(salary) as salary_stddev,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) as median_salary,
    ARRAY_AGG(name) as employee_names
FROM users
GROUP BY department
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

## Supported Data Formats

| Format | Extension | Features |
|--------|-----------|----------|
| CSV | `.csv` | Auto-delimiter detection, header inference |
| Parquet | `.parquet`, `.pq` | All compression codecs (Snappy, GZIP, Brotli, Zstd, LZ4) |
| Delta Lake | `_delta_log/` directory | Read Delta tables with ACID guarantees |
| SQLite | `.db`, `.sqlite`, `.sqlite3` | All tables loaded automatically |

## Supported SQL Features

| Feature | Status |
|---------|--------|
| SELECT columns, *, aliases | ✅ |
| DISTINCT | ✅ |
| FROM | ✅ |
| JOIN (INNER, LEFT, RIGHT, FULL, CROSS) | ✅ |
| WHERE | ✅ |
| AND, OR, NOT | ✅ |
| Comparison operators (=, !=, <, >, <=, >=) | ✅ |
| LIKE, ILIKE | ✅ |
| IN, NOT IN | ✅ |
| BETWEEN | ✅ |
| IS NULL / IS NOT NULL | ✅ |
| GROUP BY | ✅ |
| HAVING | ✅ |
| ORDER BY (ASC, DESC, NULLS FIRST/LAST) | ✅ |
| LIMIT | ✅ |
| OFFSET | ✅ |
| **CTEs (Common Table Expressions)** | ✅ |
| **Recursive CTEs** | ✅ |
| **Subqueries (scalar, correlated, EXISTS)** | ✅ |
| **Window Functions (ROW_NUMBER, RANK, LAG, LEAD, etc.)** | ✅ |
| **UNION / UNION ALL** | ✅ |
| **INTERSECT** | ✅ |
| **EXCEPT** | ✅ |
| COUNT, SUM, AVG, MIN, MAX | ✅ |
| STDDEV, VARIANCE, PERCENTILE | ✅ |
| ARRAY_AGG, STRING_AGG | ✅ |
| Arithmetic expressions (+, -, *, /, %) | ✅ |
| String functions (CONCAT, SUBSTRING, UPPER, LOWER, LENGTH, etc.) | ✅ |
| Date/Time functions (EXTRACT, DATE_TRUNC, etc.) | ✅ |
| CASE expressions | ✅ |
| CAST type conversions | ✅ |
| Regular expressions (REGEXP_MATCH, REGEXP_REPLACE) | ✅ |
| JSON functions | ✅ |
| **100+ Built-in Functions** | ✅ |

## Data Types

Knowhere supports all Apache Arrow data types with automatic inference:

- **INTEGER** - Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64
- **FLOAT** - Float32, Float64
- **STRING** - Utf8, LargeUtf8
- **BOOLEAN** - Boolean
- **DATE/TIME** - Date32, Date64, Timestamp (all units), Time32, Time64
- **NULL** - Null values
- **BINARY** - Binary, LargeBinary
- **DECIMAL** - Decimal128, Decimal256
- **LIST** - List, LargeList
- **STRUCT** - Nested structures

## Performance

Built on Apache DataFusion, Knowhere provides:
- **Vectorized execution** using Apache Arrow
- **Query optimization** with logical and physical plan optimization
- **Parallel execution** for multi-core utilization
- **Streaming execution** for large result sets
- **Predicate pushdown** for efficient filtering
- **Column pruning** to read only required columns

## License

MIT
