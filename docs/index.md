# Knowhere

A powerful SQL engine for querying CSV, Parquet, Delta Lake, and SQLite files.

<div class="grid cards" markdown>

-   :material-monitor: **Desktop GUI**

    Modern IDE-like interface with Monaco editor, SQL intellisense, and resizable panes.

    [:octicons-arrow-right-24: GUI Documentation](gui/overview.md)

-   :material-console: **Terminal TUI**

    Vim-style terminal interface for exploring your data from the command line.

    [:octicons-arrow-right-24: TUI Documentation](tui/overview.md)

</div>

## Features

- **Apache DataFusion Engine** - Production-grade SQL engine with query optimization
- **Multiple Format Support** - CSV, Parquet, Delta Lake, SQLite
- **Zero Configuration** - Automatic schema inference and format detection
- **Multi-Table Queries** - JOIN across different file formats
- **Advanced SQL** - CTEs, window functions, subqueries, 100+ built-in functions

## Quick Start

=== "Homebrew (CLI)"

    ```bash
    brew tap saivarunk/knowhere
    brew install knowhere
    ```

=== "Homebrew (GUI)"

    ```bash
    brew tap saivarunk/knowhere
    brew install --cask knowhere
    ```

=== "From Source"

    ```bash
    git clone https://github.com/saivarunk/knowhere.git
    cd knowhere
    cargo build --release
    ```

## Example

```sql
SELECT 
    users.name,
    COUNT(orders.id) as order_count,
    SUM(orders.amount) as total_spent
FROM users
LEFT JOIN orders ON users.id = orders.user_id
GROUP BY users.name
ORDER BY total_spent DESC
LIMIT 10
```
