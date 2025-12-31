## Knowhere: CSV/Parquet SQL Explorer

Knowhere is a lightweight SQL engine for querying CSV and Parquet files via an interactive TUI.

### Core Requirements

**Functionality**
- ANSI SQL-compatible query engine for CSV/Parquet files. The engine should be implemented from scratch. Don't use libraries like DataFusion for CSV/Parquet parsing.
- Point to a single file (treated as one table) or a folder (each file becomes a table, named by filename)
- Support JOINs across multiple files/tables
- Interactive TUI for writing and executing queries with results displayed inline

**Tech Stack**
- Rust
- Use existing libraries where sensible (e.g., Ratatui for TUI)

### Deliverables

1. **CLI/TUI Application**
   - `sqlx <file-or-folder>` launches TUI
   - Optional: `sqlx --query "SELECT ..." <file>` for non-interactive use

2. **Installation**
   - One-liner curl install from GitHub releases
   - Homebrew formula

3. **Samples**
	 - Sample CSV/Parquet files for testing the engine and cli
	 
4. **Tests**
   - Unit tests for SQL engine (SELECT, WHERE, JOIN, aggregations)
   - Integration tests via CLI with sample CSV/Parquet files

	 
### Success Criteria
- User can run `sqlx data.csv` and interactively query the file
- User can run `sqlx ./data-folder/` and query/join across all files in the folder
- Installation works via curl one-liner and brew