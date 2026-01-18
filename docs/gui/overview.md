# GUI Overview

Knowhere Desktop is a modern SQL query interface built with Tauri and React.

![Knowhere GUI](../assets/screenshots/gui-screenshot-1.png)
*Main interface showing SQL editor, table explorer, and results*

## Key Features

### Monaco Editor
The SQL editor uses Monaco (same as VS Code) with:

- Syntax highlighting
- SQL intellisense
- Table and column auto-complete
- Keyboard shortcuts

### Table Explorer
The sidebar displays:

- All loaded tables
- Expandable column lists with data types
- Click to auto-generate SELECT query

### Results Pane
Query results displayed with:

- Virtualized scrolling for large datasets
- Resizable columns
- Row highlighting
- Light and dark theme support
