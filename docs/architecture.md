# Architecture

Knowhere is built on a layered architecture with Apache DataFusion at its core.

## High-Level Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    User Interfaces                          │
├──────────────────────────┬──────────────────────────────────┤
│     Desktop GUI          │         Terminal TUI             │
│   (Tauri + React)        │       (Ratatui + Rust)           │
├──────────────────────────┴──────────────────────────────────┤
│                   Knowhere Core Library                     │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────────┐ │
│  │ File Loader │  │ SQL Engine  │  │ Result Serialization │ │
│  └─────────────┘  └─────────────┘  └──────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                   Apache DataFusion                         │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────────┐ │
│  │ SQL Parser  │  │ Optimizer   │  │ Execution Engine     │ │
│  └─────────────┘  └─────────────┘  └──────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                     Apache Arrow                            │
│         (In-memory columnar data representation)            │
└─────────────────────────────────────────────────────────────┘
```

## Components

### Core Library (`knowhere`)

The shared Rust library that powers both GUI and TUI:

| Module | Purpose |
|--------|---------|
| `storage/table.rs` | Schema, Row, Table data structures |
| `datafusion/context.rs` | DataFusion wrapper for SQL execution |
| `datafusion/loader.rs` | File loading and format detection |

### File Loader

Automatic format detection and loading:

```rust
// Detects format by extension
loader.load_file(path)?;  // .csv, .parquet, .db, etc.
loader.load_directory(path)?;  // All files become tables
```

**Supported Formats:**

| Format | Extensions | Handler |
|--------|------------|---------|
| CSV | `.csv` | Auto-delimiter detection |
| Parquet | `.parquet`, `.pq` | All compression codecs |
| Delta Lake | `_delta_log/` dir | ACID-compliant reads |
| SQLite | `.db`, `.sqlite`, `.sqlite3` | All tables loaded |

### DataFusion Context

SQL execution powered by Apache DataFusion:

- **Query Parsing** - Full ANSI SQL support
- **Optimization** - Logical and physical plan optimization  
- **Execution** - Vectorized execution on Arrow arrays
- **Functions** - 100+ built-in aggregate/scalar functions

### Desktop GUI (`gui/`)

Built with Tauri + React + TypeScript:

```
gui/
├── src/                 # React frontend
│   ├── components/      # UI components
│   │   ├── Editor/      # Monaco SQL editor
│   │   ├── Sidebar/     # Table explorer
│   │   └── Results/     # Virtualized results table
│   └── lib/             # API bindings, types
└── src-tauri/           # Rust backend
    ├── commands.rs      # IPC command handlers
    └── lib.rs           # Tauri app setup
```

### Terminal TUI (`src/tui/`)

Built with Ratatui:

- **Editor Widget** - Multi-line SQL input
- **Results Table** - Scrollable data view
- **Vim Keybindings** - Modal editing

## Data Flow

```
1. User opens file/folder
   ↓
2. FileLoader detects format and loads data
   ↓
3. DataFusionContext registers tables
   ↓
4. User writes SQL query
   ↓
5. DataFusion parses and optimizes query
   ↓
6. Query executes on Arrow arrays
   ↓
7. Results returned as Table struct
   ↓
8. UI renders results (GUI: React, TUI: Ratatui)
```

## Technology Stack

| Layer | Technology |
|-------|------------|
| SQL Engine | Apache DataFusion |
| Data Format | Apache Arrow |
| GUI Framework | Tauri 2.x |
| GUI Frontend | React 19, TypeScript, Monaco Editor |
| TUI Framework | Ratatui |
| Language | Rust |
