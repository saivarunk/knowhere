# Knowhere — Agent & Contributor Guide

## Project Goals

Knowhere is a multi-format SQL query engine built on Apache DataFusion. It lets users load files
from different sources (CSV, JSON, Parquet, Delta Lake, Apache Iceberg, SQLite) into a shared
session and query them with standard SQL — including cross-format JOINs. It ships both a TUI
(terminal UI via Ratatui) and a GUI (Tauri desktop app).

## Project Structure

```
src/
  datafusion/       # Core engine: context, loader, format detection, SQLite provider
  tui/              # Terminal UI (Ratatui): app state, event loop, rendering
  gui/              # Desktop GUI (Tauri)
  storage/          # Shared data types: Table, Schema, Column, Value
  cli.rs            # CLI entry point (clap)
tests/              # Integration tests (use real sample files)
samples/            # Sample data files used by tests and for manual exploration
```

## Rust Best Practices

- **Errors**: use `Result`-returning functions throughout. `unwrap()`/`expect()` are only
  acceptable in tests and in `main()` for unrecoverable startup failures.
- **Error types**: prefer the explicit `DataFusionError` enum over `Box<dyn Error>`. Add variants
  rather than stringing errors.
- **Type system**: make invalid states unrepresentable. Encode invariants in types, not runtime
  checks.
- **Modules**: one concern per file. Keep `datafusion/context.rs` for session management,
  `datafusion/loader.rs` for format detection and registration, etc.
- **Allocations**: avoid unnecessary `clone()` and heap allocations. Prefer borrowing.
- **Clippy**: the CI runs `cargo clippy -- -D warnings`. Fix all warnings; never `#[allow(...)]`
  without a comment explaining why.
- **Formatting**: `cargo fmt` is enforced in CI. Run it before every commit.

## Test-Driven Development

- Write tests alongside new features — no untested code is merged.
- **Unit tests** live in `#[cfg(test)]` blocks inside the source file they test.
- **Integration tests** live in `tests/` and use real sample files from `samples/`.
- Every new file format needs at minimum:
  - A load test (file is registered without error)
  - A query test (SELECT returns correct rows/columns)
  - A schema inference test (column names and count are correct)
- Every new SQL feature needs at least one positive test and one negative/edge-case test.
- Use `tempfile::tempdir()` for tests that create on-disk fixtures (Delta, Iceberg, Parquet);
  never leave test artifacts in the repo.
- Run `cargo test` before every commit — all tests must pass.

## Key Commands

```sh
cargo build                    # compile
cargo test                     # run full test suite
cargo test <name>              # run tests matching <name>
cargo clippy -- -D warnings    # lint (must be clean)
cargo fmt                      # format (enforced in CI)
npm run tauri dev              # run GUI in dev mode
```

## CI

GitHub Actions runs on every push/PR to `main`:
1. `cargo fmt --check`
2. `cargo clippy -- -D warnings`
3. `cargo test`

All three must pass on both `ubuntu-latest` and `macos-latest` before merging.
