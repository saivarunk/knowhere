use std::fs;
use std::io::stdout;
use std::path::Path;

use crossterm::{
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::prelude::*;

use knowhere::cli::{Cli, OutputFormat};
use knowhere::sql::executor::{execute_query, ExecutionContext};
use knowhere::storage::csv::CsvReader;
use knowhere::storage::parquet::ParquetReader;
use knowhere::storage::table::Table;
use knowhere::tui::{app::App, input::handle_events, ui::draw};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse_args();

    // Load data into execution context
    let ctx = load_data(&cli)?;

    if let Some(query) = &cli.query {
        // Non-interactive mode
        run_query(&ctx, query, cli.format)?;
    } else {
        // Interactive TUI mode
        run_tui(ctx)?;
    }

    Ok(())
}

fn load_data(cli: &Cli) -> Result<ExecutionContext, Box<dyn std::error::Error>> {
    let mut ctx = ExecutionContext::new();
    let path = &cli.path;

    if path.is_file() {
        let table = load_file(path, cli)?;
        ctx.add_table(table);
    } else if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_path = entry.path();

            if file_path.is_file() {
                let ext = file_path.extension().and_then(|e| e.to_str());
                match ext {
                    Some("csv") | Some("parquet") | Some("pq") => {
                        match load_file(&file_path, cli) {
                            Ok(table) => ctx.add_table(table),
                            Err(e) => eprintln!("Warning: Failed to load {}: {}", file_path.display(), e),
                        }
                    }
                    _ => {}
                }
            }
        }
    } else {
        return Err(format!("Path does not exist: {}", path.display()).into());
    }

    if ctx.tables.is_empty() {
        return Err("No valid data files found".into());
    }

    Ok(ctx)
}

fn load_file(path: &Path, cli: &Cli) -> Result<Table, Box<dyn std::error::Error>> {
    let ext = path.extension().and_then(|e| e.to_str());

    match ext {
        Some("csv") => {
            let reader = CsvReader::new()
                .with_delimiter(cli.delimiter)
                .with_header(!cli.no_header);
            Ok(reader.read_file(path)?)
        }
        Some("parquet") | Some("pq") => {
            let reader = ParquetReader::new();
            Ok(reader.read_file(path)?)
        }
        _ => Err(format!("Unsupported file format: {}", path.display()).into()),
    }
}

fn run_query(
    ctx: &ExecutionContext,
    query: &str,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = execute_query(ctx, query)?;

    match format {
        OutputFormat::Table => print_table(&result),
        OutputFormat::Csv => print_csv(&result),
        OutputFormat::Json => print_json(&result),
    }

    Ok(())
}

fn print_table(table: &Table) {
    if table.row_count() == 0 {
        println!("(0 rows)");
        return;
    }

    // Calculate column widths
    let widths: Vec<usize> = table
        .schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let header_width = col.name.len();
            let max_value_width = table
                .rows
                .iter()
                .map(|row| row.values.get(i).map(|v| v.to_string().len()).unwrap_or(0))
                .max()
                .unwrap_or(0);
            header_width.max(max_value_width)
        })
        .collect();

    // Print header
    let header: Vec<String> = table
        .schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, col)| format!("{:width$}", col.name, width = widths[i]))
        .collect();
    println!("{}", header.join(" | "));

    // Print separator
    let sep: Vec<String> = widths.iter().map(|&w| "-".repeat(w)).collect();
    println!("{}", sep.join("-+-"));

    // Print rows
    for row in &table.rows {
        let values: Vec<String> = row
            .values
            .iter()
            .enumerate()
            .map(|(i, v)| format!("{:width$}", v, width = widths[i]))
            .collect();
        println!("{}", values.join(" | "));
    }

    println!("({} rows)", table.row_count());
}

fn print_csv(table: &Table) {
    // Header
    let header: Vec<&str> = table.schema.columns.iter().map(|c| c.name.as_str()).collect();
    println!("{}", header.join(","));

    // Rows
    for row in &table.rows {
        let values: Vec<String> = row.values.iter().map(|v| {
            let s = v.to_string();
            if s.contains(',') || s.contains('"') || s.contains('\n') {
                format!("\"{}\"", s.replace('"', "\"\""))
            } else {
                s
            }
        }).collect();
        println!("{}", values.join(","));
    }
}

fn print_json(table: &Table) {
    print!("[");
    for (i, row) in table.rows.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!("{{");
        for (j, (col, val)) in table.schema.columns.iter().zip(row.values.iter()).enumerate() {
            if j > 0 {
                print!(",");
            }
            let val_str = match val {
                knowhere::storage::table::Value::String(s) => format!("\"{}\"", s.replace('"', "\\\"")),
                knowhere::storage::table::Value::Null => "null".to_string(),
                knowhere::storage::table::Value::Boolean(b) => b.to_string(),
                _ => val.to_string(),
            };
            print!("\"{}\":{}", col.name, val_str);
        }
        print!("}}");
    }
    println!("]");
}

fn run_tui(ctx: ExecutionContext) -> Result<(), Box<dyn std::error::Error>> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create app
    let mut app = App::new(ctx);

    // Main loop
    loop {
        terminal.draw(|frame| draw(frame, &app))?;

        if handle_events(&mut app)? {
            break;
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}
