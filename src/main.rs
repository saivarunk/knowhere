use std::io::stdout;

use crossterm::{
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::prelude::*;

use knowhere::cli::{Cli, OutputFormat};
use knowhere::datafusion::{DataFusionContext, FileLoader};
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

fn load_data(cli: &Cli) -> Result<DataFusionContext, Box<dyn std::error::Error>> {
    let mut loader = FileLoader::new()?;
    let path = &cli.path;

    if path.is_file() {
        loader.load_file(path)?;
    } else if path.is_dir() {
        loader.load_directory(path)?;
    } else {
        return Err(format!("Path does not exist: {}", path.display()).into());
    }

    let ctx = loader.into_context();

    if ctx.table_count() == 0 {
        return Err("No valid data files found".into());
    }

    Ok(ctx)
}

fn run_query(
    ctx: &DataFusionContext,
    query: &str,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = ctx.execute_sql(query)?;

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

fn run_tui(ctx: DataFusionContext) -> Result<(), Box<dyn std::error::Error>> {
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
