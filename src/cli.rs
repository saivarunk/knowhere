use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "knowhere")]
#[command(author, version, about = "A lightweight SQL engine for querying CSV and Parquet files")]
pub struct Cli {
    /// Path to a CSV/Parquet file or folder containing data files
    #[arg(required = true)]
    pub path: PathBuf,

    /// Execute a SQL query directly (non-interactive mode)
    #[arg(short, long)]
    pub query: Option<String>,

    /// Output format for non-interactive mode
    #[arg(short, long, default_value = "table")]
    pub format: OutputFormat,

    /// CSV delimiter (only for CSV files)
    #[arg(short, long, default_value = ",")]
    pub delimiter: char,

    /// Disable CSV header detection
    #[arg(long)]
    pub no_header: bool,
}

#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum OutputFormat {
    #[default]
    Table,
    Csv,
    Json,
}

impl Cli {
    pub fn parse_args() -> Self {
        Cli::parse()
    }
}
