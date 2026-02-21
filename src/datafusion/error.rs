use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataFusionError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("Delta Lake error: {0}")]
    Delta(#[from] deltalake::DeltaTableError),

    #[error("Iceberg error: {0}")]
    Iceberg(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("Unsupported file format: {0}")]
    UnsupportedFormat(String),

    #[error("Invalid table name: {0}")]
    InvalidTableName(String),

    #[error("Table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Conversion error: {0}")]
    Conversion(String),

    #[error("SQLite table not found: {0}")]
    SqliteTableNotFound(String),
}

pub type Result<T> = std::result::Result<T, DataFusionError>;
