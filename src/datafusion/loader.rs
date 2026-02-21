use std::fs;
use std::path::Path;

use super::context::DataFusionContext;
use super::error::{DataFusionError, Result};

pub struct FileLoader {
    context: DataFusionContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum FileFormat {
    Csv,
    Json,
    Parquet,
    Delta,
    Sqlite,
    Iceberg,
}

impl FileLoader {
    pub fn new() -> Result<Self> {
        let context = DataFusionContext::new()?;
        Ok(Self { context })
    }

    pub fn load_file(&mut self, path: &Path) -> Result<Vec<String>> {
        if !path.exists() {
            return Err(DataFusionError::FileNotFound(
                path.to_string_lossy().to_string(),
            ));
        }

        if path.is_dir() {
            return self.load_directory(path);
        }

        let format = detect_file_format(path)?;
        let table_name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| DataFusionError::InvalidTableName("Invalid file name".to_string()))?
            .to_string();

        match format {
            FileFormat::Csv => {
                self.context.register_csv(&table_name, path)?;
                Ok(vec![table_name])
            }
            FileFormat::Json => {
                self.context.register_json(&table_name, path)?;
                Ok(vec![table_name])
            }
            FileFormat::Parquet => {
                self.context.register_parquet(&table_name, path)?;
                Ok(vec![table_name])
            }
            FileFormat::Sqlite => self.context.register_sqlite(path),
            FileFormat::Delta => Err(DataFusionError::UnsupportedFormat(
                "Delta Lake tables must be directories".to_string(),
            )),
            FileFormat::Iceberg => Err(DataFusionError::UnsupportedFormat(
                "Iceberg tables must be directories".to_string(),
            )),
        }
    }

    pub fn load_directory(&mut self, path: &Path) -> Result<Vec<String>> {
        if !path.is_dir() {
            return Err(DataFusionError::Conversion(format!(
                "{} is not a directory",
                path.display()
            )));
        }

        // Check for Delta Lake
        if is_delta_table(path) {
            let table_name = path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| {
                    DataFusionError::InvalidTableName("Invalid directory name".to_string())
                })?
                .to_string();
            self.context.register_delta(&table_name, path)?;
            return Ok(vec![table_name]);
        }

        // Check for Iceberg
        if is_iceberg_table(path) {
            let table_name = path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| {
                    DataFusionError::InvalidTableName("Invalid directory name".to_string())
                })?
                .to_string();
            self.context.register_iceberg(&table_name, path)?;
            return Ok(vec![table_name]);
        }

        // Load all files in directory
        let mut loaded_tables = Vec::new();
        let entries = fs::read_dir(path)?;

        for entry in entries {
            let entry = entry?;
            let entry_path = entry.path();

            if entry_path.is_file() {
                match self.load_file(&entry_path) {
                    Ok(mut tables) => loaded_tables.append(&mut tables),
                    Err(e) => {
                        eprintln!("Warning: Failed to load {}: {}", entry_path.display(), e);
                    }
                }
            }
        }

        if loaded_tables.is_empty() {
            return Err(DataFusionError::Conversion(
                "No supported files found in directory".to_string(),
            ));
        }

        Ok(loaded_tables)
    }

    pub fn into_context(self) -> DataFusionContext {
        self.context
    }

    pub fn context(&self) -> &DataFusionContext {
        &self.context
    }

    pub fn context_mut(&mut self) -> &mut DataFusionContext {
        &mut self.context
    }
}

fn detect_file_format(path: &Path) -> Result<FileFormat> {
    let extension = path
        .extension()
        .and_then(|s| s.to_str())
        .ok_or_else(|| DataFusionError::UnsupportedFormat("No file extension".to_string()))?
        .to_lowercase();

    match extension.as_str() {
        "csv" => Ok(FileFormat::Csv),
        "json" | "ndjson" | "jsonl" => Ok(FileFormat::Json),
        "parquet" | "pq" => Ok(FileFormat::Parquet),
        "db" | "sqlite" | "sqlite3" => Ok(FileFormat::Sqlite),
        _ => Err(DataFusionError::UnsupportedFormat(format!(
            "Unsupported file format: {}",
            extension
        ))),
    }
}

fn is_delta_table(path: &Path) -> bool {
    path.join("_delta_log").is_dir()
}

fn is_iceberg_table(path: &Path) -> bool {
    path.join("metadata").is_dir()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::path::PathBuf;

    fn get_samples_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("samples")
    }

    #[test]
    fn test_detect_csv() {
        let path = PathBuf::from("test.csv");
        assert_eq!(detect_file_format(&path).unwrap(), FileFormat::Csv);
    }

    #[test]
    fn test_detect_parquet() {
        let path = PathBuf::from("test.parquet");
        assert_eq!(detect_file_format(&path).unwrap(), FileFormat::Parquet);

        let path = PathBuf::from("test.pq");
        assert_eq!(detect_file_format(&path).unwrap(), FileFormat::Parquet);
    }

    #[test]
    fn test_detect_sqlite() {
        let path = PathBuf::from("test.db");
        assert_eq!(detect_file_format(&path).unwrap(), FileFormat::Sqlite);

        let path = PathBuf::from("test.sqlite");
        assert_eq!(detect_file_format(&path).unwrap(), FileFormat::Sqlite);
    }

    #[test]
    fn test_load_csv_file() {
        let samples = get_samples_path();
        let users_csv = samples.join("users.csv");

        if users_csv.exists() {
            let mut loader = FileLoader::new().unwrap();
            let result = loader.load_file(&users_csv);
            assert!(result.is_ok());

            let tables = result.unwrap();
            assert_eq!(tables.len(), 1);
            assert_eq!(tables[0], "users");
        }
    }

    #[test]
    fn test_load_directory() {
        let samples = get_samples_path();

        if samples.exists() && samples.is_dir() {
            let mut loader = FileLoader::new().unwrap();
            let result = loader.load_directory(&samples);

            if result.is_ok() {
                let tables = result.unwrap();
                assert!(!tables.is_empty());
            }
        }
    }
}
