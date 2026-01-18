use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::*;
use std::path::Path;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::storage::table::Table;

use super::conversion::record_batch_to_table;
use super::error::{DataFusionError, Result};
use super::sqlite::SqliteTableProvider;

pub struct DataFusionContext {
    session: SessionContext,
    runtime: Arc<Runtime>,
    table_names: Vec<String>,
}

impl DataFusionContext {
    pub fn new() -> Result<Self> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| DataFusionError::Io(e))?,
        );

        let runtime_config = RuntimeConfig::new();
        let runtime_env = Arc::new(RuntimeEnv::new(runtime_config)?);

        let session_config = SessionConfig::new()
            .with_information_schema(true)
            .with_batch_size(8192);

        let session = SessionContext::new_with_config_rt(session_config, runtime_env);

        Ok(Self {
            session,
            runtime,
            table_names: Vec::new(),
        })
    }

    pub fn register_csv(&mut self, name: impl Into<String>, path: &Path) -> Result<()> {
        let name = name.into();
        let path_str = path.to_str().ok_or_else(|| {
            DataFusionError::Conversion("Invalid UTF-8 in path".to_string())
        })?;

        self.runtime.block_on(async {
            let ctx = &self.session;
            ctx.register_csv(&name, path_str, CsvReadOptions::default())
                .await?;
            Ok::<_, DataFusionError>(())
        })?;

        self.table_names.push(name);
        Ok(())
    }

    pub fn register_parquet(&mut self, name: impl Into<String>, path: &Path) -> Result<()> {
        let name = name.into();
        let path_str = path.to_str().ok_or_else(|| {
            DataFusionError::Conversion("Invalid UTF-8 in path".to_string())
        })?;

        self.runtime.block_on(async {
            let ctx = &self.session;
            ctx.register_parquet(&name, path_str, ParquetReadOptions::default())
                .await?;
            Ok::<_, DataFusionError>(())
        })?;

        self.table_names.push(name);
        Ok(())
    }

    pub fn register_delta(&mut self, name: impl Into<String>, path: &Path) -> Result<()> {
        let name = name.into();
        let path_str = path.to_str().ok_or_else(|| {
            DataFusionError::Conversion("Invalid UTF-8 in path".to_string())
        })?;

        self.runtime.block_on(async {
            let delta_table = deltalake::open_table(path_str).await?;
            let ctx = &self.session;
            ctx.register_table(&name, Arc::new(delta_table))?;
            Ok::<_, DataFusionError>(())
        })?;

        self.table_names.push(name);
        Ok(())
    }

    pub fn register_sqlite(&mut self, path: &Path) -> Result<Vec<String>> {
        let provider = SqliteTableProvider::new(path)?;
        let table_names = provider.list_tables()?;
        let registered_tables = table_names.clone();

        for table_name in table_names {
            let table_provider = SqliteTableProvider::new_for_table(path, &table_name)?;
            self.session
                .register_table(&table_name, Arc::new(table_provider))?;
            self.table_names.push(table_name);
        }

        Ok(registered_tables)
    }

    pub fn execute_sql(&self, sql: &str) -> Result<Table> {
        let (schema, result) = self.runtime.block_on(async {
            let df = self.session.sql(sql).await?;
            let schema = df.schema().clone();
            let batches = df.collect().await?;
            Ok::<_, DataFusionError>((schema, batches))
        })?;

        // Handle empty results - create table with schema but no rows
        if result.is_empty() {
            use super::conversion::convert_schema;
            use crate::storage::table::Table;
            let arrow_schema = schema.to_owned().into();
            let table_schema = convert_schema(&arrow_schema)?;
            return Ok(Table::new("result", table_schema));
        }

        let table = record_batch_to_table("result", result)?;
        Ok(table)
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.table_names.clone()
    }

    pub fn table_count(&self) -> usize {
        self.table_names.len()
    }
}

impl Default for DataFusionContext {
    fn default() -> Self {
        Self::new().expect("Failed to create DataFusion context")
    }
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
    fn test_context_creation() {
        let ctx = DataFusionContext::new();
        assert!(ctx.is_ok());
    }

    #[test]
    fn test_register_csv() {
        let mut ctx = DataFusionContext::new().unwrap();
        let samples = get_samples_path();
        let users_csv = samples.join("users.csv");

        if users_csv.exists() {
            let result = ctx.register_csv("users", &users_csv);
            assert!(result.is_ok());
            assert_eq!(ctx.table_count(), 1);
        }
    }

    #[test]
    fn test_simple_query() {
        let mut ctx = DataFusionContext::new().unwrap();
        let samples = get_samples_path();
        let users_csv = samples.join("users.csv");

        if users_csv.exists() {
            ctx.register_csv("users", &users_csv).unwrap();
            let result = ctx.execute_sql("SELECT * FROM users LIMIT 5");
            assert!(result.is_ok());

            let table = result.unwrap();
            assert!(table.row_count() > 0);
        }
    }
}
