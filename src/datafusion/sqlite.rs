use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError as DFError;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::memory::MemorySourceConfig;
use rusqlite::Connection;
use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::error::{DataFusionError, Result};

#[derive(Debug)]
pub struct SqliteTableProvider {
    db_path: PathBuf,
    table_name: String,
    schema: Arc<ArrowSchema>,
}

impl SqliteTableProvider {
    pub fn new(db_path: &Path) -> Result<Self> {
        // For initialization only, we don't need a specific table
        let conn = Connection::open(db_path)?;

        // Get the first table name
        let table_name: String = conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| DataFusionError::SqliteTableNotFound("No tables found".to_string()))?;

        let schema = Self::get_schema(&conn, &table_name)?;

        Ok(Self {
            db_path: db_path.to_path_buf(),
            table_name,
            schema: Arc::new(schema),
        })
    }

    pub fn new_for_table(db_path: &Path, table_name: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        let schema = Self::get_schema(&conn, table_name)?;

        Ok(Self {
            db_path: db_path.to_path_buf(),
            table_name: table_name.to_string(),
            schema: Arc::new(schema),
        })
    }

    pub fn list_tables(&self) -> Result<Vec<String>> {
        let conn = Connection::open(&self.db_path)?;
        let mut stmt = conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )?;

        let tables = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;

        Ok(tables)
    }

    fn get_schema(conn: &Connection, table_name: &str) -> Result<ArrowSchema> {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table_name))?;

        let columns: Vec<Field> = stmt
            .query_map([], |row| {
                let name: String = row.get(1)?;
                let type_name: String = row.get(2)?;
                let not_null: i64 = row.get(3)?;

                let arrow_type = match type_name.to_uppercase().as_str() {
                    t if t.contains("INT") => ArrowDataType::Int64,
                    t if t.contains("REAL") || t.contains("FLOAT") || t.contains("DOUBLE") => {
                        ArrowDataType::Float64
                    }
                    t if t.contains("BLOB") => ArrowDataType::Binary,
                    t if t.contains("BOOL") => ArrowDataType::Boolean,
                    _ => ArrowDataType::Utf8,
                };

                Ok(Field::new(name, arrow_type, not_null == 0))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(ArrowSchema::new(columns))
    }

    fn read_table_data(&self) -> Result<Vec<RecordBatch>> {
        let conn = Connection::open(&self.db_path)?;
        let query = format!("SELECT * FROM {}", self.table_name);
        let mut stmt = conn.prepare(&query)?;

        let _column_count = stmt.column_count();
        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();

        for field in self.schema.fields() {
            let builder: Box<dyn ArrayBuilder> = match field.data_type() {
                ArrowDataType::Int64 => Box::new(Int64Builder::new()),
                ArrowDataType::Float64 => Box::new(Float64Builder::new()),
                ArrowDataType::Boolean => Box::new(BooleanBuilder::new()),
                ArrowDataType::Binary => Box::new(BinaryBuilder::new()),
                ArrowDataType::Utf8 => Box::new(StringBuilder::new()),
                _ => Box::new(StringBuilder::new()),
            };
            builders.push(builder);
        }

        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            for (i, builder) in builders.iter_mut().enumerate() {
                if let ArrowDataType::Int64 = self.schema.field(i).data_type() {
                    let b = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
                    match row.get_ref(i)? {
                        rusqlite::types::ValueRef::Null => b.append_null(),
                        rusqlite::types::ValueRef::Integer(v) => b.append_value(v),
                        _ => b.append_null(),
                    }
                } else if let ArrowDataType::Float64 = self.schema.field(i).data_type() {
                    let b = builder
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .unwrap();
                    match row.get_ref(i)? {
                        rusqlite::types::ValueRef::Null => b.append_null(),
                        rusqlite::types::ValueRef::Real(v) => b.append_value(v),
                        rusqlite::types::ValueRef::Integer(v) => b.append_value(v as f64),
                        _ => b.append_null(),
                    }
                } else if let ArrowDataType::Boolean = self.schema.field(i).data_type() {
                    let b = builder
                        .as_any_mut()
                        .downcast_mut::<BooleanBuilder>()
                        .unwrap();
                    match row.get_ref(i)? {
                        rusqlite::types::ValueRef::Null => b.append_null(),
                        rusqlite::types::ValueRef::Integer(v) => b.append_value(v != 0),
                        _ => b.append_null(),
                    }
                } else if let ArrowDataType::Binary = self.schema.field(i).data_type() {
                    let b = builder
                        .as_any_mut()
                        .downcast_mut::<BinaryBuilder>()
                        .unwrap();
                    match row.get_ref(i)? {
                        rusqlite::types::ValueRef::Null => b.append_null(),
                        rusqlite::types::ValueRef::Blob(v) => b.append_value(v),
                        _ => b.append_null(),
                    }
                } else {
                    let b = builder
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .unwrap();
                    match row.get_ref(i)? {
                        rusqlite::types::ValueRef::Null => b.append_null(),
                        rusqlite::types::ValueRef::Integer(v) => b.append_value(v.to_string()),
                        rusqlite::types::ValueRef::Real(v) => b.append_value(v.to_string()),
                        rusqlite::types::ValueRef::Text(v) => {
                            b.append_value(std::str::from_utf8(v).unwrap())
                        }
                        rusqlite::types::ValueRef::Blob(_) => b.append_value("[BLOB]"),
                    }
                }
            }
        }

        let arrays: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();

        let batch =
            RecordBatch::try_new(self.schema.clone(), arrays).map_err(DataFusionError::Arrow)?;

        Ok(vec![batch])
    }
}

#[async_trait::async_trait]
impl TableProvider for SqliteTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let batches = self
            .read_table_data()
            .map_err(|e| DFError::External(Box::new(e)))?;

        let exec =
            MemorySourceConfig::try_new_exec(&[batches], self.schema.clone(), projection.cloned())?;

        Ok(exec)
    }
}

trait ArrayBuilder {
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn finish(&mut self) -> ArrayRef;
}

impl ArrayBuilder for Int64Builder {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl ArrayBuilder for Float64Builder {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl ArrayBuilder for BooleanBuilder {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl ArrayBuilder for StringBuilder {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl ArrayBuilder for BinaryBuilder {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}
