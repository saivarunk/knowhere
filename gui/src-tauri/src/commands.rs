use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tauri::State;
use knowhere::{Table, Schema, DataFusionContext, FileLoader};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
}

fn schema_to_columns(schema: &Schema) -> Vec<ColumnInfo> {
    schema.columns.iter().map(|col| ColumnInfo {
        name: col.name.clone(),
        data_type: format!("{:?}", col.data_type),
    }).collect()
}

fn value_to_json(value: &knowhere::Value) -> serde_json::Value {
    match value {
        knowhere::Value::Null => serde_json::Value::Null,
        knowhere::Value::Boolean(b) => serde_json::Value::Bool(*b),
        knowhere::Value::Integer(n) => serde_json::json!(n),
        knowhere::Value::Float(n) => serde_json::json!(n),
        knowhere::Value::String(s) => serde_json::Value::String(s.clone()),
    }
}

fn table_to_result(table: &Table) -> QueryResult {
    let columns = schema_to_columns(&table.schema);
    let rows: Vec<Vec<serde_json::Value>> = table.rows.iter().map(|row| {
        row.values.iter().map(value_to_json).collect()
    }).collect();
    let row_count = rows.len();
    
    QueryResult { columns, rows, row_count }
}

pub struct AppState {
    pub context: Option<DataFusionContext>,
}

impl AppState {
    pub fn new() -> Self {
        Self { context: None }
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

pub type SharedState = Arc<std::sync::Mutex<AppState>>;

#[tauri::command]
pub fn load_path(path: String, state: State<'_, SharedState>) -> Result<Vec<String>, String> {
    // Run everything synchronously - DataFusionContext has its own runtime
    let path_ref = std::path::Path::new(&path);
    let mut loader = FileLoader::new().map_err(|e| e.to_string())?;

    if path_ref.is_file() {
        loader.load_file(path_ref).map_err(|e| e.to_string())?;
    } else if path_ref.is_dir() {
        loader.load_directory(path_ref).map_err(|e| e.to_string())?;
    } else {
        return Err(format!("Path does not exist: {}", path_ref.display()));
    }

    let ctx = loader.into_context();
    let tables = ctx.list_tables();
    
    if tables.is_empty() {
        return Err("No valid data files found".to_string());
    }

    let mut app_state = state.lock().map_err(|e| e.to_string())?;
    app_state.context = Some(ctx);
    
    Ok(tables)
}

#[tauri::command]
pub fn execute_sql(sql: String, state: State<'_, SharedState>) -> Result<QueryResult, String> {
    let app_state = state.lock().map_err(|e| e.to_string())?;
    
    let ctx = app_state.context.as_ref()
        .ok_or_else(|| "No data loaded. Please open a file or folder first.".to_string())?;
    
    let table = ctx.execute_sql(&sql).map_err(|e| e.to_string())?;
    Ok(table_to_result(&table))
}

#[tauri::command]
pub fn list_tables(state: State<'_, SharedState>) -> Result<Vec<String>, String> {
    let app_state = state.lock().map_err(|e| e.to_string())?;
    
    let ctx = app_state.context.as_ref()
        .ok_or_else(|| "No data loaded.".to_string())?;
    
    Ok(ctx.list_tables())
}

#[tauri::command]
pub fn get_schema(table_name: String, state: State<'_, SharedState>) -> Result<Vec<ColumnInfo>, String> {
    let app_state = state.lock().map_err(|e| e.to_string())?;
    
    let ctx = app_state.context.as_ref()
        .ok_or_else(|| "No data loaded.".to_string())?;
    
    let schema = ctx.get_table_schema(&table_name)
        .ok_or_else(|| format!("Table '{}' not found.", table_name))?;
    
    Ok(schema_to_columns(&schema))
}

#[tauri::command]
pub fn get_table_preview(table_name: String, limit: i32, state: State<'_, SharedState>) -> Result<QueryResult, String> {
    let sql = format!("SELECT * FROM \"{}\" LIMIT {}", table_name, limit);
    let app_state = state.lock().map_err(|e| e.to_string())?;
    
    let ctx = app_state.context.as_ref()
        .ok_or_else(|| "No data loaded. Please open a file or folder first.".to_string())?;
    
    let table = ctx.execute_sql(&sql).map_err(|e| e.to_string())?;
    Ok(table_to_result(&table))
}
