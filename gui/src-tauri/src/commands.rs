use std::sync::Arc;
use std::path::PathBuf;
use std::fs;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecentQuery {
    pub name: String,
    pub path: String,
    pub sql: String,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppConfig {
    pub recent_queries: Vec<RecentQuery>,
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

/// Get the knowhere home directory ($HOME/knowhere)
fn get_knowhere_home() -> Result<PathBuf, String> {
    let home = dirs::home_dir()
        .ok_or_else(|| "Could not determine home directory".to_string())?;
    Ok(home.join("knowhere"))
}

/// Get the queries directory ($HOME/knowhere/queries)
fn get_queries_dir() -> Result<PathBuf, String> {
    let knowhere_home = get_knowhere_home()?;
    Ok(knowhere_home.join("queries"))
}

/// Get the config file path ($HOME/knowhere/config.json)
fn get_config_path() -> Result<PathBuf, String> {
    let knowhere_home = get_knowhere_home()?;
    Ok(knowhere_home.join("config.json"))
}

/// Load app config from JSON file
fn load_config() -> AppConfig {
    let config_path = match get_config_path() {
        Ok(p) => p,
        Err(_) => return AppConfig::default(),
    };
    
    if !config_path.exists() {
        return AppConfig::default();
    }
    
    match fs::read_to_string(&config_path) {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(_) => AppConfig::default(),
    }
}

/// Save app config to JSON file
fn save_config(config: &AppConfig) -> Result<(), String> {
    let knowhere_home = get_knowhere_home()?;
    fs::create_dir_all(&knowhere_home).map_err(|e| e.to_string())?;
    
    let config_path = get_config_path()?;
    let json = serde_json::to_string_pretty(config).map_err(|e| e.to_string())?;
    fs::write(&config_path, json).map_err(|e| e.to_string())?;
    
    Ok(())
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

// ============== Data Loading Commands ==============

#[tauri::command]
pub fn load_path(path: String, state: State<'_, SharedState>) -> Result<Vec<String>, String> {
    let path_ref = std::path::Path::new(&path);

    let mut app_state = state.lock().map_err(|e| e.to_string())?;

    // Reuse the existing context so previously loaded tables are preserved.
    let mut loader = match app_state.context.take() {
        Some(ctx) => FileLoader::from_context(ctx),
        None => FileLoader::new().map_err(|e| e.to_string())?,
    };

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

    app_state.context = Some(ctx);

    Ok(tables)
}

#[tauri::command]
pub fn clear_session(state: State<'_, SharedState>) -> Result<(), String> {
    let mut app_state = state.lock().map_err(|e| e.to_string())?;
    app_state.context = None;
    Ok(())
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

// ============== Query Persistence Commands ==============

/// Get the default queries directory path
#[tauri::command]
pub fn get_queries_directory() -> Result<String, String> {
    let queries_dir = get_queries_dir()?;
    
    // Create directory if it doesn't exist
    fs::create_dir_all(&queries_dir).map_err(|e| e.to_string())?;
    
    Ok(queries_dir.to_string_lossy().to_string())
}

/// Save a query to a file
#[tauri::command]
pub fn save_query(path: String, sql: String, name: String) -> Result<(), String> {
    let path = PathBuf::from(&path);
    
    // Create parent directories if needed
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    
    // Write the SQL file
    fs::write(&path, &sql).map_err(|e| e.to_string())?;
    
    // Add to recent queries
    let mut config = load_config();
    
    // Remove existing entry for this path if present
    config.recent_queries.retain(|q| q.path != path.to_string_lossy());
    
    // Add new entry at the beginning
    config.recent_queries.insert(0, RecentQuery {
        name,
        path: path.to_string_lossy().to_string(),
        sql: sql.chars().take(200).collect(), // Store first 200 chars as preview
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0),
    });
    
    // Keep only last 20 recent queries
    config.recent_queries.truncate(20);
    
    save_config(&config)?;
    
    Ok(())
}

/// Load a query from a file
#[tauri::command]
pub fn load_query(path: String) -> Result<String, String> {
    let content = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    
    // Update recent queries
    let mut config = load_config();
    
    // Move this query to the top if it exists, otherwise add it
    if let Some(idx) = config.recent_queries.iter().position(|q| q.path == path) {
        let query = config.recent_queries.remove(idx);
        config.recent_queries.insert(0, query);
    } else {
        let name = PathBuf::from(&path)
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "Untitled".to_string());
        
        config.recent_queries.insert(0, RecentQuery {
            name,
            path: path.clone(),
            sql: content.chars().take(200).collect(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        });
    }
    
    config.recent_queries.truncate(20);
    save_config(&config)?;
    
    Ok(content)
}

/// Get recent queries
#[tauri::command]
pub fn get_recent_queries() -> Result<Vec<RecentQuery>, String> {
    let config = load_config();
    Ok(config.recent_queries)
}

/// Clear recent queries
#[tauri::command]
pub fn clear_recent_queries() -> Result<(), String> {
    let mut config = load_config();
    config.recent_queries.clear();
    save_config(&config)?;
    Ok(())
}
