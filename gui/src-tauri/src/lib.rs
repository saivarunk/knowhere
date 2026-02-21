mod commands;

use std::sync::{Arc, Mutex};
use commands::{AppState, SharedState};

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .manage(Arc::new(Mutex::new(AppState::new())) as SharedState)
        .invoke_handler(tauri::generate_handler![
            commands::load_path,
            commands::execute_sql,
            commands::list_tables,
            commands::get_schema,
            commands::get_table_preview,
            commands::get_queries_directory,
            commands::save_query,
            commands::load_query,
            commands::get_recent_queries,
            commands::clear_recent_queries,
            commands::clear_session,
        ])
        .setup(|app| {
            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
