import { invoke } from '@tauri-apps/api/core';
import type { ColumnInfo, QueryResult, RecentQuery } from './types';

export async function loadPath(path: string): Promise<string[]> {
    return invoke<string[]>('load_path', { path });
}

export async function executeSql(sql: string): Promise<QueryResult> {
    return invoke<QueryResult>('execute_sql', { sql });
}

export async function listTables(): Promise<string[]> {
    return invoke<string[]>('list_tables');
}

export async function getSchema(tableName: string): Promise<ColumnInfo[]> {
    return invoke<ColumnInfo[]>('get_schema', { tableName });
}

export async function getTablePreview(tableName: string, limit: number = 100): Promise<QueryResult> {
    return invoke<QueryResult>('get_table_preview', { tableName, limit });
}

// Query persistence APIs

export async function getQueriesDirectory(): Promise<string> {
    return invoke<string>('get_queries_directory');
}

export async function saveQuery(path: string, sql: string, name: string): Promise<void> {
    return invoke<void>('save_query', { path, sql, name });
}

export async function loadQuery(path: string): Promise<string> {
    return invoke<string>('load_query', { path });
}

export async function getRecentQueries(): Promise<RecentQuery[]> {
    return invoke<RecentQuery[]>('get_recent_queries');
}

export async function clearRecentQueries(): Promise<void> {
    return invoke<void>('clear_recent_queries');
}
