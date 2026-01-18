import { invoke } from '@tauri-apps/api/core';
import type { ColumnInfo, QueryResult } from './types';

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
