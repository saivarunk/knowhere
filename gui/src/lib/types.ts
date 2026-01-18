export interface ColumnInfo {
    name: string;
    data_type: string;
}

export interface QueryResult {
    columns: ColumnInfo[];
    rows: (string | number | boolean | null)[][];
    row_count: number;
}

export interface TableInfo {
    name: string;
    columns: ColumnInfo[];
}

export type Theme = 'light' | 'dark';
