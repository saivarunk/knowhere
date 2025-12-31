use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use super::table::{Column, DataType, Row, Schema, Table, Value};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CsvError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parse error at line {line}: {message}")]
    Parse { line: usize, message: String },
    #[error("Empty CSV file")]
    EmptyFile,
}

pub struct CsvReader {
    delimiter: char,
    has_header: bool,
}

impl Default for CsvReader {
    fn default() -> Self {
        Self::new()
    }
}

impl CsvReader {
    pub fn new() -> Self {
        Self {
            delimiter: ',',
            has_header: true,
        }
    }

    pub fn with_delimiter(mut self, delimiter: char) -> Self {
        self.delimiter = delimiter;
        self
    }

    pub fn with_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    pub fn read_file(&self, path: &Path) -> Result<Table, CsvError> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let table_name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("table")
            .to_string();

        self.read_from_reader(reader, &table_name)
    }

    pub fn read_from_reader<R: BufRead>(&self, reader: R, table_name: &str) -> Result<Table, CsvError> {
        let mut lines = reader.lines().enumerate();

        // Read header or generate column names
        let first_line = lines
            .next()
            .ok_or(CsvError::EmptyFile)?
            .1?;

        let first_row = self.parse_line(&first_line).map_err(|e| CsvError::Parse {
            line: 1,
            message: e,
        })?;

        // Read all data rows
        let mut raw_rows: Vec<Vec<String>> = Vec::new();

        let headers: Vec<String> = if self.has_header {
            first_row
        } else {
            let headers = (0..first_row.len())
                .map(|i| format!("column{}", i + 1))
                .collect();
            raw_rows.push(first_row);
            headers
        };

        for (line_num, line_result) in lines {
            let line = line_result?;
            if line.trim().is_empty() {
                continue;
            }
            let row = self.parse_line(&line).map_err(|e| CsvError::Parse {
                line: line_num + 1,
                message: e,
            })?;
            raw_rows.push(row);
        }

        // Infer types from data
        let types = self.infer_types(&raw_rows, headers.len());

        // Build schema
        let columns: Vec<Column> = headers
            .iter()
            .zip(types.iter())
            .map(|(name, dtype)| Column::new(name.clone(), dtype.clone()))
            .collect();
        let schema = Schema::new(columns);

        // Convert raw strings to typed values
        let rows: Vec<Row> = raw_rows
            .iter()
            .map(|raw_row| {
                let values: Vec<Value> = raw_row
                    .iter()
                    .zip(types.iter())
                    .map(|(s, dtype)| self.parse_value(s, dtype))
                    .collect();
                Row::new(values)
            })
            .collect();

        Ok(Table::with_rows(table_name, schema, rows))
    }

    fn parse_line(&self, line: &str) -> Result<Vec<String>, String> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(c) = chars.next() {
            if in_quotes {
                if c == '"' {
                    // Check for escaped quote
                    if chars.peek() == Some(&'"') {
                        current_field.push('"');
                        chars.next();
                    } else {
                        in_quotes = false;
                    }
                } else {
                    current_field.push(c);
                }
            } else if c == '"' {
                in_quotes = true;
            } else if c == self.delimiter {
                fields.push(current_field.trim().to_string());
                current_field = String::new();
            } else {
                current_field.push(c);
            }
        }

        if in_quotes {
            return Err("Unclosed quote".to_string());
        }

        fields.push(current_field.trim().to_string());
        Ok(fields)
    }

    fn infer_types(&self, rows: &[Vec<String>], num_columns: usize) -> Vec<DataType> {
        let mut types = vec![DataType::Null; num_columns];

        for row in rows {
            for (i, value) in row.iter().enumerate() {
                if i >= num_columns {
                    break;
                }
                let inferred = self.infer_single_type(value);
                types[i] = self.merge_types(&types[i], &inferred);
            }
        }

        // Convert remaining Null types to String
        for dtype in &mut types {
            if *dtype == DataType::Null {
                *dtype = DataType::String;
            }
        }

        types
    }

    fn infer_single_type(&self, value: &str) -> DataType {
        let value = value.trim();

        if value.is_empty() || value.eq_ignore_ascii_case("null") || value.eq_ignore_ascii_case("na") || value.eq_ignore_ascii_case("n/a") {
            return DataType::Null;
        }

        // Try boolean
        if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
            return DataType::Boolean;
        }

        // Try integer
        if value.parse::<i64>().is_ok() {
            return DataType::Integer;
        }

        // Try float
        if value.parse::<f64>().is_ok() {
            return DataType::Float;
        }

        DataType::String
    }

    fn merge_types(&self, current: &DataType, new: &DataType) -> DataType {
        match (current, new) {
            (DataType::Null, other) | (other, DataType::Null) => other.clone(),
            (DataType::Integer, DataType::Float) | (DataType::Float, DataType::Integer) => DataType::Float,
            (a, b) if a == b => a.clone(),
            _ => DataType::String, // Fall back to string if types conflict
        }
    }

    fn parse_value(&self, value: &str, dtype: &DataType) -> Value {
        let value = value.trim();

        if value.is_empty() || value.eq_ignore_ascii_case("null") || value.eq_ignore_ascii_case("na") || value.eq_ignore_ascii_case("n/a") {
            return Value::Null;
        }

        match dtype {
            DataType::Integer => value.parse::<i64>().map(Value::Integer).unwrap_or(Value::Null),
            DataType::Float => value.parse::<f64>().map(Value::Float).unwrap_or(Value::Null),
            DataType::Boolean => {
                if value.eq_ignore_ascii_case("true") {
                    Value::Boolean(true)
                } else if value.eq_ignore_ascii_case("false") {
                    Value::Boolean(false)
                } else {
                    Value::Null
                }
            }
            DataType::String => Value::String(value.to_string()),
            DataType::Null => Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_simple_csv() {
        let csv_data = "id,name,age\n1,Alice,30\n2,Bob,25";
        let reader = CsvReader::new();
        let cursor = Cursor::new(csv_data);
        let table = reader.read_from_reader(BufReader::new(cursor), "test").unwrap();

        assert_eq!(table.name, "test");
        assert_eq!(table.row_count(), 2);
        assert_eq!(table.column_count(), 3);
    }

    #[test]
    fn test_quoted_fields() {
        let csv_data = "name,description\n\"John Doe\",\"A \"\"quoted\"\" value\"";
        let reader = CsvReader::new();
        let cursor = Cursor::new(csv_data);
        let table = reader.read_from_reader(BufReader::new(cursor), "test").unwrap();

        assert_eq!(table.row_count(), 1);
        if let Value::String(s) = &table.rows[0].values[0] {
            assert_eq!(s, "John Doe");
        }
        if let Value::String(s) = &table.rows[0].values[1] {
            assert_eq!(s, "A \"quoted\" value");
        }
    }

    #[test]
    fn test_type_inference() {
        let csv_data = "int_col,float_col,bool_col,str_col\n1,1.5,true,hello\n2,2.5,false,world";
        let reader = CsvReader::new();
        let cursor = Cursor::new(csv_data);
        let table = reader.read_from_reader(BufReader::new(cursor), "test").unwrap();

        assert_eq!(table.schema.columns[0].data_type, DataType::Integer);
        assert_eq!(table.schema.columns[1].data_type, DataType::Float);
        assert_eq!(table.schema.columns[2].data_type, DataType::Boolean);
        assert_eq!(table.schema.columns[3].data_type, DataType::String);
    }

    #[test]
    fn test_null_handling() {
        let csv_data = "a,b\n1,\n,2\nnull,NA";
        let reader = CsvReader::new();
        let cursor = Cursor::new(csv_data);
        let table = reader.read_from_reader(BufReader::new(cursor), "test").unwrap();

        assert!(table.rows[0].values[1].is_null());
        assert!(table.rows[1].values[0].is_null());
        assert!(table.rows[2].values[0].is_null());
        assert!(table.rows[2].values[1].is_null());
    }

    #[test]
    fn test_custom_delimiter() {
        let csv_data = "a;b;c\n1;2;3";
        let reader = CsvReader::new().with_delimiter(';');
        let cursor = Cursor::new(csv_data);
        let table = reader.read_from_reader(BufReader::new(cursor), "test").unwrap();

        assert_eq!(table.column_count(), 3);
    }
}
