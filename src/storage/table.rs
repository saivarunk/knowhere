use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Float,
    String,
    Boolean,
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Integer(_) => DataType::Integer,
            Value::Float(_) => DataType::Float,
            Value::String(_) => DataType::String,
            Value::Boolean(_) => DataType::Boolean,
            Value::Null => DataType::Null,
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Boolean(b) => *b,
            Value::Integer(i) => *i != 0,
            Value::Float(f) => *f != 0.0,
            Value::String(s) => !s.is_empty(),
            Value::Null => false,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::String(s) => write!(f, "{}", s),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
            (Value::Null, _) => Some(std::cmp::Ordering::Less),
            (_, Value::Null) => Some(std::cmp::Ordering::Greater),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<Column>,
    column_index: HashMap<String, usize>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        let column_index = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.to_lowercase(), i))
            .collect();
        Self {
            columns,
            column_index,
        }
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.column_index.get(&name.to_lowercase()).copied()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub schema: Schema,
    pub rows: Vec<Row>,
}

impl Table {
    pub fn new(name: impl Into<String>, schema: Schema) -> Self {
        Self {
            name: name.into(),
            schema,
            rows: Vec::new(),
        }
    }

    pub fn with_rows(name: impl Into<String>, schema: Schema, rows: Vec<Row>) -> Self {
        Self {
            name: name.into(),
            schema,
            rows,
        }
    }

    pub fn add_row(&mut self, row: Row) {
        self.rows.push(row);
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn column_count(&self) -> usize {
        self.schema.column_count()
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.schema.column_index(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_comparison() {
        assert!(Value::Integer(1) < Value::Integer(2));
        assert!(Value::Float(1.5) < Value::Float(2.5));
        assert!(Value::String("a".to_string()) < Value::String("b".to_string()));
    }

    #[test]
    fn test_schema_column_index() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
        ]);
        assert_eq!(schema.column_index("id"), Some(0));
        assert_eq!(schema.column_index("name"), Some(1));
        assert_eq!(schema.column_index("ID"), Some(0)); // case insensitive
        assert_eq!(schema.column_index("unknown"), None);
    }

    #[test]
    fn test_table_operations() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("value", DataType::String),
        ]);
        let mut table = Table::new("test", schema);

        table.add_row(Row::new(vec![
            Value::Integer(1),
            Value::String("one".to_string()),
        ]));
        table.add_row(Row::new(vec![
            Value::Integer(2),
            Value::String("two".to_string()),
        ]));

        assert_eq!(table.row_count(), 2);
        assert_eq!(table.column_count(), 2);
    }
}
