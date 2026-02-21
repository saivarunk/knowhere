use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeListArray, LargeStringArray, ListArray,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, Utc};

use crate::storage::table::{Column, DataType, Row, Schema, Table, Value};

use super::error::{DataFusionError, Result};

pub fn record_batch_to_table(
    table_name: impl Into<String>,
    batches: Vec<RecordBatch>,
) -> Result<Table> {
    if batches.is_empty() {
        return Err(DataFusionError::Conversion(
            "No record batches to convert".to_string(),
        ));
    }

    let arrow_schema = batches[0].schema();
    let schema = convert_schema(&arrow_schema)?;
    let mut rows = Vec::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut values = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                let value = convert_array_value(array, row_idx)?;
                values.push(value);
            }
            rows.push(Row::new(values));
        }
    }

    Ok(Table::with_rows(table_name, schema, rows))
}

pub fn convert_schema(arrow_schema: &arrow::datatypes::Schema) -> Result<Schema> {
    let columns = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = convert_data_type(field.data_type());
            Column::new(field.name().clone(), data_type)
        })
        .collect();

    Ok(Schema::new(columns))
}

fn convert_data_type(arrow_type: &ArrowDataType) -> DataType {
    match arrow_type {
        ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64 => DataType::Integer,
        ArrowDataType::Float32 | ArrowDataType::Float64 => DataType::Float,
        ArrowDataType::Boolean => DataType::Boolean,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataType::String,
        ArrowDataType::Date32
        | ArrowDataType::Date64
        | ArrowDataType::Timestamp(_, _)
        | ArrowDataType::Time32(_)
        | ArrowDataType::Time64(_) => DataType::String, // Convert dates/timestamps to strings
        ArrowDataType::Null => DataType::Null,
        _ => DataType::String, // Default to string for unsupported types
    }
}

fn convert_array_value(array: &ArrayRef, index: usize) -> Result<Value> {
    if array.is_null(index) {
        return Ok(Value::Null);
    }

    let value = match array.data_type() {
        ArrowDataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Value::Integer(arr.value(index) as i64)
        }
        ArrowDataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Value::Integer(arr.value(index) as i64)
        }
        ArrowDataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::Integer(arr.value(index) as i64)
        }
        ArrowDataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::Integer(arr.value(index))
        }
        ArrowDataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Value::Integer(arr.value(index) as i64)
        }
        ArrowDataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Value::Integer(arr.value(index) as i64)
        }
        ArrowDataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Value::Integer(arr.value(index) as i64)
        }
        ArrowDataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::Integer(arr.value(index) as i64)
        }
        ArrowDataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Value::Float(arr.value(index) as f64)
        }
        ArrowDataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Value::Float(arr.value(index))
        }
        ArrowDataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Boolean(arr.value(index))
        }
        ArrowDataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(arr.value(index).to_string())
        }
        // DataFusion's JSON reader infers strings as LargeUtf8
        ArrowDataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Value::String(arr.value(index).to_string())
        }
        // Nested JSON objects → Struct; render as a compact JSON object string
        ArrowDataType::Struct(fields) => {
            let arr = array.as_any().downcast_ref::<StructArray>().unwrap();
            let fields = fields.clone();
            let parts: Vec<String> = fields
                .iter()
                .enumerate()
                .filter_map(|(i, field)| {
                    let child = arr.column(i);
                    convert_array_value(child, index).ok().map(|v| {
                        let rendered = match &v {
                            Value::String(s) => format!("\"{}\"", s),
                            _ => v.to_string(),
                        };
                        format!("\"{}\":{}", field.name(), rendered)
                    })
                })
                .collect();
            Value::String(format!("{{{}}}", parts.join(",")))
        }
        // Nested JSON arrays → List; render as a compact JSON array string
        ArrowDataType::List(_) => {
            let arr = array.as_any().downcast_ref::<ListArray>().unwrap();
            let slice = arr.value(index);
            let parts: Result<Vec<String>> = (0..slice.len())
                .map(|i| {
                    convert_array_value(&slice, i).map(|v| match &v {
                        Value::String(s) => format!("\"{}\"", s),
                        _ => v.to_string(),
                    })
                })
                .collect();
            Value::String(format!("[{}]", parts?.join(",")))
        }
        ArrowDataType::LargeList(_) => {
            let arr = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let slice = arr.value(index);
            let parts: Result<Vec<String>> = (0..slice.len())
                .map(|i| {
                    convert_array_value(&slice, i).map(|v| match &v {
                        Value::String(s) => format!("\"{}\"", s),
                        _ => v.to_string(),
                    })
                })
                .collect();
            Value::String(format!("[{}]", parts?.join(",")))
        }
        ArrowDataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(index);
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(days as i64))
                .unwrap();
            Value::String(date.format("%Y-%m-%d").to_string())
        }
        ArrowDataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let millis = arr.value(index);
            let datetime = DateTime::<Utc>::from_timestamp_millis(millis).unwrap();
            Value::String(datetime.format("%Y-%m-%d").to_string())
        }
        ArrowDataType::Timestamp(unit, _) => {
            let timestamp_str = match unit {
                TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    let seconds = arr.value(index);
                    let datetime = DateTime::<Utc>::from_timestamp(seconds, 0).unwrap();
                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                }
                TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    let millis = arr.value(index);
                    let datetime = DateTime::<Utc>::from_timestamp_millis(millis).unwrap();
                    datetime.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
                }
                TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    let micros = arr.value(index);
                    let datetime = DateTime::<Utc>::from_timestamp_micros(micros).unwrap();
                    datetime.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
                }
                TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    let nanos = arr.value(index);
                    let datetime = DateTime::<Utc>::from_timestamp_nanos(nanos);
                    datetime.format("%Y-%m-%d %H:%M:%S%.9f").to_string()
                }
            };
            Value::String(timestamp_str)
        }
        ArrowDataType::Null => Value::Null,
        _ => {
            // For unsupported types, convert to string representation
            Value::String(format!("{:?}", array))
        }
    };

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema as ArrowSchema};
    use std::sync::Arc;

    #[test]
    fn test_convert_simple_batch() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        let table = record_batch_to_table("test", vec![batch]).unwrap();

        assert_eq!(table.row_count(), 3);
        assert_eq!(table.column_count(), 2);
        assert_eq!(table.schema.columns[0].name, "id");
        assert_eq!(table.schema.columns[1].name, "name");
    }

    #[test]
    fn test_convert_with_nulls() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            ArrowDataType::Int64,
            true,
        )]));

        let array = Int64Array::from(vec![Some(1), None, Some(3)]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        let table = record_batch_to_table("test", vec![batch]).unwrap();

        assert_eq!(table.rows[0].values[0], Value::Integer(1));
        assert_eq!(table.rows[1].values[0], Value::Null);
        assert_eq!(table.rows[2].values[0], Value::Integer(3));
    }
}
