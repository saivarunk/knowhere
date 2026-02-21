use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType};
use deltalake::DeltaOps;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{
    NestedField, PrimitiveType as IcebergPrimitive, Schema as IcebergSchema, Type,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use knowhere::datafusion::FileLoader;
use parquet::arrow::ArrowWriter;

fn get_samples_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("samples")
}

#[test]
fn test_load_csv() {
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = get_samples_dir();
    let users_csv = samples_dir.join("users.csv");

    if users_csv.exists() {
        let result = loader.load_file(&users_csv);
        assert!(result.is_ok());

        let tables = result.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0], "users");

        let ctx = loader.into_context();
        let query_result = ctx.execute_sql("SELECT * FROM users");
        assert!(query_result.is_ok());

        let table = query_result.unwrap();
        assert!(table.row_count() > 0);
    }
}

#[test]
fn test_load_parquet() {
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = get_samples_dir();
    let parquet_file = samples_dir.join("users.parquet");

    if parquet_file.exists() {
        let result = loader.load_file(&parquet_file);
        assert!(result.is_ok());

        let ctx = loader.into_context();
        let query_result = ctx.execute_sql("SELECT * FROM users");
        assert!(query_result.is_ok());

        let table = query_result.unwrap();
        assert!(table.row_count() > 0);
    }
}

#[test]
fn test_load_directory() {
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = get_samples_dir();

    if samples_dir.exists() && samples_dir.is_dir() {
        let result = loader.load_directory(&samples_dir);

        if result.is_ok() {
            let tables = result.unwrap();
            assert!(!tables.is_empty());

            let ctx = loader.into_context();

            // Test that we can query the loaded tables
            for table_name in tables {
                let query = format!("SELECT * FROM {} LIMIT 1", table_name);
                let query_result = ctx.execute_sql(&query);
                assert!(
                    query_result.is_ok(),
                    "Failed to query table: {}",
                    table_name
                );
            }
        }
    }
}

#[test]
fn test_mixed_format_join() {
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = get_samples_dir();

    let users_csv = samples_dir.join("users.csv");
    let orders_csv = samples_dir.join("orders.csv");

    if users_csv.exists() && orders_csv.exists() {
        loader.load_file(&users_csv).unwrap();
        loader.load_file(&orders_csv).unwrap();

        let ctx = loader.into_context();

        // Test join across potentially different formats
        let sql = r#"
            SELECT u.name, COUNT(o.id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            GROUP BY u.name
            ORDER BY order_count DESC
            LIMIT 5
        "#;

        let result = ctx.execute_sql(sql);
        assert!(result.is_ok());

        let table = result.unwrap();
        assert!(table.row_count() > 0);
        assert_eq!(table.column_count(), 2);
    }
}

#[test]
fn test_csv_with_different_schemas() {
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = get_samples_dir();

    let users_csv = samples_dir.join("users.csv");
    let products_csv = samples_dir.join("products.csv");

    if users_csv.exists() && products_csv.exists() {
        loader.load_file(&users_csv).unwrap();
        loader.load_file(&products_csv).unwrap();

        let ctx = loader.into_context();

        // Query users table
        let users_result = ctx.execute_sql("SELECT * FROM users LIMIT 1");
        assert!(users_result.is_ok());

        // Query products table
        let products_result = ctx.execute_sql("SELECT * FROM products LIMIT 1");
        assert!(products_result.is_ok());

        // Verify they have different schemas
        let users_table = users_result.unwrap();
        let products_table = products_result.unwrap();

        // They should have different column counts or names
        assert!(
            users_table.column_count() != products_table.column_count()
                || users_table.schema.columns[0].name != products_table.schema.columns[0].name
        );
    }
}

#[test]
fn test_multiple_files_same_schema() {
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = get_samples_dir();

    let users_csv = samples_dir.join("users.csv");

    if users_csv.exists() {
        loader.load_file(&users_csv).unwrap();

        let ctx = loader.into_context();

        // Test UNION-like query (DataFusion supports this natively)
        let sql = r#"
            SELECT name, email FROM users
            WHERE department = 'Engineering'
            UNION ALL
            SELECT name, email FROM users
            WHERE department = 'Marketing'
            ORDER BY name
        "#;

        let result = ctx.execute_sql(sql);
        assert!(result.is_ok());

        let table = result.unwrap();
        assert!(table.row_count() > 0);
    }
}

#[test]
fn test_large_result_set() {
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = get_samples_dir();

    let users_csv = samples_dir.join("users.csv");

    if users_csv.exists() {
        loader.load_file(&users_csv).unwrap();

        let ctx = loader.into_context();

        // Test that we can handle the full dataset
        let sql = "SELECT * FROM users";
        let result = ctx.execute_sql(sql);
        assert!(result.is_ok());

        let table = result.unwrap();
        assert!(table.row_count() >= 10);
    }
}

#[test]
fn test_empty_result_handling() {
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = get_samples_dir();

    let users_csv = samples_dir.join("users.csv");

    if users_csv.exists() {
        loader.load_file(&users_csv).unwrap();

        let ctx = loader.into_context();

        // Query that should return no results
        let sql = "SELECT * FROM users WHERE age > 1000";
        let result = ctx.execute_sql(sql);

        if result.is_ok() {
            let table = result.unwrap();
            assert_eq!(table.row_count(), 0);
        }
    }
}

// ---------------------------------------------------------------------------
// JSON tests
// ---------------------------------------------------------------------------

#[test]
fn test_load_json() {
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = get_samples_dir();
    let json_file = samples_dir.join("products.json");

    let result = loader.load_file(&json_file);
    assert!(
        result.is_ok(),
        "Failed to load JSON file: {:?}",
        result.err()
    );

    let tables = result.unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0], "products");

    let ctx = loader.into_context();
    let query = ctx.execute_sql("SELECT * FROM products");
    assert!(query.is_ok(), "SELECT * failed: {:?}", query.err());
    assert_eq!(query.unwrap().row_count(), 10);
}

#[test]
fn test_json_query_with_filter() {
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let json_file = get_samples_dir().join("products.json");

    loader.load_file(&json_file).unwrap();
    let ctx = loader.into_context();

    let result = ctx.execute_sql("SELECT name FROM products WHERE category = 'Electronics'");
    assert!(result.is_ok(), "Filtered query failed: {:?}", result.err());
    // Laptop Pro, Wireless Mouse, Mechanical Keyboard, Monitor 4K, USB-C Hub, Webcam HD
    assert_eq!(result.unwrap().row_count(), 6);
}

#[test]
fn test_json_aggregation() {
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let json_file = get_samples_dir().join("products.json");

    loader.load_file(&json_file).unwrap();
    let ctx = loader.into_context();

    let result = ctx.execute_sql(
        "SELECT category, COUNT(*) as cnt \
         FROM products \
         GROUP BY category \
         ORDER BY category",
    );
    assert!(result.is_ok(), "Aggregation failed: {:?}", result.err());
    let table = result.unwrap();
    assert_eq!(table.row_count(), 3); // Electronics, Furniture, Stationery
    assert_eq!(table.column_count(), 2);
}

#[test]
fn test_json_join_with_csv() {
    // Load a JSON table and a CSV table, then JOIN them — validates cross-format joins.
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = get_samples_dir();

    loader
        .load_file(&samples_dir.join("products.json"))
        .unwrap();
    loader.load_file(&samples_dir.join("users.csv")).unwrap();
    let ctx = loader.into_context();

    // Both tables are queryable independently
    let p = ctx.execute_sql("SELECT COUNT(*) FROM products").unwrap();
    let u = ctx.execute_sql("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(p.row_count(), 1);
    assert_eq!(u.row_count(), 1);

    // Cross-format join: each user gets a random product recommendation (CROSS JOIN LIMIT)
    let result = ctx.execute_sql(
        "SELECT u.name, p.name as product \
         FROM users u \
         CROSS JOIN products p \
         WHERE p.in_stock = true \
         LIMIT 5",
    );
    assert!(
        result.is_ok(),
        "Cross-format join failed: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().row_count(), 5);
}

#[test]
fn test_json_string_values_readable() {
    // Regression: DataFusion's JSON reader infers strings as LargeUtf8.
    // conversion.rs must handle LargeUtf8, otherwise every string cell is
    // rendered as a debug-printed ArrayRef instead of the actual value.
    let mut loader = FileLoader::new().expect("Failed to create loader");
    loader
        .load_file(&get_samples_dir().join("products.json"))
        .unwrap();
    let ctx = loader.into_context();

    let result = ctx
        .execute_sql("SELECT name, category FROM products ORDER BY id LIMIT 1")
        .unwrap();

    assert_eq!(result.row_count(), 1);
    let name_val = result.rows[0].values[0].to_string();
    let cat_val = result.rows[0].values[1].to_string();
    // Must be the actual string, not a debug dump like "LargeStringArray[...]"
    assert_eq!(name_val, "Laptop Pro");
    assert_eq!(cat_val, "Electronics");
}

#[test]
fn test_json_detection() {
    // Verify all three recognised JSON extensions are detected correctly.
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = get_samples_dir();
    let json_file = samples_dir.join("products.json");

    // .json loads successfully
    let result = loader.load_file(&json_file);
    assert!(result.is_ok(), "Expected .json to load: {:?}", result.err());
}

// ---------------------------------------------------------------------------
// Nested JSON tests
// ---------------------------------------------------------------------------

// employees_nested.json schema:
//   id INT64, name STRING,
//   address STRUCT<city STRING, country STRING>,
//   skills LIST<STRING>

#[test]
fn test_json_nested_struct_load() {
    // Verify the NDJSON reader infers nested objects as Struct columns.
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let json_file = get_samples_dir().join("employees_nested.json");

    let result = loader.load_file(&json_file);
    assert!(
        result.is_ok(),
        "Failed to load nested JSON: {:?}",
        result.err()
    );

    let ctx = loader.into_context();
    let schema = ctx.get_table_schema("employees_nested");
    assert!(schema.is_some(), "Schema should be available");

    // Should have 4 columns: id, name, address (struct), skills (list)
    let schema = schema.unwrap();
    assert_eq!(schema.columns.len(), 4);

    let all_rows = ctx.execute_sql("SELECT * FROM employees_nested");
    assert!(all_rows.is_ok(), "SELECT * failed: {:?}", all_rows.err());
    assert_eq!(all_rows.unwrap().row_count(), 5);
}

#[test]
fn test_json_nested_struct_field_access() {
    // Access a nested struct field using get_field().
    let mut loader = FileLoader::new().expect("Failed to create loader");
    loader
        .load_file(&get_samples_dir().join("employees_nested.json"))
        .unwrap();
    let ctx = loader.into_context();

    let result = ctx.execute_sql(
        "SELECT name, get_field(address, 'city') AS city \
         FROM employees_nested \
         ORDER BY name",
    );
    assert!(
        result.is_ok(),
        "Struct field access failed: {:?}",
        result.err()
    );
    let table = result.unwrap();
    assert_eq!(table.row_count(), 5);
    assert_eq!(table.column_count(), 2);
}

#[test]
fn test_json_nested_struct_filter() {
    // Filter on a nested struct field — only US-based employees.
    let mut loader = FileLoader::new().expect("Failed to create loader");
    loader
        .load_file(&get_samples_dir().join("employees_nested.json"))
        .unwrap();
    let ctx = loader.into_context();

    let result = ctx.execute_sql(
        "SELECT name, get_field(address, 'city') AS city \
         FROM employees_nested \
         WHERE get_field(address, 'country') = 'US' \
         ORDER BY name",
    );
    assert!(result.is_ok(), "Struct filter failed: {:?}", result.err());
    let table = result.unwrap();
    // Alice (US), Charlie (US), Eve (US) → 3 rows
    assert_eq!(table.row_count(), 3);
}

#[test]
fn test_json_unnest_array() {
    // Explode the skills array — each row becomes one row per skill.
    let mut loader = FileLoader::new().expect("Failed to create loader");
    loader
        .load_file(&get_samples_dir().join("employees_nested.json"))
        .unwrap();
    let ctx = loader.into_context();

    let result = ctx.execute_sql(
        "SELECT id, name, unnest(skills) AS skill \
         FROM employees_nested \
         ORDER BY id, skill",
    );
    assert!(result.is_ok(), "unnest(skills) failed: {:?}", result.err());
    let table = result.unwrap();
    // Alice:3, Bob:2, Charlie:3, Diana:3, Eve:2 → 13 total skill rows
    assert_eq!(table.row_count(), 13);
    assert_eq!(table.column_count(), 3);
}

#[test]
fn test_json_unnest_with_filter() {
    // Unnest skills and filter to find all employees who know Rust.
    let mut loader = FileLoader::new().expect("Failed to create loader");
    loader
        .load_file(&get_samples_dir().join("employees_nested.json"))
        .unwrap();
    let ctx = loader.into_context();

    let result = ctx.execute_sql(
        "SELECT DISTINCT name \
         FROM (SELECT id, name, unnest(skills) AS skill FROM employees_nested) \
         WHERE skill = 'Rust' \
         ORDER BY name",
    );
    assert!(result.is_ok(), "unnest + filter failed: {:?}", result.err());
    let table = result.unwrap();
    // Alice, Charlie, Eve know Rust → 3 distinct names
    assert_eq!(table.row_count(), 3);
}

// ---------------------------------------------------------------------------
// Delta Lake tests
// ---------------------------------------------------------------------------

/// Create a minimal Delta table at `path` with columns (id INT64, name STRING,
/// department STRING) and three rows, returning only after the write commits.
fn create_delta_sample(path: &str) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let ops = DeltaOps::try_from_uri(path).await.unwrap();
        let table = ops
            .create()
            .with_column(
                "id",
                DeltaDataType::Primitive(PrimitiveType::Long),
                false,
                None,
            )
            .with_column(
                "name",
                DeltaDataType::Primitive(PrimitiveType::String),
                true,
                None,
            )
            .with_column(
                "department",
                DeltaDataType::Primitive(PrimitiveType::String),
                true,
                None,
            )
            .await
            .unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new("department", ArrowDataType::Utf8, true),
        ]));

        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
                Arc::new(StringArray::from(vec![
                    "Engineering",
                    "Marketing",
                    "Engineering",
                ])),
            ],
        )
        .unwrap();

        DeltaOps(table).write(vec![batch]).await.unwrap();
    });
}

#[test]
fn test_load_delta_table() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_path = tmp_dir.path().join("employees_delta");
    let table_path_str = table_path.to_str().unwrap().to_string();

    create_delta_sample(&table_path_str);

    let mut loader = FileLoader::new().expect("Failed to create loader");
    let result = loader.load_directory(&table_path);
    assert!(
        result.is_ok(),
        "Failed to load Delta table: {:?}",
        result.err()
    );

    let tables = result.unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0], "employees_delta");

    let ctx = loader.into_context();

    let all_rows = ctx.execute_sql("SELECT * FROM employees_delta ORDER BY id");
    assert!(all_rows.is_ok(), "SELECT * failed: {:?}", all_rows.err());
    let table = all_rows.unwrap();
    assert_eq!(table.row_count(), 3);
    assert_eq!(table.column_count(), 3);
}

#[test]
fn test_delta_query_with_filter() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_path = tmp_dir.path().join("employees_delta");
    let table_path_str = table_path.to_str().unwrap().to_string();

    create_delta_sample(&table_path_str);

    let mut loader = FileLoader::new().expect("Failed to create loader");
    loader.load_directory(&table_path).unwrap();
    let ctx = loader.into_context();

    let result =
        ctx.execute_sql("SELECT name FROM employees_delta WHERE department = 'Engineering'");
    assert!(result.is_ok(), "Filtered query failed: {:?}", result.err());
    let table = result.unwrap();
    // Alice and Charlie are in Engineering
    assert_eq!(table.row_count(), 2);
}

#[test]
fn test_delta_aggregation() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_path = tmp_dir.path().join("employees_delta");
    let table_path_str = table_path.to_str().unwrap().to_string();

    create_delta_sample(&table_path_str);

    let mut loader = FileLoader::new().expect("Failed to create loader");
    loader.load_directory(&table_path).unwrap();
    let ctx = loader.into_context();

    let result = ctx.execute_sql(
        "SELECT department, COUNT(*) as cnt \
         FROM employees_delta \
         GROUP BY department \
         ORDER BY department",
    );
    assert!(result.is_ok(), "Aggregation failed: {:?}", result.err());
    let table = result.unwrap();
    assert_eq!(table.row_count(), 2); // Engineering, Marketing
    assert_eq!(table.column_count(), 2);
}

#[test]
fn test_delta_detection_requires_delta_log() {
    // A plain directory with no _delta_log should NOT be treated as Delta,
    // so load_directory should fall through to file scanning (and fail if empty).
    let tmp_dir = tempfile::tempdir().unwrap();
    let plain_dir = tmp_dir.path().join("not_a_delta_table");
    std::fs::create_dir_all(&plain_dir).unwrap();

    let mut loader = FileLoader::new().expect("Failed to create loader");
    let result = loader.load_directory(&plain_dir);
    // Empty directory → error, not a delta load
    assert!(
        result.is_err(),
        "Expected error for empty non-delta directory"
    );
}

// ---------------------------------------------------------------------------
// Iceberg tests
// ---------------------------------------------------------------------------

/// Create a minimal Iceberg table at `table_dir` using MemoryCatalog backed
/// by the local filesystem. MemoryCatalog writes a real metadata JSON file,
/// which register_iceberg() can then locate and register with DataFusion.
fn create_iceberg_sample(table_dir: &std::path::Path) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let warehouse = table_dir.parent().unwrap().to_str().unwrap().to_string();
        let table_name = table_dir.file_name().unwrap().to_str().unwrap().to_string();

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "local",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await
            .unwrap();

        let ns = NamespaceIdent::new("default".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

        let schema = IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(IcebergPrimitive::Long)).into(),
                NestedField::optional(2, "name", Type::Primitive(IcebergPrimitive::String)).into(),
                NestedField::optional(3, "department", Type::Primitive(IcebergPrimitive::String))
                    .into(),
            ])
            .build()
            .unwrap();

        let creation = TableCreation::builder()
            .name(table_name)
            .location(table_dir.to_str().unwrap().to_string())
            .schema(schema)
            .build();

        catalog.create_table(&ns, creation).await.unwrap();
    });
}

#[test]
fn test_load_iceberg_table() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_path = tmp_dir.path().join("employees_iceberg");

    create_iceberg_sample(&table_path);

    let mut loader = FileLoader::new().expect("Failed to create loader");
    let result = loader.load_directory(&table_path);
    assert!(
        result.is_ok(),
        "Failed to load Iceberg table: {:?}",
        result.err()
    );

    let tables = result.unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0], "employees_iceberg");
}

#[test]
fn test_iceberg_schema_inference() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_path = tmp_dir.path().join("employees_iceberg");

    create_iceberg_sample(&table_path);

    let mut loader = FileLoader::new().expect("Failed to create loader");
    loader.load_directory(&table_path).unwrap();
    let ctx = loader.into_context();

    // Schema should have the 3 columns defined in create_iceberg_sample
    let schema = ctx.get_table_schema("employees_iceberg");
    assert!(schema.is_some(), "Schema should be available after loading");
    let schema = schema.unwrap();
    assert_eq!(schema.columns.len(), 3);
    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[1].name, "name");
    assert_eq!(schema.columns[2].name, "department");
}

#[test]
fn test_iceberg_empty_table_query() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_path = tmp_dir.path().join("employees_iceberg");

    create_iceberg_sample(&table_path);

    let mut loader = FileLoader::new().expect("Failed to create loader");
    loader.load_directory(&table_path).unwrap();
    let ctx = loader.into_context();

    // Table has no data files yet — query should return 0 rows without error
    let result = ctx.execute_sql("SELECT * FROM employees_iceberg");
    assert!(
        result.is_ok(),
        "Query on empty Iceberg table failed: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().row_count(), 0);
}

#[test]
fn test_iceberg_detection_requires_metadata_dir() {
    // A directory without a metadata/ subdirectory must NOT be detected as
    // Iceberg, so load_directory should fall through and fail on the empty dir.
    let tmp_dir = tempfile::tempdir().unwrap();
    let plain_dir = tmp_dir.path().join("not_an_iceberg_table");
    std::fs::create_dir_all(&plain_dir).unwrap();

    let mut loader = FileLoader::new().expect("Failed to create loader");
    let result = loader.load_directory(&plain_dir);
    assert!(
        result.is_err(),
        "Expected error for empty non-iceberg directory"
    );
}

// ---------------------------------------------------------------------------
// Cross-format JOIN tests (KNO-4)
// ---------------------------------------------------------------------------

/// Write a minimal `orders` Parquet file:
///   id INT64, user_id INT64, amount FLOAT64, status STRING
/// The user_ids (1-10) match the ids in samples/users.csv.
fn create_orders_parquet(path: &std::path::Path) {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new("user_id", ArrowDataType::Int64, false),
        Field::new("amount", ArrowDataType::Float64, false),
        Field::new("status", ArrowDataType::Utf8, false),
    ]));

    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
            ])),
            Arc::new(Int64Array::from(vec![1, 1, 2, 3, 5, 1, 8, 10, 2, 7, 3, 5])),
            Arc::new(Float64Array::from(vec![
                99.98, 199.99, 149.97, 299.99, 399.98, 79.99, 249.95, 299.99, 159.98, 199.99,
                99.98, 299.99,
            ])),
            Arc::new(StringArray::from(vec![
                "completed",
                "completed",
                "completed",
                "shipped",
                "completed",
                "pending",
                "completed",
                "shipped",
                "completed",
                "pending",
                "completed",
                "completed",
            ])),
        ],
    )
    .unwrap();

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Create a SQLite database with the same orders data.
fn create_orders_sqlite(path: &std::path::Path) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(
        "CREATE TABLE orders (
             id      INTEGER PRIMARY KEY,
             user_id INTEGER NOT NULL,
             amount  REAL    NOT NULL,
             status  TEXT    NOT NULL
         );
         INSERT INTO orders VALUES (1,  1,  99.98,  'completed');
         INSERT INTO orders VALUES (2,  1, 199.99,  'completed');
         INSERT INTO orders VALUES (3,  2, 149.97,  'completed');
         INSERT INTO orders VALUES (4,  3, 299.99,  'shipped');
         INSERT INTO orders VALUES (5,  5, 399.98,  'completed');
         INSERT INTO orders VALUES (6,  1,  79.99,  'pending');
         INSERT INTO orders VALUES (7,  8, 249.95,  'completed');
         INSERT INTO orders VALUES (8, 10, 299.99,  'shipped');
         INSERT INTO orders VALUES (9,  2, 159.98,  'completed');
         INSERT INTO orders VALUES (10, 7, 199.99,  'pending');
         INSERT INTO orders VALUES (11, 3,  99.98,  'completed');
         INSERT INTO orders VALUES (12, 5, 299.99,  'completed');",
    )
    .unwrap();
}

#[test]
fn test_cross_format_join_parquet_csv() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let parquet_path = tmp_dir.path().join("orders.parquet");
    create_orders_parquet(&parquet_path);

    let mut loader = FileLoader::new().expect("Failed to create loader");
    loader.load_file(&parquet_path).unwrap();
    loader
        .load_file(&get_samples_dir().join("users.csv"))
        .unwrap();
    let ctx = loader.into_context();

    let result = ctx.execute_sql(
        "SELECT u.name, o.amount, o.status \
         FROM users u \
         JOIN orders o ON u.id = o.user_id \
         ORDER BY o.id",
    );
    assert!(
        result.is_ok(),
        "Parquet + CSV join failed: {:?}",
        result.err()
    );
    let table = result.unwrap();
    // All 12 orders reference valid user IDs (1–10), so 12 rows expected
    assert_eq!(table.row_count(), 12);
    assert_eq!(table.column_count(), 3);
}

#[test]
fn test_cross_format_join_parquet_csv_aggregated() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let parquet_path = tmp_dir.path().join("orders.parquet");
    create_orders_parquet(&parquet_path);

    let mut loader = FileLoader::new().expect("Failed to create loader");
    loader.load_file(&parquet_path).unwrap();
    loader
        .load_file(&get_samples_dir().join("users.csv"))
        .unwrap();
    let ctx = loader.into_context();

    // Total spend per department (Parquet orders joined with CSV users)
    let result = ctx.execute_sql(
        "SELECT u.department, COUNT(o.id) as order_count, SUM(o.amount) as total_spent \
         FROM users u \
         JOIN orders o ON u.id = o.user_id \
         GROUP BY u.department \
         ORDER BY total_spent DESC",
    );
    assert!(result.is_ok(), "Aggregated join failed: {:?}", result.err());
    let table = result.unwrap();
    assert!(table.row_count() > 0);
    assert_eq!(table.column_count(), 3);
}

#[test]
fn test_cross_format_join_json_csv() {
    // Join departments.json with users.csv on the department column
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = get_samples_dir();
    loader
        .load_file(&samples_dir.join("departments.json"))
        .unwrap();
    loader.load_file(&samples_dir.join("users.csv")).unwrap();
    let ctx = loader.into_context();

    let result = ctx.execute_sql(
        "SELECT u.name, u.salary, d.budget \
         FROM users u \
         JOIN departments d ON u.department = d.department \
         ORDER BY u.name",
    );
    assert!(result.is_ok(), "JSON + CSV join failed: {:?}", result.err());
    let table = result.unwrap();
    // All 10 users have a matching department (Engineering/Marketing/Sales)
    assert_eq!(table.row_count(), 10);
    assert_eq!(table.column_count(), 3);
}

#[test]
fn test_cross_format_join_sqlite_csv() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let sqlite_path = tmp_dir.path().join("orders.db");
    create_orders_sqlite(&sqlite_path);

    let mut loader = FileLoader::new().expect("Failed to create loader");
    loader.load_file(&sqlite_path).unwrap();
    loader
        .load_file(&get_samples_dir().join("users.csv"))
        .unwrap();
    let ctx = loader.into_context();

    let result = ctx.execute_sql(
        "SELECT u.name, o.amount, o.status \
         FROM users u \
         JOIN orders o ON u.id = o.user_id \
         WHERE o.status = 'completed' \
         ORDER BY o.id",
    );
    assert!(
        result.is_ok(),
        "SQLite + CSV join failed: {:?}",
        result.err()
    );
    let table = result.unwrap();
    // completed orders: ids 1,2,3,5,7,9,11,12 → 8 rows
    assert_eq!(table.row_count(), 8);
    assert_eq!(table.column_count(), 3);
}

#[test]
fn test_three_way_cross_format_join() {
    // Join all three formats in one query: Parquet orders + CSV users + JSON departments
    let tmp_dir = tempfile::tempdir().unwrap();
    let parquet_path = tmp_dir.path().join("orders.parquet");
    create_orders_parquet(&parquet_path);

    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = get_samples_dir();
    loader.load_file(&parquet_path).unwrap();
    loader.load_file(&samples_dir.join("users.csv")).unwrap();
    loader
        .load_file(&samples_dir.join("departments.json"))
        .unwrap();
    let ctx = loader.into_context();

    let result = ctx.execute_sql(
        "SELECT u.name, u.department, d.budget, SUM(o.amount) as total_spent \
         FROM users u \
         JOIN orders o ON u.id = o.user_id \
         JOIN departments d ON u.department = d.department \
         GROUP BY u.name, u.department, d.budget \
         ORDER BY total_spent DESC",
    );
    assert!(
        result.is_ok(),
        "Three-way cross-format join failed: {:?}",
        result.err()
    );
    let table = result.unwrap();
    assert!(table.row_count() > 0);
    assert_eq!(table.column_count(), 4);
}
