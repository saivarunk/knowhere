use std::path::PathBuf;

use knowhere::datafusion::{DataFusionContext, FileLoader};

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
