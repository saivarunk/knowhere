use std::path::PathBuf;

use knowhere::datafusion::{DataFusionContext, FileLoader};
use knowhere::storage::table::Value;

fn load_test_context() -> DataFusionContext {
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("samples");

    // Load users
    loader
        .load_file(&samples_dir.join("users.csv"))
        .expect("Failed to load users.csv");

    // Load orders
    loader
        .load_file(&samples_dir.join("orders.csv"))
        .expect("Failed to load orders.csv");

    // Load products
    loader
        .load_file(&samples_dir.join("products.csv"))
        .expect("Failed to load products.csv");

    loader.into_context()
}

#[test]
fn test_select_all_from_users() {
    let ctx = load_test_context();
    let result = ctx.execute_sql("SELECT * FROM users").unwrap();

    assert_eq!(result.row_count(), 10);
    assert_eq!(result.column_count(), 7);
}

#[test]
fn test_select_specific_columns() {
    let ctx = load_test_context();
    let result = ctx.execute_sql("SELECT name, email FROM users").unwrap();

    assert_eq!(result.column_count(), 2);
    assert_eq!(result.row_count(), 10);
}

#[test]
fn test_where_clause_comparison() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT * FROM users WHERE age > 35")
        .unwrap();

    // Should include Charlie (45), Fiona (41), George (55), Ivan (38), Julia (42)
    assert_eq!(result.row_count(), 5);
}

#[test]
fn test_where_clause_string() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT * FROM users WHERE department = 'Engineering'")
        .unwrap();

    assert_eq!(result.row_count(), 5);
}

#[test]
fn test_where_and_condition() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT * FROM users WHERE department = 'Engineering' AND age > 35")
        .unwrap();

    // Charlie (45), George (55), Ivan (38)
    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_where_or_condition() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT * FROM users WHERE department = 'Sales' OR department = 'Marketing'")
        .unwrap();

    // Sales: Diana, Hannah; Marketing: Bob, Fiona, Julia
    assert_eq!(result.row_count(), 5);
}

#[test]
fn test_order_by_asc() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT name, age FROM users ORDER BY age ASC")
        .unwrap();

    // First should be Hannah (24)
    if let Value::Integer(age) = &result.rows[0].values[1] {
        assert_eq!(*age, 24);
    }
}

#[test]
fn test_order_by_desc() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT name, age FROM users ORDER BY age DESC")
        .unwrap();

    // First should be George (55)
    if let Value::Integer(age) = &result.rows[0].values[1] {
        assert_eq!(*age, 55);
    }
}

#[test]
fn test_limit() {
    let ctx = load_test_context();
    let result = ctx.execute_sql("SELECT * FROM users LIMIT 3").unwrap();

    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_limit_offset() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT * FROM users LIMIT 3 OFFSET 2")
        .unwrap();

    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_count_all() {
    let ctx = load_test_context();
    let result = ctx.execute_sql("SELECT COUNT(*) FROM users").unwrap();

    assert_eq!(result.row_count(), 1);
    if let Value::Integer(count) = &result.rows[0].values[0] {
        assert_eq!(*count, 10);
    }
}

#[test]
fn test_count_with_where() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT COUNT(*) FROM users WHERE department = 'Engineering'")
        .unwrap();

    if let Value::Integer(count) = &result.rows[0].values[0] {
        assert_eq!(*count, 5);
    }
}

#[test]
fn test_sum() {
    let ctx = load_test_context();
    let result = ctx.execute_sql("SELECT SUM(quantity) FROM orders").unwrap();

    if let Value::Float(sum) = &result.rows[0].values[0] {
        assert!((sum - 22.0).abs() < 0.01);
    }
}

#[test]
fn test_avg() {
    let ctx = load_test_context();
    let result = ctx.execute_sql("SELECT AVG(age) FROM users").unwrap();

    if let Value::Float(avg) = &result.rows[0].values[0] {
        assert!((avg - 36.9).abs() < 0.1);
    }
}

#[test]
fn test_min_max() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT MIN(age), MAX(age) FROM users")
        .unwrap();

    if let Value::Integer(min) = &result.rows[0].values[0] {
        assert_eq!(*min, 24);
    }
    if let Value::Integer(max) = &result.rows[0].values[1] {
        assert_eq!(*max, 55);
    }
}

#[test]
fn test_group_by() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT department, COUNT(*) FROM users GROUP BY department")
        .unwrap();

    // Should have 3 groups: Engineering, Marketing, Sales
    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_group_by_having() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql(
            "SELECT department, COUNT(*) FROM users GROUP BY department HAVING COUNT(*) > 2",
        )
        .unwrap();

    // Only Engineering (5) and Marketing (3) have more than 2
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_join() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql(
            "SELECT users.name, orders.id FROM users JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();

    // Should match users with their orders
    assert!(result.row_count() > 0);
}

#[test]
fn test_left_join() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql(
            "SELECT users.name, orders.id FROM users LEFT JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();

    // Should include all users, even those without orders
    assert!(result.row_count() >= 10);
}

#[test]
fn test_distinct() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT DISTINCT department FROM users")
        .unwrap();

    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_like() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT * FROM users WHERE name LIKE 'A%'")
        .unwrap();

    // Alice
    assert_eq!(result.row_count(), 1);
}

#[test]
fn test_like_middle() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT * FROM users WHERE name LIKE '%son%'")
        .unwrap();

    // Alice Johnson, Edward Norton
    assert!(result.row_count() >= 1);
}

#[test]
fn test_in_clause() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT * FROM orders WHERE status IN ('pending', 'shipped')")
        .unwrap();

    assert!(result.row_count() > 0);
}

#[test]
fn test_between() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT * FROM users WHERE age BETWEEN 30 AND 40")
        .unwrap();

    // Alice (32), Diana (35), Ivan (38)
    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_is_null() {
    let ctx = load_test_context();
    // All our test data has values, so this should return 0 rows
    let result = ctx
        .execute_sql("SELECT * FROM users WHERE email IS NULL")
        .unwrap();
    assert_eq!(result.row_count(), 0);
}

#[test]
fn test_is_not_null() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT * FROM users WHERE email IS NOT NULL")
        .unwrap();
    assert_eq!(result.row_count(), 10);
}

#[test]
fn test_column_alias() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT name AS user_name FROM users LIMIT 1")
        .unwrap();

    assert_eq!(result.schema.columns[0].name, "user_name");
}

#[test]
fn test_arithmetic() {
    let ctx = load_test_context();
    let result = ctx
        .execute_sql("SELECT quantity * price AS total FROM orders LIMIT 1")
        .unwrap();

    assert_eq!(result.column_count(), 1);
}

#[test]
fn test_complex_query() {
    let ctx = load_test_context();
    let result = ctx.execute_sql(
        "SELECT users.name, COUNT(orders.id) AS order_count, SUM(orders.quantity * orders.price) AS total_spent
         FROM users
         LEFT JOIN orders ON users.id = orders.user_id
         GROUP BY users.name
         HAVING COUNT(orders.id) > 0
         ORDER BY total_spent DESC
         LIMIT 5",
    )
    .unwrap();

    assert!(result.row_count() <= 5);
    assert_eq!(result.column_count(), 3);
}
