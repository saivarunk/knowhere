use std::path::PathBuf;

use knowhere::datafusion::{DataFusionContext, FileLoader};
use knowhere::storage::table::Value;

fn load_test_context() -> DataFusionContext {
    let mut loader = FileLoader::new().expect("Failed to create loader");
    let samples_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("samples");

    loader
        .load_file(&samples_dir.join("users.csv"))
        .expect("Failed to load users.csv");

    loader
        .load_file(&samples_dir.join("orders.csv"))
        .expect("Failed to load orders.csv");

    loader
        .load_file(&samples_dir.join("products.csv"))
        .expect("Failed to load products.csv");

    loader.into_context()
}

#[test]
fn test_simple_cte() {
    let ctx = load_test_context();
    let sql = r#"
        WITH high_earners AS (
            SELECT name, salary FROM users WHERE salary > 80000
        )
        SELECT * FROM high_earners ORDER BY salary DESC
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() > 0);
    assert_eq!(result.column_count(), 2);
}

#[test]
fn test_multiple_ctes() {
    let ctx = load_test_context();
    let sql = r#"
        WITH
            dept_avg AS (
                SELECT department, AVG(salary) as avg_sal FROM users GROUP BY department
            ),
            high_depts AS (
                SELECT * FROM dept_avg WHERE avg_sal > 70000
            )
        SELECT * FROM high_depts
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() >= 0);
    assert_eq!(result.column_count(), 2);
}

#[test]
fn test_cte_with_join() {
    let ctx = load_test_context();
    let sql = r#"
        WITH user_order_counts AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
        )
        SELECT u.name, uoc.order_count
        FROM users u
        JOIN user_order_counts uoc ON u.id = uoc.user_id
        ORDER BY uoc.order_count DESC
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() > 0);
    assert_eq!(result.column_count(), 2);
}

#[test]
fn test_cte_in_subquery() {
    let ctx = load_test_context();
    let sql = r#"
        WITH avg_age AS (
            SELECT AVG(age) as avg FROM users
        )
        SELECT name, age
        FROM users
        WHERE age > (SELECT avg FROM avg_age)
        ORDER BY age
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() > 0);
}

#[test]
fn test_cte_with_aggregates() {
    let ctx = load_test_context();
    let sql = r#"
        WITH department_stats AS (
            SELECT
                department,
                COUNT(*) as emp_count,
                AVG(salary) as avg_salary,
                MAX(salary) as max_salary,
                MIN(salary) as min_salary
            FROM users
            GROUP BY department
        )
        SELECT * FROM department_stats
        ORDER BY avg_salary DESC
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert_eq!(result.row_count(), 3); // 3 departments
    assert_eq!(result.column_count(), 5);
}

#[test]
fn test_cte_filter_and_transform() {
    let ctx = load_test_context();
    let sql = r#"
        WITH engineering_users AS (
            SELECT id, name, salary
            FROM users
            WHERE department = 'Engineering'
        )
        SELECT name, salary, salary * 1.1 as increased_salary
        FROM engineering_users
        WHERE salary > 70000
        ORDER BY salary DESC
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() >= 0);
    assert_eq!(result.column_count(), 3);
}

#[test]
fn test_nested_cte_references() {
    let ctx = load_test_context();
    let sql = r#"
        WITH
            base_data AS (
                SELECT name, age, salary FROM users
            ),
            filtered_data AS (
                SELECT * FROM base_data WHERE age > 30
            ),
            final_data AS (
                SELECT * FROM filtered_data WHERE salary > 60000
            )
        SELECT * FROM final_data
        ORDER BY salary DESC
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() >= 0);
    assert_eq!(result.column_count(), 3);
}
