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

    loader.into_context()
}

#[test]
fn test_row_number() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT name, salary,
               ROW_NUMBER() OVER (ORDER BY salary DESC) as rank
        FROM users
        ORDER BY salary DESC
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert_eq!(result.row_count(), 10);
    assert_eq!(result.column_count(), 3);

    // First row should have rank 1
    if let Value::Integer(rank) = &result.rows[0].values[2] {
        assert_eq!(*rank, 1);
    }
}

#[test]
fn test_partition_by() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT department, name, salary,
               RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
        FROM users
        ORDER BY department, dept_rank
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert_eq!(result.row_count(), 10);
    assert_eq!(result.column_count(), 4);
}

#[test]
fn test_dense_rank() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT name, salary,
               DENSE_RANK() OVER (ORDER BY salary DESC) as dense_rank
        FROM users
        ORDER BY dense_rank
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert_eq!(result.row_count(), 10);
    assert_eq!(result.column_count(), 3);
}

#[test]
fn test_lag_lead() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT name, salary,
               LAG(salary, 1) OVER (ORDER BY salary) as prev_salary,
               LEAD(salary, 1) OVER (ORDER BY salary) as next_salary
        FROM users
        ORDER BY salary
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert_eq!(result.row_count(), 10);
    assert_eq!(result.column_count(), 4);

    // First row's prev_salary should be NULL
    assert!(result.rows[0].values[2].is_null());
}

#[test]
fn test_first_value_last_value() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT department, name, salary,
               FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary) as min_dept_salary,
               LAST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as max_dept_salary
        FROM users
        ORDER BY department, salary
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert_eq!(result.row_count(), 10);
    assert_eq!(result.column_count(), 5);
}

#[test]
fn test_sum_over() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT name, salary,
               SUM(salary) OVER (ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
        FROM users
        ORDER BY name
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert_eq!(result.row_count(), 10);
    assert_eq!(result.column_count(), 3);
}

#[test]
fn test_avg_over_partition() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT department, name, salary,
               AVG(salary) OVER (PARTITION BY department) as dept_avg_salary
        FROM users
        ORDER BY department, name
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert_eq!(result.row_count(), 10);
    assert_eq!(result.column_count(), 4);
}

#[test]
fn test_count_over() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT department, name,
               COUNT(*) OVER (PARTITION BY department) as dept_count
        FROM users
        ORDER BY department, name
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert_eq!(result.row_count(), 10);
    assert_eq!(result.column_count(), 3);
}

#[test]
fn test_multiple_window_functions() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT
            name,
            salary,
            department,
            ROW_NUMBER() OVER (ORDER BY salary DESC) as overall_rank,
            RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
            AVG(salary) OVER (PARTITION BY department) as dept_avg
        FROM users
        ORDER BY salary DESC
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert_eq!(result.row_count(), 10);
    assert_eq!(result.column_count(), 6);
}

#[test]
fn test_window_with_where() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT name, salary,
               RANK() OVER (ORDER BY salary DESC) as salary_rank
        FROM users
        WHERE age > 30
        ORDER BY salary DESC
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() > 0);
    assert_eq!(result.column_count(), 3);
}
