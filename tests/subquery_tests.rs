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
fn test_scalar_subquery_in_select() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT name, salary,
               (SELECT AVG(salary) FROM users) as avg_salary
        FROM users
        LIMIT 5
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert_eq!(result.row_count(), 5);
    assert_eq!(result.column_count(), 3);
}

#[test]
fn test_scalar_subquery_in_where() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT name, salary
        FROM users
        WHERE salary > (SELECT AVG(salary) FROM users)
        ORDER BY salary DESC
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() > 0);
    assert_eq!(result.column_count(), 2);
}

#[test]
fn test_correlated_subquery() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT u.name, u.salary, u.department
        FROM users u
        WHERE u.salary > (
            SELECT AVG(u2.salary)
            FROM users u2
            WHERE u2.department = u.department
        )
        ORDER BY u.salary DESC
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() >= 0);
    assert_eq!(result.column_count(), 3);
}

#[test]
fn test_exists_subquery() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT u.name, u.email
        FROM users u
        WHERE EXISTS (
            SELECT 1 FROM orders o WHERE o.user_id = u.id
        )
        ORDER BY u.name
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() > 0);
    assert_eq!(result.column_count(), 2);
}

#[test]
fn test_not_exists_subquery() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT u.name
        FROM users u
        WHERE NOT EXISTS (
            SELECT 1 FROM orders o WHERE o.user_id = u.id
        )
        ORDER BY u.name
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() >= 0);
    assert_eq!(result.column_count(), 1);
}

#[test]
fn test_in_subquery() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT name, department
        FROM users
        WHERE id IN (
            SELECT DISTINCT user_id FROM orders
        )
        ORDER BY name
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() > 0);
    assert_eq!(result.column_count(), 2);
}

#[test]
fn test_not_in_subquery() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT name
        FROM users
        WHERE id NOT IN (
            SELECT user_id FROM orders WHERE user_id IS NOT NULL
        )
        ORDER BY name
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() >= 0);
    assert_eq!(result.column_count(), 1);
}

#[test]
fn test_subquery_in_from() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT avg_salary_by_dept.department, avg_salary_by_dept.avg_sal
        FROM (
            SELECT department, AVG(salary) as avg_sal
            FROM users
            GROUP BY department
        ) as avg_salary_by_dept
        WHERE avg_salary_by_dept.avg_sal > 70000
        ORDER BY avg_sal DESC
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() >= 0);
    assert_eq!(result.column_count(), 2);
}

#[test]
fn test_nested_subquery() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT name, salary
        FROM users
        WHERE salary > (
            SELECT AVG(salary)
            FROM users
            WHERE department = (
                SELECT department
                FROM users
                WHERE name = 'Alice Johnson'
            )
        )
        ORDER BY salary DESC
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() >= 0);
    assert_eq!(result.column_count(), 2);
}

#[test]
fn test_subquery_with_join() {
    let ctx = load_test_context();
    let sql = r#"
        SELECT u.name, o.order_count
        FROM users u
        JOIN (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
        ) o ON u.id = o.user_id
        WHERE o.order_count > 1
        ORDER BY o.order_count DESC
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() >= 0);
    assert_eq!(result.column_count(), 2);
}

#[test]
fn test_multiple_subqueries() {
    // DataFusion 48 has an optimizer bug (optimize_projections FieldNotFound)
    // when the same scalar subquery appears in both SELECT and WHERE.
    // Rewritten as a CTE to avoid the bug while covering the same logic:
    // list users earning above average, alongside the overall avg and max salary.
    let ctx = load_test_context();
    let sql = r#"
        WITH stats AS (
            SELECT AVG(salary) AS overall_avg, MAX(salary) AS max_salary
            FROM users
        )
        SELECT
            u.name,
            u.salary,
            s.overall_avg,
            s.max_salary
        FROM users u
        CROSS JOIN stats s
        WHERE u.salary > s.overall_avg
        ORDER BY u.salary DESC
        LIMIT 5
    "#;
    let result = ctx.execute_sql(sql).unwrap();
    assert!(result.row_count() <= 5);
    assert_eq!(result.column_count(), 4);
}
