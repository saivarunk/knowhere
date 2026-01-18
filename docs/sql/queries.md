# SQL Queries

## Basic Queries

```sql
-- Select all columns
SELECT * FROM users

-- Select specific columns
SELECT name, email, age FROM users

-- Filter with WHERE
SELECT * FROM users WHERE age > 30

-- Sort results
SELECT * FROM users ORDER BY age DESC

-- Limit results
SELECT * FROM users LIMIT 10
```

## Common Table Expressions (CTEs)

```sql
-- Simple CTE
WITH high_earners AS (
    SELECT name, salary FROM users WHERE salary > 80000
)
SELECT * FROM high_earners ORDER BY salary DESC

-- Multiple CTEs
WITH
    dept_avg AS (
        SELECT department, AVG(salary) as avg_sal 
        FROM users GROUP BY department
    ),
    high_depts AS (
        SELECT * FROM dept_avg WHERE avg_sal > 70000
    )
SELECT * FROM high_depts
```

## Window Functions

```sql
-- Row number
SELECT name, salary,
       ROW_NUMBER() OVER (ORDER BY salary DESC) as rank
FROM users

-- Partition by
SELECT department, name, salary,
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
FROM users

-- Running totals
SELECT name, salary,
       SUM(salary) OVER (ORDER BY name) as running_total
FROM users
```

## Subqueries

```sql
-- Scalar subquery
SELECT name, salary
FROM users
WHERE salary > (SELECT AVG(salary) FROM users)

-- EXISTS subquery
SELECT u.name
FROM users u
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.user_id = u.id
)
```

## Joins

```sql
-- Inner join
SELECT users.name, orders.amount
FROM users
JOIN orders ON users.id = orders.user_id

-- Left join with aggregation
SELECT
    users.name,
    COUNT(orders.id) AS order_count,
    SUM(orders.amount) AS total_spent
FROM users
LEFT JOIN orders ON users.id = orders.user_id
GROUP BY users.name
```

## Set Operations

```sql
-- Union
SELECT name FROM users WHERE department = 'Engineering'
UNION
SELECT name FROM users WHERE department = 'Marketing'

-- Intersect
SELECT user_id FROM orders WHERE status = 'completed'
INTERSECT
SELECT user_id FROM orders WHERE status = 'shipped'
```
