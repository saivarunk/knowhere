# SQL Functions

Knowhere supports 100+ built-in functions via Apache DataFusion.

## Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Count all rows |
| `COUNT(column)` | Count non-null values |
| `SUM(column)` | Sum of values |
| `AVG(column)` | Average of values |
| `MIN(column)` | Minimum value |
| `MAX(column)` | Maximum value |
| `STDDEV(column)` | Standard deviation |
| `VARIANCE(column)` | Variance |
| `ARRAY_AGG(column)` | Collect into array |
| `STRING_AGG(column, sep)` | Concatenate with separator |

## String Functions

| Function | Description |
|----------|-------------|
| `CONCAT(a, b, ...)` | Concatenate strings |
| `LENGTH(str)` | String length |
| `UPPER(str)` | Convert to uppercase |
| `LOWER(str)` | Convert to lowercase |
| `TRIM(str)` | Remove whitespace |
| `SUBSTRING(str, start, len)` | Extract substring |
| `REPLACE(str, from, to)` | Replace occurrences |
| `SPLIT_PART(str, delim, n)` | Split and get nth part |
| `REGEXP_MATCH(str, pattern)` | Regex match |
| `REGEXP_REPLACE(str, pattern, replacement)` | Regex replace |

## Date/Time Functions

| Function | Description |
|----------|-------------|
| `NOW()` | Current timestamp |
| `CURRENT_DATE` | Current date |
| `EXTRACT(part FROM date)` | Extract date part |
| `DATE_TRUNC(precision, date)` | Truncate to precision |
| `DATE_PART(part, date)` | Get date part |
| `TO_TIMESTAMP(str, format)` | Parse timestamp |

## Math Functions

| Function | Description |
|----------|-------------|
| `ABS(x)` | Absolute value |
| `CEIL(x)` | Round up |
| `FLOOR(x)` | Round down |
| `ROUND(x, d)` | Round to d decimals |
| `POWER(x, y)` | x raised to y |
| `SQRT(x)` | Square root |
| `LOG(x)` | Natural logarithm |
| `EXP(x)` | e^x |

## Conditional Functions

| Function | Description |
|----------|-------------|
| `COALESCE(a, b, ...)` | First non-null value |
| `NULLIF(a, b)` | Null if a = b |
| `CASE WHEN ... THEN ... END` | Conditional expression |

## Type Conversion

```sql
-- Cast to different types
CAST(column AS INTEGER)
CAST(column AS VARCHAR)
CAST(column AS TIMESTAMP)
CAST(column AS DOUBLE)
```
