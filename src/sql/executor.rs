use super::ast::*;
use super::planner::LogicalPlan;
use super::Parser;
use crate::sql::Planner;
use crate::storage::table::{Column, DataType, Row, Schema, Table, Value};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("Table not found: {0}")]
    TableNotFound(String),
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    #[error("Type error: {0}")]
    TypeError(String),
    #[error("Division by zero")]
    DivisionByZero,
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Plan error: {0}")]
    PlanError(String),
}

pub struct ExecutionContext {
    pub tables: HashMap<String, Table>,
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, table: Table) {
        let name = table.name.to_lowercase();
        self.tables.insert(name, table);
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(&name.to_lowercase())
    }
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Executor<'a> {
    ctx: &'a ExecutionContext,
    table_aliases: HashMap<String, String>,
}

impl<'a> Executor<'a> {
    pub fn new(ctx: &'a ExecutionContext) -> Self {
        Self {
            ctx,
            table_aliases: HashMap::new(),
        }
    }

    pub fn execute(&mut self, plan: &LogicalPlan) -> Result<Table, ExecutionError> {
        match plan {
            LogicalPlan::TableScan { table_name, alias } => {
                self.execute_table_scan(table_name, alias.as_deref())
            }
            LogicalPlan::Projection {
                input,
                exprs,
                distinct,
            } => self.execute_projection(input, exprs, *distinct),
            LogicalPlan::Filter { input, predicate } => self.execute_filter(input, predicate),
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => self.execute_join(left, right, join_type, condition.as_ref()),
            LogicalPlan::CrossJoin { left, right } => self.execute_cross_join(left, right),
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                having,
            } => self.execute_aggregate(input, group_by, aggregates, having.as_ref()),
            LogicalPlan::Sort { input, order_by } => self.execute_sort(input, order_by),
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => self.execute_limit(input, *limit, *offset),
            LogicalPlan::Empty => Ok(Table::new("result", Schema::new(vec![]))),
        }
    }

    fn execute_table_scan(
        &mut self,
        table_name: &str,
        alias: Option<&str>,
    ) -> Result<Table, ExecutionError> {
        let table = self
            .ctx
            .get_table(table_name)
            .ok_or_else(|| ExecutionError::TableNotFound(table_name.to_string()))?;

        // Register alias
        if let Some(a) = alias {
            self.table_aliases
                .insert(a.to_lowercase(), table_name.to_lowercase());
        }
        self.table_aliases
            .insert(table_name.to_lowercase(), table_name.to_lowercase());

        Ok(table.clone())
    }

    fn execute_projection(
        &mut self,
        input: &LogicalPlan,
        exprs: &[(Expr, Option<String>)],
        distinct: bool,
    ) -> Result<Table, ExecutionError> {
        let input_table = self.execute(input)?;

        // Handle SELECT * expansion
        let expanded_exprs = self.expand_star_exprs(exprs, &input_table)?;

        // Build output schema
        let columns: Vec<Column> = expanded_exprs
            .iter()
            .enumerate()
            .map(|(i, (expr, alias))| {
                let name = alias
                    .clone()
                    .unwrap_or_else(|| self.expr_to_name(expr, i));
                let dtype = self.infer_expr_type(expr, &input_table);
                Column::new(name, dtype)
            })
            .collect();

        let schema = Schema::new(columns);
        let mut result = Table::new("result", schema);

        // Evaluate expressions for each row
        for row in input_table.iter() {
            let values: Vec<Value> = expanded_exprs
                .iter()
                .map(|(expr, _)| self.evaluate_expr(expr, row, &input_table))
                .collect::<Result<Vec<_>, _>>()?;
            result.add_row(Row::new(values));
        }

        // Handle DISTINCT
        if distinct {
            result = self.deduplicate(result);
        }

        Ok(result)
    }

    fn expand_star_exprs(
        &self,
        exprs: &[(Expr, Option<String>)],
        table: &Table,
    ) -> Result<Vec<(Expr, Option<String>)>, ExecutionError> {
        let mut result = Vec::new();

        for (expr, alias) in exprs {
            match expr {
                Expr::Column(ColumnRef { table: None, column }) if column == "*" => {
                    // Expand * to all columns
                    for col in &table.schema.columns {
                        result.push((Expr::Column(ColumnRef::new(&col.name)), None));
                    }
                }
                Expr::Column(ColumnRef {
                    table: Some(tbl),
                    column,
                }) if column == "*" => {
                    // Expand table.* - for now just expand all columns with that prefix
                    let prefix = format!("{}.", tbl);
                    for col in &table.schema.columns {
                        if col.name.to_lowercase().starts_with(&prefix.to_lowercase()) {
                            result.push((Expr::Column(ColumnRef::new(&col.name)), None));
                        }
                    }
                }
                _ => {
                    result.push((expr.clone(), alias.clone()));
                }
            }
        }

        Ok(result)
    }

    fn execute_filter(
        &mut self,
        input: &LogicalPlan,
        predicate: &Expr,
    ) -> Result<Table, ExecutionError> {
        let input_table = self.execute(input)?;

        let schema = input_table.schema.clone();
        let mut result = Table::new("result", schema);

        for row in input_table.iter() {
            let value = self.evaluate_expr(predicate, row, &input_table)?;
            if value.is_truthy() {
                result.add_row(row.clone());
            }
        }

        Ok(result)
    }

    fn execute_join(
        &mut self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        join_type: &JoinType,
        condition: Option<&Expr>,
    ) -> Result<Table, ExecutionError> {
        let left_table = self.execute(left)?;
        let right_table = self.execute(right)?;

        // Get table names for prefixing
        let left_name = left_table.name.clone();
        let right_name = right_table.name.clone();

        // Build combined schema with prefixed column names
        let mut columns = Vec::new();
        for col in &left_table.schema.columns {
            let prefixed_name = format!("{}.{}", left_name, col.name);
            columns.push(Column::new(prefixed_name, col.data_type.clone()));
        }
        for col in &right_table.schema.columns {
            let prefixed_name = format!("{}.{}", right_name, col.name);
            columns.push(Column::new(prefixed_name, col.data_type.clone()));
        }
        let schema = Schema::new(columns);
        let mut result = Table::new("result", schema.clone());

        // Track which left rows have been matched (for LEFT JOIN)
        let mut left_matched: HashSet<usize> = HashSet::new();
        // Track which right rows have been matched (for RIGHT JOIN)
        let mut right_matched: HashSet<usize> = HashSet::new();

        // Create a combined table for expression evaluation
        let combined_schema = schema.clone();
        let combined_table = Table::new("combined", combined_schema);

        // Nested loop join
        for (left_idx, left_row) in left_table.iter().enumerate() {
            let mut has_match = false;

            for (right_idx, right_row) in right_table.iter().enumerate() {
                // Combine rows for evaluation
                let mut combined_values = left_row.values.clone();
                combined_values.extend(right_row.values.clone());
                let combined_row = Row::new(combined_values);

                let matches = match condition {
                    Some(cond) => {
                        let val = self.evaluate_expr(cond, &combined_row, &combined_table)?;
                        val.is_truthy()
                    }
                    None => true, // CROSS JOIN
                };

                if matches {
                    has_match = true;
                    left_matched.insert(left_idx);
                    right_matched.insert(right_idx);
                    result.add_row(combined_row);
                }
            }

            // For LEFT JOIN, add unmatched left rows with NULLs
            if !has_match && matches!(join_type, JoinType::Left) {
                let mut combined_values = left_row.values.clone();
                for _ in 0..right_table.column_count() {
                    combined_values.push(Value::Null);
                }
                result.add_row(Row::new(combined_values));
            }
        }

        // For RIGHT JOIN, add unmatched right rows with NULLs
        if matches!(join_type, JoinType::Right) {
            for (right_idx, right_row) in right_table.iter().enumerate() {
                if !right_matched.contains(&right_idx) {
                    let mut combined_values = Vec::new();
                    for _ in 0..left_table.column_count() {
                        combined_values.push(Value::Null);
                    }
                    combined_values.extend(right_row.values.clone());
                    result.add_row(Row::new(combined_values));
                }
            }
        }

        Ok(result)
    }

    fn execute_cross_join(
        &mut self,
        left: &LogicalPlan,
        right: &LogicalPlan,
    ) -> Result<Table, ExecutionError> {
        self.execute_join(left, right, &JoinType::Cross, None)
    }

    fn execute_aggregate(
        &mut self,
        input: &LogicalPlan,
        group_by: &[Expr],
        aggregates: &[(Expr, Option<String>)],
        having: Option<&Expr>,
    ) -> Result<Table, ExecutionError> {
        let input_table = self.execute(input)?;

        // Group rows
        let groups = self.group_rows(&input_table, group_by)?;

        // Build schema for result
        let mut columns = Vec::new();
        for (i, expr) in group_by.iter().enumerate() {
            let name = self.expr_to_name(expr, i);
            let dtype = self.infer_expr_type(expr, &input_table);
            columns.push(Column::new(name, dtype));
        }
        for (i, (expr, _alias)) in aggregates.iter().enumerate() {
            // Use expression name (e.g., "COUNT") not the alias for aggregate columns
            // The alias will be applied in the final projection step
            let name = self.expr_to_name(expr, group_by.len() + i);
            columns.push(Column::new(name, DataType::Float)); // Aggregates typically return numbers
        }

        let schema = Schema::new(columns);
        let mut result = Table::new("result", schema);

        // Compute aggregates for each group
        for (group_key, group_rows) in groups {
            let mut values = group_key;

            for (agg_expr, _) in aggregates {
                let agg_value = self.compute_aggregate(agg_expr, &group_rows, &input_table)?;
                values.push(agg_value);
            }

            let row = Row::new(values);

            // Apply HAVING filter
            if let Some(having_expr) = having {
                let having_val = self.evaluate_expr(having_expr, &row, &result)?;
                if !having_val.is_truthy() {
                    continue;
                }
            }

            result.add_row(row);
        }

        Ok(result)
    }

    fn group_rows(
        &self,
        table: &Table,
        group_by: &[Expr],
    ) -> Result<Vec<(Vec<Value>, Vec<Row>)>, ExecutionError> {
        let mut groups: HashMap<Vec<String>, (Vec<Value>, Vec<Row>)> = HashMap::new();

        for row in table.iter() {
            let key_values: Vec<Value> = group_by
                .iter()
                .map(|expr| self.evaluate_expr(expr, row, table))
                .collect::<Result<_, _>>()?;

            let key_strings: Vec<String> = key_values.iter().map(|v| format!("{:?}", v)).collect();

            groups
                .entry(key_strings)
                .or_insert_with(|| (key_values.clone(), Vec::new()))
                .1
                .push(row.clone());
        }

        // If no GROUP BY, treat all rows as one group
        if group_by.is_empty() && !table.rows.is_empty() {
            return Ok(vec![(Vec::new(), table.rows.clone())]);
        }

        Ok(groups.into_values().collect())
    }

    fn compute_aggregate(
        &self,
        expr: &Expr,
        rows: &[Row],
        table: &Table,
    ) -> Result<Value, ExecutionError> {
        match expr {
            Expr::Function {
                name,
                args,
                distinct,
            } => {
                let func_name = name.to_uppercase();
                match func_name.as_str() {
                    "COUNT" => {
                        if args.len() == 1 {
                            if let Expr::Column(ColumnRef { column, .. }) = &args[0] {
                                if column == "*" {
                                    return Ok(Value::Integer(rows.len() as i64));
                                }
                            }
                        }
                        // COUNT(column) - count non-null values
                        let mut count = 0i64;
                        let mut seen: HashSet<String> = HashSet::new();

                        for row in rows {
                            let val = self.evaluate_expr(&args[0], row, table)?;
                            if !val.is_null() {
                                if *distinct {
                                    let key = format!("{:?}", val);
                                    if seen.insert(key) {
                                        count += 1;
                                    }
                                } else {
                                    count += 1;
                                }
                            }
                        }
                        Ok(Value::Integer(count))
                    }
                    "SUM" => {
                        let mut sum = 0.0f64;
                        let mut seen: HashSet<String> = HashSet::new();

                        for row in rows {
                            let val = self.evaluate_expr(&args[0], row, table)?;
                            if !val.is_null() {
                                if *distinct {
                                    let key = format!("{:?}", val);
                                    if !seen.insert(key) {
                                        continue;
                                    }
                                }
                                if let Some(n) = val.as_float() {
                                    sum += n;
                                }
                            }
                        }
                        Ok(Value::Float(sum))
                    }
                    "AVG" => {
                        let mut sum = 0.0f64;
                        let mut count = 0i64;
                        let mut seen: HashSet<String> = HashSet::new();

                        for row in rows {
                            let val = self.evaluate_expr(&args[0], row, table)?;
                            if !val.is_null() {
                                if *distinct {
                                    let key = format!("{:?}", val);
                                    if !seen.insert(key) {
                                        continue;
                                    }
                                }
                                if let Some(n) = val.as_float() {
                                    sum += n;
                                    count += 1;
                                }
                            }
                        }
                        if count > 0 {
                            Ok(Value::Float(sum / count as f64))
                        } else {
                            Ok(Value::Null)
                        }
                    }
                    "MIN" => {
                        let mut min: Option<Value> = None;
                        for row in rows {
                            let val = self.evaluate_expr(&args[0], row, table)?;
                            if !val.is_null() {
                                min = Some(match min {
                                    None => val,
                                    Some(m) => {
                                        if val < m {
                                            val
                                        } else {
                                            m
                                        }
                                    }
                                });
                            }
                        }
                        Ok(min.unwrap_or(Value::Null))
                    }
                    "MAX" => {
                        let mut max: Option<Value> = None;
                        for row in rows {
                            let val = self.evaluate_expr(&args[0], row, table)?;
                            if !val.is_null() {
                                max = Some(match max {
                                    None => val,
                                    Some(m) => {
                                        if val > m {
                                            val
                                        } else {
                                            m
                                        }
                                    }
                                });
                            }
                        }
                        Ok(max.unwrap_or(Value::Null))
                    }
                    _ => Err(ExecutionError::InvalidOperation(format!(
                        "Unknown aggregate function: {}",
                        name
                    ))),
                }
            }
            _ => {
                // Non-aggregate expression - evaluate for first row
                if let Some(row) = rows.first() {
                    self.evaluate_expr(expr, row, table)
                } else {
                    Ok(Value::Null)
                }
            }
        }
    }

    fn execute_sort(
        &mut self,
        input: &LogicalPlan,
        order_by: &[OrderByItem],
    ) -> Result<Table, ExecutionError> {
        let input_table = self.execute(input)?;

        let schema = input_table.schema.clone();
        let mut rows: Vec<Row> = input_table.rows.clone();

        // Sort rows
        rows.sort_by(|a, b| {
            for item in order_by {
                let val_a = self.evaluate_expr(&item.expr, a, &input_table).unwrap();
                let val_b = self.evaluate_expr(&item.expr, b, &input_table).unwrap();

                let cmp = val_a.partial_cmp(&val_b).unwrap_or(std::cmp::Ordering::Equal);
                let cmp = if item.ascending { cmp } else { cmp.reverse() };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(Table::with_rows("result", schema, rows))
    }

    fn execute_limit(
        &mut self,
        input: &LogicalPlan,
        limit: u64,
        offset: Option<u64>,
    ) -> Result<Table, ExecutionError> {
        let input_table = self.execute(input)?;

        let schema = input_table.schema.clone();
        let offset = offset.unwrap_or(0) as usize;
        let limit = limit as usize;

        let rows: Vec<Row> = input_table
            .rows
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect();

        Ok(Table::with_rows("result", schema, rows))
    }

    fn deduplicate(&self, table: Table) -> Table {
        let mut seen: HashSet<Vec<String>> = HashSet::new();
        let mut rows = Vec::new();

        for row in table.rows {
            let key: Vec<String> = row.values.iter().map(|v| format!("{:?}", v)).collect();
            if seen.insert(key) {
                rows.push(row);
            }
        }

        Table::with_rows(table.name, table.schema, rows)
    }

    fn evaluate_expr(&self, expr: &Expr, row: &Row, table: &Table) -> Result<Value, ExecutionError> {
        match expr {
            Expr::Integer(n) => Ok(Value::Integer(*n)),
            Expr::Float(f) => Ok(Value::Float(*f)),
            Expr::String(s) => Ok(Value::String(s.clone())),
            Expr::Boolean(b) => Ok(Value::Boolean(*b)),
            Expr::Null => Ok(Value::Null),

            Expr::Column(col_ref) => self.resolve_column(col_ref, row, table),

            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expr(left, row, table)?;
                let right_val = self.evaluate_expr(right, row, table)?;
                self.apply_binary_op(&left_val, op, &right_val)
            }

            Expr::UnaryOp { op, expr } => {
                let val = self.evaluate_expr(expr, row, table)?;
                self.apply_unary_op(op, &val)
            }

            Expr::Function { name, args, .. } => {
                // For non-aggregate functions
                let func_name = name.to_uppercase();
                match func_name.as_str() {
                    "UPPER" => {
                        let val = self.evaluate_expr(&args[0], row, table)?;
                        Ok(Value::String(val.to_string().to_uppercase()))
                    }
                    "LOWER" => {
                        let val = self.evaluate_expr(&args[0], row, table)?;
                        Ok(Value::String(val.to_string().to_lowercase()))
                    }
                    "LENGTH" => {
                        let val = self.evaluate_expr(&args[0], row, table)?;
                        Ok(Value::Integer(val.to_string().len() as i64))
                    }
                    "COALESCE" => {
                        for arg in args {
                            let val = self.evaluate_expr(arg, row, table)?;
                            if !val.is_null() {
                                return Ok(val);
                            }
                        }
                        Ok(Value::Null)
                    }
                    "ABS" => {
                        let val = self.evaluate_expr(&args[0], row, table)?;
                        match val {
                            Value::Integer(n) => Ok(Value::Integer(n.abs())),
                            Value::Float(f) => Ok(Value::Float(f.abs())),
                            _ => Ok(Value::Null),
                        }
                    }
                    // Aggregate functions - look up in table schema if available
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
                        // After aggregation, the result should be in the table's columns
                        // Try to find by function name (e.g., "COUNT")
                        if let Some(idx) = table.schema.column_index(name) {
                            return Ok(row.values.get(idx).cloned().unwrap_or(Value::Null));
                        }
                        // If we're evaluating an aggregate at row level, return null
                        // (aggregates should be computed at group level)
                        Ok(Value::Null)
                    }
                    _ => Err(ExecutionError::InvalidOperation(format!(
                        "Unknown function: {}",
                        name
                    ))),
                }
            }

            Expr::IsNull { expr, negated } => {
                let val = self.evaluate_expr(expr, row, table)?;
                let is_null = val.is_null();
                Ok(Value::Boolean(if *negated { !is_null } else { is_null }))
            }

            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let val = self.evaluate_expr(expr, row, table)?;
                let mut found = false;
                for item in list {
                    let item_val = self.evaluate_expr(item, row, table)?;
                    if val == item_val {
                        found = true;
                        break;
                    }
                }
                Ok(Value::Boolean(if *negated { !found } else { found }))
            }

            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let val = self.evaluate_expr(expr, row, table)?;
                let low_val = self.evaluate_expr(low, row, table)?;
                let high_val = self.evaluate_expr(high, row, table)?;
                let in_range = val >= low_val && val <= high_val;
                Ok(Value::Boolean(if *negated { !in_range } else { in_range }))
            }

            Expr::Like {
                expr,
                pattern,
                negated,
            } => {
                let val = self.evaluate_expr(expr, row, table)?;
                let pattern_val = self.evaluate_expr(pattern, row, table)?;

                let val_str = val.to_string();
                let pattern_str = pattern_val.to_string();

                let matches = self.like_match(&val_str, &pattern_str);
                Ok(Value::Boolean(if *negated { !matches } else { matches }))
            }

            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    let op_val = self.evaluate_expr(op, row, table)?;
                    for (when_expr, then_expr) in when_clauses {
                        let when_val = self.evaluate_expr(when_expr, row, table)?;
                        if op_val == when_val {
                            return self.evaluate_expr(then_expr, row, table);
                        }
                    }
                } else {
                    for (when_expr, then_expr) in when_clauses {
                        let when_val = self.evaluate_expr(when_expr, row, table)?;
                        if when_val.is_truthy() {
                            return self.evaluate_expr(then_expr, row, table);
                        }
                    }
                }
                if let Some(else_expr) = else_clause {
                    self.evaluate_expr(else_expr, row, table)
                } else {
                    Ok(Value::Null)
                }
            }
        }
    }

    fn resolve_column(
        &self,
        col_ref: &ColumnRef,
        row: &Row,
        table: &Table,
    ) -> Result<Value, ExecutionError> {
        // Try direct column name first
        if let Some(idx) = table.schema.column_index(&col_ref.column) {
            return Ok(row.values.get(idx).cloned().unwrap_or(Value::Null));
        }

        // Try with table prefix
        if let Some(ref tbl) = col_ref.table {
            let prefixed = format!("{}.{}", tbl, col_ref.column);
            if let Some(idx) = table.schema.column_index(&prefixed) {
                return Ok(row.values.get(idx).cloned().unwrap_or(Value::Null));
            }

            // Try resolving table alias
            if let Some(real_table) = self.table_aliases.get(&tbl.to_lowercase()) {
                let prefixed = format!("{}.{}", real_table, col_ref.column);
                if let Some(idx) = table.schema.column_index(&prefixed) {
                    return Ok(row.values.get(idx).cloned().unwrap_or(Value::Null));
                }
            }
        }

        // Search all columns for a match
        for (i, col) in table.schema.columns.iter().enumerate() {
            if col.name.to_lowercase().ends_with(&format!(".{}", col_ref.column.to_lowercase())) {
                return Ok(row.values.get(i).cloned().unwrap_or(Value::Null));
            }
            if col.name.to_lowercase() == col_ref.column.to_lowercase() {
                return Ok(row.values.get(i).cloned().unwrap_or(Value::Null));
            }
        }

        Err(ExecutionError::ColumnNotFound(format!(
            "{}{}",
            col_ref
                .table
                .as_ref()
                .map(|t| format!("{}.", t))
                .unwrap_or_default(),
            col_ref.column
        )))
    }

    fn apply_binary_op(
        &self,
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
    ) -> Result<Value, ExecutionError> {
        // Handle NULL propagation for most operations
        if left.is_null() || right.is_null() {
            return match op {
                BinaryOperator::And => {
                    // NULL AND FALSE = FALSE
                    if let Value::Boolean(false) = left {
                        return Ok(Value::Boolean(false));
                    }
                    if let Value::Boolean(false) = right {
                        return Ok(Value::Boolean(false));
                    }
                    Ok(Value::Null)
                }
                BinaryOperator::Or => {
                    // NULL OR TRUE = TRUE
                    if let Value::Boolean(true) = left {
                        return Ok(Value::Boolean(true));
                    }
                    if let Value::Boolean(true) = right {
                        return Ok(Value::Boolean(true));
                    }
                    Ok(Value::Null)
                }
                _ => Ok(Value::Null),
            };
        }

        match op {
            BinaryOperator::Add => match (left, right) {
                (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a + b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
                (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
                (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a + *b as f64)),
                _ => Err(ExecutionError::TypeError("Cannot add these types".into())),
            },
            BinaryOperator::Subtract => match (left, right) {
                (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a - b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
                (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
                (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a - *b as f64)),
                _ => Err(ExecutionError::TypeError(
                    "Cannot subtract these types".into(),
                )),
            },
            BinaryOperator::Multiply => match (left, right) {
                (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a * b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
                (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
                (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a * *b as f64)),
                _ => Err(ExecutionError::TypeError(
                    "Cannot multiply these types".into(),
                )),
            },
            BinaryOperator::Divide => {
                let divisor = match right {
                    Value::Integer(0) => return Err(ExecutionError::DivisionByZero),
                    Value::Float(f) if *f == 0.0 => return Err(ExecutionError::DivisionByZero),
                    _ => {}
                };
                let _ = divisor;
                match (left, right) {
                    (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a / b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a / b)),
                    (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 / b)),
                    (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a / *b as f64)),
                    _ => Err(ExecutionError::TypeError(
                        "Cannot divide these types".into(),
                    )),
                }
            }
            BinaryOperator::Modulo => match (left, right) {
                (Value::Integer(a), Value::Integer(b)) => {
                    if *b == 0 {
                        Err(ExecutionError::DivisionByZero)
                    } else {
                        Ok(Value::Integer(a % b))
                    }
                }
                _ => Err(ExecutionError::TypeError("Modulo requires integers".into())),
            },
            BinaryOperator::Eq => Ok(Value::Boolean(left == right)),
            BinaryOperator::NotEq => Ok(Value::Boolean(left != right)),
            BinaryOperator::Lt => Ok(Value::Boolean(left < right)),
            BinaryOperator::LtEq => Ok(Value::Boolean(left <= right)),
            BinaryOperator::Gt => Ok(Value::Boolean(left > right)),
            BinaryOperator::GtEq => Ok(Value::Boolean(left >= right)),
            BinaryOperator::And => match (left, right) {
                (Value::Boolean(a), Value::Boolean(b)) => Ok(Value::Boolean(*a && *b)),
                _ => Ok(Value::Boolean(left.is_truthy() && right.is_truthy())),
            },
            BinaryOperator::Or => match (left, right) {
                (Value::Boolean(a), Value::Boolean(b)) => Ok(Value::Boolean(*a || *b)),
                _ => Ok(Value::Boolean(left.is_truthy() || right.is_truthy())),
            },
            BinaryOperator::Concat => {
                Ok(Value::String(format!("{}{}", left, right)))
            }
        }
    }

    fn apply_unary_op(&self, op: &UnaryOperator, val: &Value) -> Result<Value, ExecutionError> {
        match op {
            UnaryOperator::Not => match val {
                Value::Boolean(b) => Ok(Value::Boolean(!b)),
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Boolean(!val.is_truthy())),
            },
            UnaryOperator::Minus => match val {
                Value::Integer(n) => Ok(Value::Integer(-n)),
                Value::Float(f) => Ok(Value::Float(-f)),
                Value::Null => Ok(Value::Null),
                _ => Err(ExecutionError::TypeError(
                    "Cannot negate this type".into(),
                )),
            },
            UnaryOperator::Plus => match val {
                Value::Integer(_) | Value::Float(_) | Value::Null => Ok(val.clone()),
                _ => Err(ExecutionError::TypeError(
                    "Unary plus requires number".into(),
                )),
            },
        }
    }

    fn like_match(&self, text: &str, pattern: &str) -> bool {
        // Convert SQL LIKE pattern to regex-like matching
        let mut text_chars = text.chars().peekable();
        let mut pattern_chars = pattern.chars().peekable();

        self.like_match_impl(&mut text_chars, &mut pattern_chars)
    }

    fn like_match_impl(
        &self,
        text: &mut std::iter::Peekable<std::str::Chars>,
        pattern: &mut std::iter::Peekable<std::str::Chars>,
    ) -> bool {
        loop {
            match (pattern.peek(), text.peek()) {
                (None, None) => return true,
                (None, Some(_)) => return false,
                (Some('%'), _) => {
                    pattern.next();
                    if pattern.peek().is_none() {
                        return true;
                    }
                    // Try matching the rest at every position
                    loop {
                        let mut pattern_clone = pattern.clone();
                        let mut text_clone = text.clone();
                        if self.like_match_impl(&mut text_clone, &mut pattern_clone) {
                            return true;
                        }
                        if text.next().is_none() {
                            return false;
                        }
                    }
                }
                (Some('_'), Some(_)) => {
                    pattern.next();
                    text.next();
                }
                (Some('_'), None) => return false,
                (Some(p), Some(t)) => {
                    if p.to_lowercase().next() == t.to_lowercase().next() {
                        pattern.next();
                        text.next();
                    } else {
                        return false;
                    }
                }
                (Some(_), None) => return false,
            }
        }
    }

    fn expr_to_name(&self, expr: &Expr, idx: usize) -> String {
        match expr {
            Expr::Column(col_ref) => {
                if let Some(ref table) = col_ref.table {
                    format!("{}.{}", table, col_ref.column)
                } else {
                    col_ref.column.clone()
                }
            }
            Expr::Function { name, .. } => name.clone(),
            _ => format!("column{}", idx + 1),
        }
    }

    fn infer_expr_type(&self, expr: &Expr, table: &Table) -> DataType {
        match expr {
            Expr::Integer(_) => DataType::Integer,
            Expr::Float(_) => DataType::Float,
            Expr::String(_) => DataType::String,
            Expr::Boolean(_) => DataType::Boolean,
            Expr::Null => DataType::Null,
            Expr::Column(col_ref) => {
                if let Some(idx) = table.schema.column_index(&col_ref.column) {
                    table.schema.columns[idx].data_type.clone()
                } else {
                    DataType::String
                }
            }
            Expr::Function { name, .. } => {
                match name.to_uppercase().as_str() {
                    "COUNT" => DataType::Integer,
                    "SUM" | "AVG" => DataType::Float,
                    _ => DataType::String,
                }
            }
            Expr::BinaryOp { op, .. } => match op {
                BinaryOperator::And | BinaryOperator::Or => DataType::Boolean,
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => DataType::Boolean,
                BinaryOperator::Concat => DataType::String,
                _ => DataType::Float,
            },
            _ => DataType::String,
        }
    }
}

pub fn execute_query(ctx: &ExecutionContext, sql: &str) -> Result<Table, ExecutionError> {
    let mut parser = Parser::new(sql).map_err(|e| ExecutionError::ParseError(e.to_string()))?;
    let stmt = parser
        .parse()
        .map_err(|e| ExecutionError::ParseError(e.to_string()))?;
    let mut planner = Planner::new();
    let plan = planner
        .plan(&stmt)
        .map_err(|e| ExecutionError::PlanError(e))?;
    let mut executor = Executor::new(ctx);
    executor.execute(&plan)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_context() -> ExecutionContext {
        let mut ctx = ExecutionContext::new();

        // Create users table
        let users_schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
            Column::new("age", DataType::Integer),
        ]);
        let mut users = Table::new("users", users_schema);
        users.add_row(Row::new(vec![
            Value::Integer(1),
            Value::String("Alice".to_string()),
            Value::Integer(30),
        ]));
        users.add_row(Row::new(vec![
            Value::Integer(2),
            Value::String("Bob".to_string()),
            Value::Integer(25),
        ]));
        users.add_row(Row::new(vec![
            Value::Integer(3),
            Value::String("Charlie".to_string()),
            Value::Integer(35),
        ]));
        ctx.add_table(users);

        // Create orders table
        let orders_schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("user_id", DataType::Integer),
            Column::new("amount", DataType::Float),
        ]);
        let mut orders = Table::new("orders", orders_schema);
        orders.add_row(Row::new(vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Float(100.0),
        ]));
        orders.add_row(Row::new(vec![
            Value::Integer(2),
            Value::Integer(1),
            Value::Float(200.0),
        ]));
        orders.add_row(Row::new(vec![
            Value::Integer(3),
            Value::Integer(2),
            Value::Float(150.0),
        ]));
        ctx.add_table(orders);

        ctx
    }

    #[test]
    fn test_simple_select() {
        let ctx = create_test_context();
        let result = execute_query(&ctx, "SELECT * FROM users").unwrap();
        assert_eq!(result.row_count(), 3);
    }

    #[test]
    fn test_select_columns() {
        let ctx = create_test_context();
        let result = execute_query(&ctx, "SELECT name, age FROM users").unwrap();
        assert_eq!(result.column_count(), 2);
        assert_eq!(result.row_count(), 3);
    }

    #[test]
    fn test_where_clause() {
        let ctx = create_test_context();
        let result = execute_query(&ctx, "SELECT * FROM users WHERE age > 28").unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn test_order_by() {
        let ctx = create_test_context();
        let result = execute_query(&ctx, "SELECT * FROM users ORDER BY age DESC").unwrap();
        assert_eq!(result.row_count(), 3);
        // First row should be Charlie (age 35)
        assert_eq!(result.rows[0].values[2], Value::Integer(35));
    }

    #[test]
    fn test_limit() {
        let ctx = create_test_context();
        let result = execute_query(&ctx, "SELECT * FROM users LIMIT 2").unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn test_count() {
        let ctx = create_test_context();
        let result = execute_query(&ctx, "SELECT COUNT(*) FROM users").unwrap();
        assert_eq!(result.row_count(), 1);
        assert_eq!(result.rows[0].values[0], Value::Integer(3));
    }

    #[test]
    fn test_sum() {
        let ctx = create_test_context();
        let result = execute_query(&ctx, "SELECT SUM(amount) FROM orders").unwrap();
        assert_eq!(result.row_count(), 1);
        if let Value::Float(sum) = &result.rows[0].values[0] {
            assert!((sum - 450.0).abs() < 0.01);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_group_by() {
        let ctx = create_test_context();
        let result =
            execute_query(&ctx, "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id").unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn test_join() {
        let ctx = create_test_context();
        let result = execute_query(
            &ctx,
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();
        assert_eq!(result.row_count(), 3);
    }

    #[test]
    fn test_like() {
        let ctx = create_test_context();
        let result = execute_query(&ctx, "SELECT * FROM users WHERE name LIKE 'A%'").unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn test_in_clause() {
        let ctx = create_test_context();
        let result =
            execute_query(&ctx, "SELECT * FROM users WHERE name IN ('Alice', 'Bob')").unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn test_between() {
        let ctx = create_test_context();
        let result = execute_query(&ctx, "SELECT * FROM users WHERE age BETWEEN 25 AND 32").unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn test_distinct() {
        let ctx = create_test_context();
        let result = execute_query(&ctx, "SELECT DISTINCT user_id FROM orders").unwrap();
        assert_eq!(result.row_count(), 2);
    }
}
