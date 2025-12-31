use super::ast::*;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    // Scan a table
    TableScan {
        table_name: String,
        alias: Option<String>,
    },

    // Project (SELECT columns)
    Projection {
        input: Box<LogicalPlan>,
        exprs: Vec<(Expr, Option<String>)>, // (expression, alias)
        distinct: bool,
    },

    // Filter (WHERE)
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },

    // Join
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        condition: Option<Expr>,
    },

    // Aggregation (GROUP BY)
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<(Expr, Option<String>)>,
        having: Option<Expr>,
    },

    // Sort (ORDER BY)
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderByItem>,
    },

    // Limit
    Limit {
        input: Box<LogicalPlan>,
        limit: u64,
        offset: Option<u64>,
    },

    // Cross Join (cartesian product)
    CrossJoin {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },

    // Empty (for SELECT without FROM)
    Empty,
}

pub struct Planner {
    table_aliases: HashMap<String, String>,
}

impl Default for Planner {
    fn default() -> Self {
        Self::new()
    }
}

impl Planner {
    pub fn new() -> Self {
        Self {
            table_aliases: HashMap::new(),
        }
    }

    pub fn plan(&mut self, stmt: &SelectStatement) -> Result<LogicalPlan, String> {
        self.table_aliases.clear();

        // Build the base plan from FROM clause and JOINs
        let mut plan = self.plan_from_clause(stmt)?;

        // Apply WHERE filter
        if let Some(ref predicate) = stmt.where_clause {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: predicate.clone(),
            };
        }

        // Check if we have aggregations
        let has_aggregates = self.has_aggregates(&stmt.columns);

        if has_aggregates || !stmt.group_by.is_empty() {
            // Extract aggregates and group by
            let (aggregates, group_exprs) = self.extract_aggregates(stmt)?;

            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: group_exprs,
                aggregates,
                having: stmt.having.clone(),
            };

            // Project the final columns (aggregates already computed)
            let projection_exprs = self.plan_projection_after_aggregate(stmt)?;
            plan = LogicalPlan::Projection {
                input: Box::new(plan),
                exprs: projection_exprs,
                distinct: stmt.distinct,
            };
        } else {
            // Simple projection
            let projection_exprs = self.plan_projection(stmt)?;
            plan = LogicalPlan::Projection {
                input: Box::new(plan),
                exprs: projection_exprs,
                distinct: stmt.distinct,
            };
        }

        // Apply ORDER BY
        if !stmt.order_by.is_empty() {
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: stmt.order_by.clone(),
            };
        }

        // Apply LIMIT/OFFSET
        if let Some(limit) = stmt.limit {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit,
                offset: stmt.offset,
            };
        }

        Ok(plan)
    }

    fn plan_from_clause(&mut self, stmt: &SelectStatement) -> Result<LogicalPlan, String> {
        let base_plan = match &stmt.from {
            Some(from) => {
                let table_name = from.table.name.clone();
                let alias = from.table.alias.clone();

                if let Some(ref a) = alias {
                    self.table_aliases.insert(a.clone(), table_name.clone());
                }
                self.table_aliases
                    .insert(table_name.clone(), table_name.clone());

                LogicalPlan::TableScan { table_name, alias }
            }
            None => LogicalPlan::Empty,
        };

        // Apply JOINs
        let mut plan = base_plan;
        for join in &stmt.joins {
            let right_table = join.table.name.clone();
            let right_alias = join.table.alias.clone();

            if let Some(ref a) = right_alias {
                self.table_aliases.insert(a.clone(), right_table.clone());
            }
            self.table_aliases
                .insert(right_table.clone(), right_table.clone());

            let right_plan = LogicalPlan::TableScan {
                table_name: right_table,
                alias: right_alias,
            };

            plan = match join.join_type {
                JoinType::Cross => LogicalPlan::CrossJoin {
                    left: Box::new(plan),
                    right: Box::new(right_plan),
                },
                _ => LogicalPlan::Join {
                    left: Box::new(plan),
                    right: Box::new(right_plan),
                    join_type: join.join_type.clone(),
                    condition: join.condition.clone(),
                },
            };
        }

        Ok(plan)
    }

    fn has_aggregates(&self, columns: &[SelectColumn]) -> bool {
        for col in columns {
            if let SelectColumn::Expr { expr, .. } = col {
                if self.expr_has_aggregate(expr) {
                    return true;
                }
            }
        }
        false
    }

    fn expr_has_aggregate(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function { name, .. } => {
                matches!(
                    name.to_uppercase().as_str(),
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
                )
            }
            Expr::BinaryOp { left, right, .. } => {
                self.expr_has_aggregate(left) || self.expr_has_aggregate(right)
            }
            Expr::UnaryOp { expr, .. } => self.expr_has_aggregate(expr),
            _ => false,
        }
    }

    fn extract_aggregates(
        &self,
        stmt: &SelectStatement,
    ) -> Result<(Vec<(Expr, Option<String>)>, Vec<Expr>), String> {
        let mut aggregates = Vec::new();

        for col in &stmt.columns {
            match col {
                SelectColumn::Expr { expr, alias } => {
                    if self.expr_has_aggregate(expr) {
                        aggregates.push((expr.clone(), alias.clone()));
                    }
                }
                SelectColumn::AllColumns | SelectColumn::TableAllColumns(_) => {
                    // These shouldn't appear with aggregates typically
                }
            }
        }

        Ok((aggregates, stmt.group_by.clone()))
    }

    fn plan_projection(
        &self,
        stmt: &SelectStatement,
    ) -> Result<Vec<(Expr, Option<String>)>, String> {
        let mut exprs = Vec::new();

        for col in &stmt.columns {
            match col {
                SelectColumn::AllColumns => {
                    // This will be expanded at execution time
                    exprs.push((Expr::Column(ColumnRef::new("*")), None));
                }
                SelectColumn::TableAllColumns(table) => {
                    exprs.push((
                        Expr::Column(ColumnRef::with_table(table.clone(), "*")),
                        None,
                    ));
                }
                SelectColumn::Expr { expr, alias } => {
                    exprs.push((expr.clone(), alias.clone()));
                }
            }
        }

        Ok(exprs)
    }

    fn plan_projection_after_aggregate(
        &self,
        stmt: &SelectStatement,
    ) -> Result<Vec<(Expr, Option<String>)>, String> {
        let mut exprs = Vec::new();

        for col in &stmt.columns {
            if let SelectColumn::Expr { expr, alias } = col {
                exprs.push((expr.clone(), alias.clone()));
            }
        }

        Ok(exprs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::Parser;

    #[test]
    fn test_simple_plan() {
        let mut parser = Parser::new("SELECT * FROM users").unwrap();
        let stmt = parser.parse().unwrap();
        let mut planner = Planner::new();
        let plan = planner.plan(&stmt).unwrap();

        // Should be: Projection -> TableScan
        assert!(matches!(plan, LogicalPlan::Projection { .. }));
    }

    #[test]
    fn test_filter_plan() {
        let mut parser = Parser::new("SELECT * FROM users WHERE age > 18").unwrap();
        let stmt = parser.parse().unwrap();
        let mut planner = Planner::new();
        let plan = planner.plan(&stmt).unwrap();

        // Should be: Projection -> Filter -> TableScan
        assert!(matches!(plan, LogicalPlan::Projection { .. }));
    }

    #[test]
    fn test_join_plan() {
        let mut parser =
            Parser::new("SELECT * FROM users u JOIN orders o ON u.id = o.user_id").unwrap();
        let stmt = parser.parse().unwrap();
        let mut planner = Planner::new();
        let plan = planner.plan(&stmt).unwrap();

        assert!(matches!(plan, LogicalPlan::Projection { .. }));
    }

    #[test]
    fn test_aggregate_plan() {
        let mut parser =
            Parser::new("SELECT department, COUNT(*) FROM employees GROUP BY department").unwrap();
        let stmt = parser.parse().unwrap();
        let mut planner = Planner::new();
        let plan = planner.plan(&stmt).unwrap();

        // Should have Aggregate node
        assert!(matches!(plan, LogicalPlan::Projection { .. }));
    }
}
