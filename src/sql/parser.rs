use super::ast::*;
use super::lexer::{Lexer, Token, TokenKind};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Unexpected token: expected {expected}, found {found:?} at position {position}")]
    UnexpectedToken {
        expected: String,
        found: TokenKind,
        position: usize,
    },
    #[error("Unexpected end of input")]
    UnexpectedEof,
    #[error("Lexer error: {0}")]
    LexerError(String),
}

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    pub fn new(input: &str) -> Result<Self, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens = lexer.tokenize().map_err(ParseError::LexerError)?;
        Ok(Self { tokens, position: 0 })
    }

    pub fn parse(&mut self) -> Result<SelectStatement, ParseError> {
        self.parse_select()
    }

    fn parse_select(&mut self) -> Result<SelectStatement, ParseError> {
        self.expect(TokenKind::Select)?;

        let mut stmt = SelectStatement::new();

        // DISTINCT
        if self.check(&TokenKind::Distinct) {
            self.advance();
            stmt.distinct = true;
        } else if self.check(&TokenKind::All) {
            self.advance();
        }

        // Columns
        stmt.columns = self.parse_select_columns()?;

        // FROM
        if self.check(&TokenKind::From) {
            self.advance();
            stmt.from = Some(self.parse_from_clause()?);

            // JOINs
            while self.is_join_keyword() {
                stmt.joins.push(self.parse_join_clause()?);
            }
        }

        // WHERE
        if self.check(&TokenKind::Where) {
            self.advance();
            stmt.where_clause = Some(self.parse_expr()?);
        }

        // GROUP BY
        if self.check(&TokenKind::Group) {
            self.advance();
            self.expect(TokenKind::By)?;
            stmt.group_by = self.parse_expr_list()?;
        }

        // HAVING
        if self.check(&TokenKind::Having) {
            self.advance();
            stmt.having = Some(self.parse_expr()?);
        }

        // ORDER BY
        if self.check(&TokenKind::Order) {
            self.advance();
            self.expect(TokenKind::By)?;
            stmt.order_by = self.parse_order_by_list()?;
        }

        // LIMIT
        if self.check(&TokenKind::Limit) {
            self.advance();
            stmt.limit = Some(self.parse_integer()?);
        }

        // OFFSET
        if self.check(&TokenKind::Offset) {
            self.advance();
            stmt.offset = Some(self.parse_integer()?);
        }

        Ok(stmt)
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>, ParseError> {
        let mut columns = Vec::new();

        loop {
            columns.push(self.parse_select_column()?);

            if self.check(&TokenKind::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_select_column(&mut self) -> Result<SelectColumn, ParseError> {
        // Check for *
        if self.check(&TokenKind::Star) {
            self.advance();
            return Ok(SelectColumn::AllColumns);
        }

        // Check for table.*
        if let Some(TokenKind::Identifier(name)) = self.peek_kind() {
            let name = name.clone();
            if self.peek_next_kind() == Some(&TokenKind::Dot) {
                self.advance(); // consume identifier
                self.advance(); // consume dot
                if self.check(&TokenKind::Star) {
                    self.advance();
                    return Ok(SelectColumn::TableAllColumns(name));
                } else {
                    // It's table.column, reparse as expression
                    let column = self.parse_identifier()?;
                    let expr = Expr::Column(ColumnRef::with_table(name, column));
                    let alias = self.parse_optional_alias()?;
                    return Ok(SelectColumn::Expr { expr, alias });
                }
            }
        }

        // Regular expression
        let expr = self.parse_expr()?;
        let alias = self.parse_optional_alias()?;
        Ok(SelectColumn::Expr { expr, alias })
    }

    fn parse_optional_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.check(&TokenKind::As) {
            self.advance();
            Ok(Some(self.parse_identifier()?))
        } else if let Some(TokenKind::Identifier(_)) = self.peek_kind() {
            // Alias without AS keyword
            if !self.is_keyword() {
                Ok(Some(self.parse_identifier()?))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn parse_from_clause(&mut self) -> Result<FromClause, ParseError> {
        let table = self.parse_table_ref()?;
        Ok(FromClause { table })
    }

    fn parse_table_ref(&mut self) -> Result<TableRef, ParseError> {
        let name = self.parse_identifier()?;
        let alias = self.parse_optional_alias()?;
        Ok(TableRef { name, alias })
    }

    fn is_join_keyword(&self) -> bool {
        matches!(
            self.peek_kind(),
            Some(TokenKind::Join)
                | Some(TokenKind::Inner)
                | Some(TokenKind::Left)
                | Some(TokenKind::Right)
                | Some(TokenKind::Cross)
        )
    }

    fn parse_join_clause(&mut self) -> Result<JoinClause, ParseError> {
        let join_type = self.parse_join_type()?;
        self.expect(TokenKind::Join)?;
        let table = self.parse_table_ref()?;

        let condition = if join_type == JoinType::Cross {
            None
        } else if self.check(&TokenKind::On) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(JoinClause {
            join_type,
            table,
            condition,
        })
    }

    fn parse_join_type(&mut self) -> Result<JoinType, ParseError> {
        if self.check(&TokenKind::Inner) {
            self.advance();
            Ok(JoinType::Inner)
        } else if self.check(&TokenKind::Left) {
            self.advance();
            if self.check(&TokenKind::Outer) {
                self.advance();
            }
            Ok(JoinType::Left)
        } else if self.check(&TokenKind::Right) {
            self.advance();
            if self.check(&TokenKind::Outer) {
                self.advance();
            }
            Ok(JoinType::Right)
        } else if self.check(&TokenKind::Cross) {
            self.advance();
            Ok(JoinType::Cross)
        } else {
            // Just JOIN means INNER JOIN
            Ok(JoinType::Inner)
        }
    }

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByItem>, ParseError> {
        let mut items = Vec::new();

        loop {
            let expr = self.parse_expr()?;
            let ascending = if self.check(&TokenKind::Desc) {
                self.advance();
                false
            } else {
                if self.check(&TokenKind::Asc) {
                    self.advance();
                }
                true
            };
            items.push(OrderByItem { expr, ascending });

            if self.check(&TokenKind::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(items)
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut exprs = Vec::new();

        loop {
            exprs.push(self.parse_expr()?);

            if self.check(&TokenKind::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(exprs)
    }

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_and_expr()?;

        while self.check(&TokenKind::Or) {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_not_expr()?;

        while self.check(&TokenKind::And) {
            self.advance();
            let right = self.parse_not_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<Expr, ParseError> {
        if self.check(&TokenKind::Not) {
            self.advance();
            let expr = self.parse_not_expr()?;
            Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            })
        } else {
            self.parse_comparison_expr()
        }
    }

    fn parse_comparison_expr(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_additive_expr()?;

        loop {
            if self.check(&TokenKind::Is) {
                self.advance();
                let negated = if self.check(&TokenKind::Not) {
                    self.advance();
                    true
                } else {
                    false
                };
                self.expect(TokenKind::Null)?;
                left = Expr::IsNull {
                    expr: Box::new(left),
                    negated,
                };
            } else if self.check(&TokenKind::In) || (self.check(&TokenKind::Not) && self.peek_next_kind() == Some(&TokenKind::In)) {
                let negated = if self.check(&TokenKind::Not) {
                    self.advance();
                    true
                } else {
                    false
                };
                self.advance(); // consume IN
                self.expect(TokenKind::LParen)?;
                let list = self.parse_expr_list()?;
                self.expect(TokenKind::RParen)?;
                left = Expr::InList {
                    expr: Box::new(left),
                    list,
                    negated,
                };
            } else if self.check(&TokenKind::Like) || (self.check(&TokenKind::Not) && self.peek_next_kind() == Some(&TokenKind::Like)) {
                let negated = if self.check(&TokenKind::Not) {
                    self.advance();
                    true
                } else {
                    false
                };
                self.advance(); // consume LIKE
                let pattern = self.parse_additive_expr()?;
                left = Expr::Like {
                    expr: Box::new(left),
                    pattern: Box::new(pattern),
                    negated,
                };
            } else if self.check(&TokenKind::Between) || (self.check(&TokenKind::Not) && self.peek_next_kind() == Some(&TokenKind::Between)) {
                let negated = if self.check(&TokenKind::Not) {
                    self.advance();
                    true
                } else {
                    false
                };
                self.advance(); // consume BETWEEN
                let low = self.parse_additive_expr()?;
                self.expect(TokenKind::And)?;
                let high = self.parse_additive_expr()?;
                left = Expr::Between {
                    expr: Box::new(left),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated,
                };
            } else if let Some(op) = self.parse_comparison_op() {
                let right = self.parse_additive_expr()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    fn parse_comparison_op(&mut self) -> Option<BinaryOperator> {
        let op = match self.peek_kind() {
            Some(TokenKind::Eq) => Some(BinaryOperator::Eq),
            Some(TokenKind::NotEq) => Some(BinaryOperator::NotEq),
            Some(TokenKind::Lt) => Some(BinaryOperator::Lt),
            Some(TokenKind::LtEq) => Some(BinaryOperator::LtEq),
            Some(TokenKind::Gt) => Some(BinaryOperator::Gt),
            Some(TokenKind::GtEq) => Some(BinaryOperator::GtEq),
            _ => None,
        };

        if op.is_some() {
            self.advance();
        }

        op
    }

    fn parse_additive_expr(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_multiplicative_expr()?;

        loop {
            let op = match self.peek_kind() {
                Some(TokenKind::Plus) => Some(BinaryOperator::Add),
                Some(TokenKind::Minus) => Some(BinaryOperator::Subtract),
                Some(TokenKind::Concat) => Some(BinaryOperator::Concat),
                _ => None,
            };

            if let Some(op) = op {
                self.advance();
                let right = self.parse_multiplicative_expr()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    fn parse_multiplicative_expr(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_unary_expr()?;

        loop {
            let op = match self.peek_kind() {
                Some(TokenKind::Star) => Some(BinaryOperator::Multiply),
                Some(TokenKind::Slash) => Some(BinaryOperator::Divide),
                Some(TokenKind::Percent) => Some(BinaryOperator::Modulo),
                _ => None,
            };

            if let Some(op) = op {
                self.advance();
                let right = self.parse_unary_expr()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    fn parse_unary_expr(&mut self) -> Result<Expr, ParseError> {
        match self.peek_kind() {
            Some(TokenKind::Minus) => {
                self.advance();
                let expr = self.parse_unary_expr()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(expr),
                })
            }
            Some(TokenKind::Plus) => {
                self.advance();
                let expr = self.parse_unary_expr()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Plus,
                    expr: Box::new(expr),
                })
            }
            _ => self.parse_primary_expr(),
        }
    }

    fn parse_primary_expr(&mut self) -> Result<Expr, ParseError> {
        match self.peek_kind().cloned() {
            Some(TokenKind::Integer(n)) => {
                self.advance();
                Ok(Expr::Integer(n))
            }
            Some(TokenKind::Float(f)) => {
                self.advance();
                Ok(Expr::Float(f))
            }
            Some(TokenKind::String(s)) => {
                self.advance();
                Ok(Expr::String(s))
            }
            Some(TokenKind::True) => {
                self.advance();
                Ok(Expr::Boolean(true))
            }
            Some(TokenKind::False) => {
                self.advance();
                Ok(Expr::Boolean(false))
            }
            Some(TokenKind::Null) => {
                self.advance();
                Ok(Expr::Null)
            }
            Some(TokenKind::LParen) => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(TokenKind::RParen)?;
                Ok(expr)
            }
            Some(TokenKind::Case) => self.parse_case_expr(),
            Some(TokenKind::Count) | Some(TokenKind::Sum) | Some(TokenKind::Avg)
            | Some(TokenKind::Min) | Some(TokenKind::Max) => self.parse_aggregate_function(),
            Some(TokenKind::Identifier(_)) => self.parse_column_or_function(),
            _ => Err(self.unexpected_token("expression")),
        }
    }

    fn parse_case_expr(&mut self) -> Result<Expr, ParseError> {
        self.advance(); // consume CASE

        let operand = if !self.check(&TokenKind::When) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let mut when_clauses = Vec::new();
        while self.check(&TokenKind::When) {
            self.advance();
            let when_expr = self.parse_expr()?;
            self.expect(TokenKind::Then)?;
            let then_expr = self.parse_expr()?;
            when_clauses.push((when_expr, then_expr));
        }

        let else_clause = if self.check(&TokenKind::Else) {
            self.advance();
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        self.expect(TokenKind::End)?;

        Ok(Expr::Case {
            operand,
            when_clauses,
            else_clause,
        })
    }

    fn parse_aggregate_function(&mut self) -> Result<Expr, ParseError> {
        let name = match self.peek_kind() {
            Some(TokenKind::Count) => "COUNT",
            Some(TokenKind::Sum) => "SUM",
            Some(TokenKind::Avg) => "AVG",
            Some(TokenKind::Min) => "MIN",
            Some(TokenKind::Max) => "MAX",
            _ => return Err(self.unexpected_token("aggregate function")),
        }
        .to_string();

        self.advance();
        self.expect(TokenKind::LParen)?;

        let distinct = if self.check(&TokenKind::Distinct) {
            self.advance();
            true
        } else {
            false
        };

        let args = if self.check(&TokenKind::Star) {
            self.advance();
            vec![Expr::Column(ColumnRef::new("*"))]
        } else {
            self.parse_expr_list()?
        };

        self.expect(TokenKind::RParen)?;

        Ok(Expr::Function {
            name,
            args,
            distinct,
        })
    }

    fn parse_column_or_function(&mut self) -> Result<Expr, ParseError> {
        let name = self.parse_identifier()?;

        // Check if it's a function call
        if self.check(&TokenKind::LParen) {
            self.advance();
            let args = if self.check(&TokenKind::RParen) {
                Vec::new()
            } else {
                self.parse_expr_list()?
            };
            self.expect(TokenKind::RParen)?;
            return Ok(Expr::Function {
                name,
                args,
                distinct: false,
            });
        }

        // Check for table.column
        if self.check(&TokenKind::Dot) {
            self.advance();
            let column = self.parse_identifier()?;
            return Ok(Expr::Column(ColumnRef::with_table(name, column)));
        }

        Ok(Expr::Column(ColumnRef::new(name)))
    }

    fn parse_identifier(&mut self) -> Result<String, ParseError> {
        match self.peek_kind().cloned() {
            Some(TokenKind::Identifier(name)) => {
                self.advance();
                Ok(name)
            }
            _ => Err(self.unexpected_token("identifier")),
        }
    }

    fn parse_integer(&mut self) -> Result<u64, ParseError> {
        match self.peek_kind() {
            Some(TokenKind::Integer(n)) => {
                let n = *n;
                self.advance();
                Ok(n as u64)
            }
            _ => Err(self.unexpected_token("integer")),
        }
    }

    fn is_keyword(&self) -> bool {
        matches!(
            self.peek_kind(),
            Some(TokenKind::Select)
                | Some(TokenKind::From)
                | Some(TokenKind::Where)
                | Some(TokenKind::And)
                | Some(TokenKind::Or)
                | Some(TokenKind::Join)
                | Some(TokenKind::Inner)
                | Some(TokenKind::Left)
                | Some(TokenKind::Right)
                | Some(TokenKind::On)
                | Some(TokenKind::Group)
                | Some(TokenKind::By)
                | Some(TokenKind::Having)
                | Some(TokenKind::Order)
                | Some(TokenKind::Limit)
                | Some(TokenKind::Offset)
        )
    }

    fn peek_kind(&self) -> Option<&TokenKind> {
        self.tokens.get(self.position).map(|t| &t.kind)
    }

    fn peek_next_kind(&self) -> Option<&TokenKind> {
        self.tokens.get(self.position + 1).map(|t| &t.kind)
    }

    fn check(&self, kind: &TokenKind) -> bool {
        self.peek_kind() == Some(kind)
    }

    fn advance(&mut self) -> Option<&Token> {
        if self.position < self.tokens.len() {
            let token = &self.tokens[self.position];
            self.position += 1;
            Some(token)
        } else {
            None
        }
    }

    fn expect(&mut self, expected: TokenKind) -> Result<&Token, ParseError> {
        if self.check(&expected) {
            Ok(self.advance().unwrap())
        } else {
            Err(self.unexpected_token(&format!("{:?}", expected)))
        }
    }

    fn unexpected_token(&self, expected: &str) -> ParseError {
        match self.tokens.get(self.position) {
            Some(token) => ParseError::UnexpectedToken {
                expected: expected.to_string(),
                found: token.kind.clone(),
                position: token.position,
            },
            None => ParseError::UnexpectedEof,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let mut parser = Parser::new("SELECT * FROM users").unwrap();
        let stmt = parser.parse().unwrap();

        assert!(!stmt.distinct);
        assert_eq!(stmt.columns.len(), 1);
        assert!(matches!(stmt.columns[0], SelectColumn::AllColumns));
        assert!(stmt.from.is_some());
    }

    #[test]
    fn test_select_columns() {
        let mut parser = Parser::new("SELECT id, name AS user_name FROM users").unwrap();
        let stmt = parser.parse().unwrap();

        assert_eq!(stmt.columns.len(), 2);
    }

    #[test]
    fn test_where_clause() {
        let mut parser = Parser::new("SELECT * FROM users WHERE age > 18").unwrap();
        let stmt = parser.parse().unwrap();

        assert!(stmt.where_clause.is_some());
    }

    #[test]
    fn test_join() {
        let mut parser =
            Parser::new("SELECT * FROM users u JOIN orders o ON u.id = o.user_id").unwrap();
        let stmt = parser.parse().unwrap();

        assert_eq!(stmt.joins.len(), 1);
        assert_eq!(stmt.joins[0].join_type, JoinType::Inner);
    }

    #[test]
    fn test_group_by() {
        let mut parser =
            Parser::new("SELECT department, COUNT(*) FROM employees GROUP BY department").unwrap();
        let stmt = parser.parse().unwrap();

        assert_eq!(stmt.group_by.len(), 1);
    }

    #[test]
    fn test_order_by() {
        let mut parser = Parser::new("SELECT * FROM users ORDER BY name ASC, age DESC").unwrap();
        let stmt = parser.parse().unwrap();

        assert_eq!(stmt.order_by.len(), 2);
        assert!(stmt.order_by[0].ascending);
        assert!(!stmt.order_by[1].ascending);
    }

    #[test]
    fn test_limit_offset() {
        let mut parser = Parser::new("SELECT * FROM users LIMIT 10 OFFSET 20").unwrap();
        let stmt = parser.parse().unwrap();

        assert_eq!(stmt.limit, Some(10));
        assert_eq!(stmt.offset, Some(20));
    }

    #[test]
    fn test_aggregate_functions() {
        let mut parser = Parser::new("SELECT COUNT(*), SUM(amount), AVG(price) FROM orders").unwrap();
        let stmt = parser.parse().unwrap();

        assert_eq!(stmt.columns.len(), 3);
    }

    #[test]
    fn test_complex_where() {
        let mut parser = Parser::new(
            "SELECT * FROM users WHERE (age > 18 AND status = 'active') OR role = 'admin'",
        )
        .unwrap();
        let stmt = parser.parse().unwrap();

        assert!(stmt.where_clause.is_some());
    }

    #[test]
    fn test_in_clause() {
        let mut parser =
            Parser::new("SELECT * FROM users WHERE status IN ('active', 'pending')").unwrap();
        let stmt = parser.parse().unwrap();

        assert!(stmt.where_clause.is_some());
    }

    #[test]
    fn test_like_clause() {
        let mut parser = Parser::new("SELECT * FROM users WHERE name LIKE 'John%'").unwrap();
        let stmt = parser.parse().unwrap();

        assert!(stmt.where_clause.is_some());
    }

    #[test]
    fn test_between_clause() {
        let mut parser = Parser::new("SELECT * FROM users WHERE age BETWEEN 18 AND 65").unwrap();
        let stmt = parser.parse().unwrap();

        assert!(stmt.where_clause.is_some());
    }

    #[test]
    fn test_is_null() {
        let mut parser = Parser::new("SELECT * FROM users WHERE email IS NOT NULL").unwrap();
        let stmt = parser.parse().unwrap();

        assert!(stmt.where_clause.is_some());
    }
}
