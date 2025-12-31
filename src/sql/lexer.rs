use std::iter::Peekable;
use std::str::Chars;

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    // Keywords
    Select,
    From,
    Where,
    And,
    Or,
    Not,
    As,
    Join,
    Inner,
    Left,
    Right,
    Outer,
    On,
    Group,
    By,
    Having,
    Order,
    Asc,
    Desc,
    Limit,
    Offset,
    Distinct,
    All,
    Null,
    Is,
    In,
    Like,
    Between,
    Case,
    When,
    Then,
    Else,
    End,
    True,
    False,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Cross,

    // Literals
    Integer(i64),
    Float(f64),
    String(String),
    Identifier(String),

    // Operators
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    Concat,

    // Punctuation
    Comma,
    Dot,
    Semicolon,
    LParen,
    RParen,

    // Special
    Eof,
}

#[derive(Debug, Clone)]
pub struct Token {
    pub kind: TokenKind,
    pub position: usize,
}

impl Token {
    pub fn new(kind: TokenKind, position: usize) -> Self {
        Self { kind, position }
    }
}

pub struct Lexer<'a> {
    input: &'a str,
    chars: Peekable<Chars<'a>>,
    position: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            chars: input.chars().peekable(),
            position: 0,
        }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>, String> {
        let mut tokens = Vec::new();

        loop {
            let token = self.next_token()?;
            let is_eof = token.kind == TokenKind::Eof;
            tokens.push(token);
            if is_eof {
                break;
            }
        }

        Ok(tokens)
    }

    fn next_token(&mut self) -> Result<Token, String> {
        self.skip_whitespace();

        let position = self.position;

        match self.peek() {
            None => Ok(Token::new(TokenKind::Eof, position)),
            Some(c) => match c {
                // Single-char tokens
                ',' => {
                    self.advance();
                    Ok(Token::new(TokenKind::Comma, position))
                }
                '.' => {
                    self.advance();
                    Ok(Token::new(TokenKind::Dot, position))
                }
                ';' => {
                    self.advance();
                    Ok(Token::new(TokenKind::Semicolon, position))
                }
                '(' => {
                    self.advance();
                    Ok(Token::new(TokenKind::LParen, position))
                }
                ')' => {
                    self.advance();
                    Ok(Token::new(TokenKind::RParen, position))
                }
                '+' => {
                    self.advance();
                    Ok(Token::new(TokenKind::Plus, position))
                }
                '-' => {
                    self.advance();
                    // Check for comment
                    if self.peek() == Some('-') {
                        self.skip_line_comment();
                        self.next_token()
                    } else {
                        Ok(Token::new(TokenKind::Minus, position))
                    }
                }
                '*' => {
                    self.advance();
                    Ok(Token::new(TokenKind::Star, position))
                }
                '/' => {
                    self.advance();
                    if self.peek() == Some('*') {
                        self.skip_block_comment()?;
                        self.next_token()
                    } else {
                        Ok(Token::new(TokenKind::Slash, position))
                    }
                }
                '%' => {
                    self.advance();
                    Ok(Token::new(TokenKind::Percent, position))
                }
                '=' => {
                    self.advance();
                    Ok(Token::new(TokenKind::Eq, position))
                }
                '<' => {
                    self.advance();
                    if self.peek() == Some('=') {
                        self.advance();
                        Ok(Token::new(TokenKind::LtEq, position))
                    } else if self.peek() == Some('>') {
                        self.advance();
                        Ok(Token::new(TokenKind::NotEq, position))
                    } else {
                        Ok(Token::new(TokenKind::Lt, position))
                    }
                }
                '>' => {
                    self.advance();
                    if self.peek() == Some('=') {
                        self.advance();
                        Ok(Token::new(TokenKind::GtEq, position))
                    } else {
                        Ok(Token::new(TokenKind::Gt, position))
                    }
                }
                '!' => {
                    self.advance();
                    if self.peek() == Some('=') {
                        self.advance();
                        Ok(Token::new(TokenKind::NotEq, position))
                    } else {
                        Err(format!("Unexpected character '!' at position {}", position))
                    }
                }
                '|' => {
                    self.advance();
                    if self.peek() == Some('|') {
                        self.advance();
                        Ok(Token::new(TokenKind::Concat, position))
                    } else {
                        Err(format!("Unexpected character '|' at position {}", position))
                    }
                }
                '\'' => self.read_string(),
                '"' => self.read_quoted_identifier(),
                c if c.is_ascii_digit() => self.read_number(),
                c if c.is_alphabetic() || c == '_' => self.read_identifier_or_keyword(),
                c => Err(format!("Unexpected character '{}' at position {}", c, position)),
            },
        }
    }

    fn peek(&mut self) -> Option<char> {
        self.chars.peek().copied()
    }

    fn advance(&mut self) -> Option<char> {
        let c = self.chars.next();
        if c.is_some() {
            self.position += 1;
        }
        c
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn skip_line_comment(&mut self) {
        while let Some(c) = self.peek() {
            self.advance();
            if c == '\n' {
                break;
            }
        }
    }

    fn skip_block_comment(&mut self) -> Result<(), String> {
        self.advance(); // consume '*'
        loop {
            match self.advance() {
                None => return Err("Unterminated block comment".to_string()),
                Some('*') => {
                    if self.peek() == Some('/') {
                        self.advance();
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
    }

    fn read_string(&mut self) -> Result<Token, String> {
        let position = self.position;
        self.advance(); // consume opening quote
        let mut value = String::new();

        loop {
            match self.advance() {
                None => return Err("Unterminated string literal".to_string()),
                Some('\'') => {
                    // Check for escaped quote
                    if self.peek() == Some('\'') {
                        value.push('\'');
                        self.advance();
                    } else {
                        break;
                    }
                }
                Some(c) => value.push(c),
            }
        }

        Ok(Token::new(TokenKind::String(value), position))
    }

    fn read_quoted_identifier(&mut self) -> Result<Token, String> {
        let position = self.position;
        self.advance(); // consume opening quote
        let mut value = String::new();

        loop {
            match self.advance() {
                None => return Err("Unterminated quoted identifier".to_string()),
                Some('"') => {
                    if self.peek() == Some('"') {
                        value.push('"');
                        self.advance();
                    } else {
                        break;
                    }
                }
                Some(c) => value.push(c),
            }
        }

        Ok(Token::new(TokenKind::Identifier(value), position))
    }

    fn read_number(&mut self) -> Result<Token, String> {
        let position = self.position;
        let start = self.position;
        let mut has_dot = false;

        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                self.advance();
            } else if c == '.' && !has_dot {
                has_dot = true;
                self.advance();
            } else {
                break;
            }
        }

        let value = &self.input[start..self.position];
        if has_dot {
            value
                .parse::<f64>()
                .map(|f| Token::new(TokenKind::Float(f), position))
                .map_err(|_| format!("Invalid float literal: {}", value))
        } else {
            value
                .parse::<i64>()
                .map(|i| Token::new(TokenKind::Integer(i), position))
                .map_err(|_| format!("Invalid integer literal: {}", value))
        }
    }

    fn read_identifier_or_keyword(&mut self) -> Result<Token, String> {
        let position = self.position;
        let start = self.position;

        while let Some(c) = self.peek() {
            if c.is_alphanumeric() || c == '_' {
                self.advance();
            } else {
                break;
            }
        }

        let value = &self.input[start..self.position];
        let kind = match value.to_uppercase().as_str() {
            "SELECT" => TokenKind::Select,
            "FROM" => TokenKind::From,
            "WHERE" => TokenKind::Where,
            "AND" => TokenKind::And,
            "OR" => TokenKind::Or,
            "NOT" => TokenKind::Not,
            "AS" => TokenKind::As,
            "JOIN" => TokenKind::Join,
            "INNER" => TokenKind::Inner,
            "LEFT" => TokenKind::Left,
            "RIGHT" => TokenKind::Right,
            "OUTER" => TokenKind::Outer,
            "CROSS" => TokenKind::Cross,
            "ON" => TokenKind::On,
            "GROUP" => TokenKind::Group,
            "BY" => TokenKind::By,
            "HAVING" => TokenKind::Having,
            "ORDER" => TokenKind::Order,
            "ASC" => TokenKind::Asc,
            "DESC" => TokenKind::Desc,
            "LIMIT" => TokenKind::Limit,
            "OFFSET" => TokenKind::Offset,
            "DISTINCT" => TokenKind::Distinct,
            "ALL" => TokenKind::All,
            "NULL" => TokenKind::Null,
            "IS" => TokenKind::Is,
            "IN" => TokenKind::In,
            "LIKE" => TokenKind::Like,
            "BETWEEN" => TokenKind::Between,
            "CASE" => TokenKind::Case,
            "WHEN" => TokenKind::When,
            "THEN" => TokenKind::Then,
            "ELSE" => TokenKind::Else,
            "END" => TokenKind::End,
            "TRUE" => TokenKind::True,
            "FALSE" => TokenKind::False,
            "COUNT" => TokenKind::Count,
            "SUM" => TokenKind::Sum,
            "AVG" => TokenKind::Avg,
            "MIN" => TokenKind::Min,
            "MAX" => TokenKind::Max,
            _ => TokenKind::Identifier(value.to_string()),
        };

        Ok(Token::new(kind, position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let mut lexer = Lexer::new("SELECT * FROM users");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens[0].kind, TokenKind::Select);
        assert_eq!(tokens[1].kind, TokenKind::Star);
        assert_eq!(tokens[2].kind, TokenKind::From);
        assert!(matches!(tokens[3].kind, TokenKind::Identifier(ref s) if s == "users"));
        assert_eq!(tokens[4].kind, TokenKind::Eof);
    }

    #[test]
    fn test_string_literal() {
        let mut lexer = Lexer::new("'hello world'");
        let tokens = lexer.tokenize().unwrap();

        assert!(matches!(tokens[0].kind, TokenKind::String(ref s) if s == "hello world"));
    }

    #[test]
    fn test_escaped_string() {
        let mut lexer = Lexer::new("'it''s a test'");
        let tokens = lexer.tokenize().unwrap();

        assert!(matches!(tokens[0].kind, TokenKind::String(ref s) if s == "it's a test"));
    }

    #[test]
    fn test_numbers() {
        let mut lexer = Lexer::new("42 3.14");
        let tokens = lexer.tokenize().unwrap();

        assert!(matches!(tokens[0].kind, TokenKind::Integer(42)));
        assert!(matches!(tokens[1].kind, TokenKind::Float(f) if (f - 3.14).abs() < 0.001));
    }

    #[test]
    fn test_operators() {
        let mut lexer = Lexer::new("= <> != < > <= >=");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens[0].kind, TokenKind::Eq);
        assert_eq!(tokens[1].kind, TokenKind::NotEq);
        assert_eq!(tokens[2].kind, TokenKind::NotEq);
        assert_eq!(tokens[3].kind, TokenKind::Lt);
        assert_eq!(tokens[4].kind, TokenKind::Gt);
        assert_eq!(tokens[5].kind, TokenKind::LtEq);
        assert_eq!(tokens[6].kind, TokenKind::GtEq);
    }

    #[test]
    fn test_complex_query() {
        let mut lexer = Lexer::new(
            "SELECT id, name AS user_name FROM users WHERE age >= 18 ORDER BY name ASC LIMIT 10",
        );
        let tokens = lexer.tokenize().unwrap();

        assert!(tokens.len() > 10);
        assert_eq!(tokens[0].kind, TokenKind::Select);
    }

    #[test]
    fn test_line_comment() {
        let mut lexer = Lexer::new("SELECT -- this is a comment\n* FROM users");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens[0].kind, TokenKind::Select);
        assert_eq!(tokens[1].kind, TokenKind::Star);
        assert_eq!(tokens[2].kind, TokenKind::From);
    }

    #[test]
    fn test_block_comment() {
        let mut lexer = Lexer::new("SELECT /* comment */ * FROM users");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens[0].kind, TokenKind::Select);
        assert_eq!(tokens[1].kind, TokenKind::Star);
        assert_eq!(tokens[2].kind, TokenKind::From);
    }
}
