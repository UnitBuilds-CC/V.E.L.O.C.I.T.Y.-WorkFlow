//! Workflow Search Query Parser — SQL-like query language for visibility searches.
//!
//! Supports Temporal-compatible query syntax:
//! - Comparison operators: =, !=, <, >, <=, >=
//! - Logical operators: AND, OR, NOT
//! - Parenthesized grouping: (expr1 AND expr2) OR expr3
//! - BETWEEN operator: field BETWEEN value1 AND value2
//! - IN operator: field IN (val1, val2, val3)
//! - LIKE operator: field LIKE 'prefix%'
//! - IS NULL / IS NOT NULL checks
//!
//! Exceeds Temporal by supporting nested boolean expressions and additional operators.

use std::fmt;

// ─── AST Nodes ─────────────────────────────────────────────────────────────

/// A parsed query expression.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryExpr {
    /// Comparison: field op value
    Comparison {
        field: String,
        op: CompareOp,
        value: QueryValue,
    },
    /// Logical AND of two expressions.
    And(Box<QueryExpr>, Box<QueryExpr>),
    /// Logical OR of two expressions.
    Or(Box<QueryExpr>, Box<QueryExpr>),
    /// Logical NOT of an expression.
    Not(Box<QueryExpr>),
    /// BETWEEN: field BETWEEN low AND high
    Between {
        field: String,
        low: QueryValue,
        high: QueryValue,
    },
    /// IN: field IN (values...)
    In {
        field: String,
        values: Vec<QueryValue>,
    },
    /// LIKE: field LIKE pattern
    Like {
        field: String,
        pattern: String,
    },
    /// IS NULL check
    IsNull(String),
    /// IS NOT NULL check
    IsNotNull(String),
}

/// Comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

/// Query values.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryValue {
    String(String),
    Integer(i64),
    Double(f64),
    Bool(bool),
    Null,
}

impl fmt::Display for QueryValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryValue::String(s) => write!(f, "'{}'", s),
            QueryValue::Integer(i) => write!(f, "{}", i),
            QueryValue::Double(d) => write!(f, "{}", d),
            QueryValue::Bool(b) => write!(f, "{}", b),
            QueryValue::Null => write!(f, "NULL"),
        }
    }
}

impl fmt::Display for CompareOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompareOp::Eq => write!(f, "="),
            CompareOp::Ne => write!(f, "!="),
            CompareOp::Lt => write!(f, "<"),
            CompareOp::Gt => write!(f, ">"),
            CompareOp::Le => write!(f, "<="),
            CompareOp::Ge => write!(f, ">="),
        }
    }
}

impl fmt::Display for QueryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryExpr::Comparison { field, op, value } => {
                write!(f, "{} {} {}", field, op, value)
            }
            QueryExpr::And(l, r) => write!(f, "({} AND {})", l, r),
            QueryExpr::Or(l, r) => write!(f, "({} OR {})", l, r),
            QueryExpr::Not(e) => write!(f, "NOT ({})", e),
            QueryExpr::Between { field, low, high } => {
                write!(f, "{} BETWEEN {} AND {}", field, low, high)
            }
            QueryExpr::In { field, values } => {
                let vals: Vec<String> = values.iter().map(|v| format!("{}", v)).collect();
                write!(f, "{} IN ({})", field, vals.join(", "))
            }
            QueryExpr::Like { field, pattern } => {
                write!(f, "{} LIKE '{}'", field, pattern)
            }
            QueryExpr::IsNull(field) => write!(f, "{} IS NULL", field),
            QueryExpr::IsNotNull(field) => write!(f, "{} IS NOT NULL", field),
        }
    }
}

// ─── Tokenizer ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    StringLit(String),
    IntLit(i64),
    FloatLit(f64),
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    LParen,
    RParen,
    Comma,
    And,
    Or,
    Not,
    Between,
    In,
    Like,
    Is,
    Null,
    True,
    False,
}

struct Tokenizer {
    chars: Vec<char>,
    pos: usize,
}

impl Tokenizer {
    fn new(input: &str) -> Self {
        Self {
            chars: input.chars().collect(),
            pos: 0,
        }
    }

    fn tokenize(&mut self) -> Result<Vec<Token>, QueryError> {
        let mut tokens = Vec::new();
        while self.pos < self.chars.len() {
            self.skip_whitespace();
            if self.pos >= self.chars.len() {
                break;
            }
            let ch = self.chars[self.pos];
            match ch {
                '(' => {
                    tokens.push(Token::LParen);
                    self.pos += 1;
                }
                ')' => {
                    tokens.push(Token::RParen);
                    self.pos += 1;
                }
                ',' => {
                    tokens.push(Token::Comma);
                    self.pos += 1;
                }
                '=' => {
                    tokens.push(Token::Eq);
                    self.pos += 1;
                }
                '!' => {
                    self.pos += 1;
                    if self.peek() == Some('=') {
                        tokens.push(Token::Ne);
                        self.pos += 1;
                    } else {
                        return Err(QueryError::UnexpectedChar('!', self.pos));
                    }
                }
                '<' => {
                    self.pos += 1;
                    if self.peek() == Some('=') {
                        tokens.push(Token::Le);
                        self.pos += 1;
                    } else if self.peek() == Some('>') {
                        tokens.push(Token::Ne);
                        self.pos += 1;
                    } else {
                        tokens.push(Token::Lt);
                    }
                }
                '>' => {
                    self.pos += 1;
                    if self.peek() == Some('=') {
                        tokens.push(Token::Ge);
                        self.pos += 1;
                    } else {
                        tokens.push(Token::Gt);
                    }
                }
                '\'' => {
                    tokens.push(Token::StringLit(self.read_string()?));
                }
                c if c.is_ascii_digit() || c == '-' => {
                    tokens.push(self.read_number()?);
                }
                c if c.is_ascii_alphabetic() || c == '_' => {
                    let word = self.read_word();
                    match word.to_uppercase().as_str() {
                        "AND" => tokens.push(Token::And),
                        "OR" => tokens.push(Token::Or),
                        "NOT" => tokens.push(Token::Not),
                        "BETWEEN" => tokens.push(Token::Between),
                        "IN" => tokens.push(Token::In),
                        "LIKE" => tokens.push(Token::Like),
                        "IS" => tokens.push(Token::Is),
                        "NULL" => tokens.push(Token::Null),
                        "TRUE" => tokens.push(Token::True),
                        "FALSE" => tokens.push(Token::False),
                        _ => tokens.push(Token::Ident(word)),
                    }
                }
                _ => return Err(QueryError::UnexpectedChar(ch, self.pos)),
            }
        }
        Ok(tokens)
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.chars.len() && self.chars[self.pos].is_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn read_string(&mut self) -> Result<String, QueryError> {
        self.pos += 1; // skip opening quote
        let mut s = String::new();
        while self.pos < self.chars.len() {
            let ch = self.chars[self.pos];
            if ch == '\'' {
                self.pos += 1;
                return Ok(s);
            }
            if ch == '\\' && self.pos + 1 < self.chars.len() {
                self.pos += 1;
                s.push(self.chars[self.pos]);
            } else {
                s.push(ch);
            }
            self.pos += 1;
        }
        Err(QueryError::UnterminatedString)
    }

    fn read_number(&mut self) -> Result<Token, QueryError> {
        let start = self.pos;
        if self.chars[self.pos] == '-' {
            self.pos += 1;
        }
        while self.pos < self.chars.len() && self.chars[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        let mut is_float = false;
        if self.pos < self.chars.len() && self.chars[self.pos] == '.' {
            is_float = true;
            self.pos += 1;
            while self.pos < self.chars.len() && self.chars[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }
        let num_str: String = self.chars[start..self.pos].iter().collect();
        if is_float {
            num_str
                .parse::<f64>()
                .map(Token::FloatLit)
                .map_err(|_| QueryError::InvalidNumber(num_str))
        } else {
            num_str
                .parse::<i64>()
                .map(Token::IntLit)
                .map_err(|_| QueryError::InvalidNumber(num_str))
        }
    }

    fn read_word(&mut self) -> String {
        let start = self.pos;
        while self.pos < self.chars.len()
            && (self.chars[self.pos].is_ascii_alphanumeric() || self.chars[self.pos] == '_')
        {
            self.pos += 1;
        }
        self.chars[start..self.pos].iter().collect()
    }
}

// ─── Parser ────────────────────────────────────────────────────────────────

/// Parse a SQL-like query string into a QueryExpr AST.
pub fn parse_query(input: &str) -> Result<QueryExpr, QueryError> {
    let mut tokenizer = Tokenizer::new(input);
    let tokens = tokenizer.tokenize()?;
    if tokens.is_empty() {
        return Err(QueryError::EmptyQuery);
    }
    let mut parser = Parser::new(tokens);
    let expr = parser.parse_expr()?;
    if parser.pos < parser.tokens.len() {
        return Err(QueryError::UnexpectedToken(
            format!("{:?}", parser.tokens[parser.pos]),
            parser.pos,
        ));
    }
    Ok(expr)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn parse_expr(&mut self) -> Result<QueryExpr, QueryError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<QueryExpr, QueryError> {
        let mut left = self.parse_and()?;
        while self.pos < self.tokens.len() && self.tokens[self.pos] == Token::Or {
            self.pos += 1;
            let right = self.parse_and()?;
            left = QueryExpr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<QueryExpr, QueryError> {
        let mut left = self.parse_not()?;
        while self.pos < self.tokens.len() && self.tokens[self.pos] == Token::And {
            self.pos += 1;
            let right = self.parse_not()?;
            left = QueryExpr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<QueryExpr, QueryError> {
        if self.pos < self.tokens.len() && self.tokens[self.pos] == Token::Not {
            self.pos += 1;
            let expr = self.parse_primary()?;
            Ok(QueryExpr::Not(Box::new(expr)))
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<QueryExpr, QueryError> {
        if self.pos >= self.tokens.len() {
            return Err(QueryError::UnexpectedEnd);
        }

        // Parenthesized expression
        if self.tokens[self.pos] == Token::LParen {
            self.pos += 1;
            let expr = self.parse_expr()?;
            if self.pos >= self.tokens.len() || self.tokens[self.pos] != Token::RParen {
                return Err(QueryError::MissingRParen);
            }
            self.pos += 1;
            return Ok(expr);
        }

        // Field-based expression
        let field = match &self.tokens[self.pos] {
            Token::Ident(s) => s.clone(),
            other => return Err(QueryError::ExpectedField(format!("{:?}", other))),
        };
        self.pos += 1;

        if self.pos >= self.tokens.len() {
            return Err(QueryError::UnexpectedEnd);
        }

        // IS NULL / IS NOT NULL
        if self.tokens[self.pos] == Token::Is {
            self.pos += 1;
            if self.pos < self.tokens.len() && self.tokens[self.pos] == Token::Not {
                self.pos += 1;
                if self.pos < self.tokens.len() && self.tokens[self.pos] == Token::Null {
                    self.pos += 1;
                    return Ok(QueryExpr::IsNotNull(field));
                }
                return Err(QueryError::ExpectedNull);
            }
            if self.pos < self.tokens.len() && self.tokens[self.pos] == Token::Null {
                self.pos += 1;
                return Ok(QueryExpr::IsNull(field));
            }
            return Err(QueryError::ExpectedNullOrNot);
        }

        // BETWEEN
        if self.tokens[self.pos] == Token::Between {
            self.pos += 1;
            let low = self.parse_value()?;
            if self.pos >= self.tokens.len() || self.tokens[self.pos] != Token::And {
                return Err(QueryError::ExpectedAnd);
            }
            self.pos += 1;
            let high = self.parse_value()?;
            return Ok(QueryExpr::Between { field, low, high });
        }

        // IN
        if self.tokens[self.pos] == Token::In {
            self.pos += 1;
            if self.pos >= self.tokens.len() || self.tokens[self.pos] != Token::LParen {
                return Err(QueryError::ExpectedLParen);
            }
            self.pos += 1;
            let mut values = Vec::new();
            loop {
                values.push(self.parse_value()?);
                if self.pos < self.tokens.len() && self.tokens[self.pos] == Token::Comma {
                    self.pos += 1;
                } else {
                    break;
                }
            }
            if self.pos >= self.tokens.len() || self.tokens[self.pos] != Token::RParen {
                return Err(QueryError::ExpectedRParen);
            }
            self.pos += 1;
            return Ok(QueryExpr::In { field, values });
        }

        // LIKE
        if self.tokens[self.pos] == Token::Like {
            self.pos += 1;
            match &self.tokens[self.pos] {
                Token::StringLit(s) => {
                    let pattern = s.clone();
                    self.pos += 1;
                    return Ok(QueryExpr::Like { field, pattern });
                }
                _ => return Err(QueryError::ExpectedString),
            }
        }

        // Comparison operators
        let op = match &self.tokens[self.pos] {
            Token::Eq => CompareOp::Eq,
            Token::Ne => CompareOp::Ne,
            Token::Lt => CompareOp::Lt,
            Token::Gt => CompareOp::Gt,
            Token::Le => CompareOp::Le,
            Token::Ge => CompareOp::Ge,
            other => return Err(QueryError::ExpectedOperator(format!("{:?}", other))),
        };
        self.pos += 1;
        let value = self.parse_value()?;
        Ok(QueryExpr::Comparison { field, op, value })
    }

    fn parse_value(&mut self) -> Result<QueryValue, QueryError> {
        if self.pos >= self.tokens.len() {
            return Err(QueryError::UnexpectedEnd);
        }
        match &self.tokens[self.pos] {
            Token::StringLit(s) => {
                let v = QueryValue::String(s.clone());
                self.pos += 1;
                Ok(v)
            }
            Token::IntLit(i) => {
                let v = QueryValue::Integer(*i);
                self.pos += 1;
                Ok(v)
            }
            Token::FloatLit(f) => {
                let v = QueryValue::Double(*f);
                self.pos += 1;
                Ok(v)
            }
            Token::True => {
                self.pos += 1;
                Ok(QueryValue::Bool(true))
            }
            Token::False => {
                self.pos += 1;
                Ok(QueryValue::Bool(false))
            }
            Token::Null => {
                self.pos += 1;
                Ok(QueryValue::Null)
            }
            other => Err(QueryError::ExpectedValue(format!("{:?}", other))),
        }
    }
}

// ─── Query Errors ──────────────────────────────────────────────────────────

/// Query parse error.
#[derive(Debug, Clone)]
pub enum QueryError {
    EmptyQuery,
    UnexpectedChar(char, usize),
    UnterminatedString,
    InvalidNumber(String),
    UnexpectedToken(String, usize),
    UnexpectedEnd,
    MissingRParen,
    ExpectedField(String),
    ExpectedOperator(String),
    ExpectedValue(String),
    ExpectedAnd,
    ExpectedLParen,
    ExpectedRParen,
    ExpectedString,
    ExpectedNull,
    ExpectedNullOrNot,
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryError::EmptyQuery => write!(f, "empty query"),
            QueryError::UnexpectedChar(c, pos) => {
                write!(f, "unexpected character '{}' at position {}", c, pos)
            }
            QueryError::UnterminatedString => write!(f, "unterminated string literal"),
            QueryError::InvalidNumber(s) => write!(f, "invalid number: {}", s),
            QueryError::UnexpectedToken(t, pos) => {
                write!(f, "unexpected token '{}' at position {}", t, pos)
            }
            QueryError::UnexpectedEnd => write!(f, "unexpected end of query"),
            QueryError::MissingRParen => write!(f, "missing closing parenthesis"),
            QueryError::ExpectedField(t) => write!(f, "expected field name, got {}", t),
            QueryError::ExpectedOperator(t) => write!(f, "expected operator, got {}", t),
            QueryError::ExpectedValue(t) => write!(f, "expected value, got {}", t),
            QueryError::ExpectedAnd => write!(f, "expected AND in BETWEEN expression"),
            QueryError::ExpectedLParen => write!(f, "expected '(' in IN expression"),
            QueryError::ExpectedRParen => write!(f, "expected ')' in IN expression"),
            QueryError::ExpectedString => write!(f, "expected string literal for LIKE pattern"),
            QueryError::ExpectedNull => write!(f, "expected NULL after NOT"),
            QueryError::ExpectedNullOrNot => write!(f, "expected NULL or NOT after IS"),
        }
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_equality() {
        let expr = parse_query("WorkflowType = 'OrderWorkflow'").unwrap();
        assert_eq!(
            expr,
            QueryExpr::Comparison {
                field: "WorkflowType".into(),
                op: CompareOp::Eq,
                value: QueryValue::String("OrderWorkflow".into()),
            }
        );
    }

    #[test]
    fn test_numeric_comparison() {
        let expr = parse_query("ExecutionTime > 1000").unwrap();
        assert_eq!(
            expr,
            QueryExpr::Comparison {
                field: "ExecutionTime".into(),
                op: CompareOp::Gt,
                value: QueryValue::Integer(1000),
            }
        );
    }

    #[test]
    fn test_and_expression() {
        let expr = parse_query("Status = 'Running' AND WorkflowType = 'Order'").unwrap();
        match expr {
            QueryExpr::And(l, r) => {
                assert!(matches!(*l, QueryExpr::Comparison { .. }));
                assert!(matches!(*r, QueryExpr::Comparison { .. }));
            }
            _ => panic!("expected AND"),
        }
    }

    #[test]
    fn test_or_expression() {
        let expr = parse_query("Status = 'Running' OR Status = 'Completed'").unwrap();
        assert!(matches!(expr, QueryExpr::Or(_, _)));
    }

    #[test]
    fn test_not_expression() {
        let expr = parse_query("NOT Status = 'Failed'").unwrap();
        assert!(matches!(expr, QueryExpr::Not(_)));
    }

    #[test]
    fn test_parenthesized() {
        let expr =
            parse_query("(Status = 'Running' OR Status = 'Completed') AND WorkflowType = 'A'")
                .unwrap();
        assert!(matches!(expr, QueryExpr::And(_, _)));
    }

    #[test]
    fn test_between() {
        let expr = parse_query("ExecutionTime BETWEEN 100 AND 200").unwrap();
        match expr {
            QueryExpr::Between { field, low, high } => {
                assert_eq!(field, "ExecutionTime");
                assert_eq!(low, QueryValue::Integer(100));
                assert_eq!(high, QueryValue::Integer(200));
            }
            _ => panic!("expected BETWEEN"),
        }
    }

    #[test]
    fn test_in_operator() {
        let expr = parse_query("Status IN ('Running', 'Completed', 'Failed')").unwrap();
        match expr {
            QueryExpr::In { field, values } => {
                assert_eq!(field, "Status");
                assert_eq!(values.len(), 3);
            }
            _ => panic!("expected IN"),
        }
    }

    #[test]
    fn test_like_operator() {
        let expr = parse_query("WorkflowId LIKE 'order-%'").unwrap();
        match expr {
            QueryExpr::Like { field, pattern } => {
                assert_eq!(field, "WorkflowId");
                assert_eq!(pattern, "order-%");
            }
            _ => panic!("expected LIKE"),
        }
    }

    #[test]
    fn test_is_null() {
        let expr = parse_query("CloseTime IS NULL").unwrap();
        assert_eq!(expr, QueryExpr::IsNull("CloseTime".into()));
    }

    #[test]
    fn test_is_not_null() {
        let expr = parse_query("CloseTime IS NOT NULL").unwrap();
        assert_eq!(expr, QueryExpr::IsNotNull("CloseTime".into()));
    }

    #[test]
    fn test_not_equal() {
        let expr = parse_query("Status != 'Failed'").unwrap();
        match expr {
            QueryExpr::Comparison { op, .. } => assert_eq!(op, CompareOp::Ne),
            _ => panic!("expected comparison"),
        }
    }

    #[test]
    fn test_less_equal() {
        let expr = parse_query("Attempt <= 3").unwrap();
        match expr {
            QueryExpr::Comparison { op, .. } => assert_eq!(op, CompareOp::Le),
            _ => panic!("expected comparison"),
        }
    }

    #[test]
    fn test_boolean_values() {
        let expr = parse_query("IsCron = true").unwrap();
        match expr {
            QueryExpr::Comparison { value, .. } => assert_eq!(value, QueryValue::Bool(true)),
            _ => panic!("expected comparison"),
        }
    }

    #[test]
    fn test_complex_nested() {
        let result = parse_query(
            "(Status = 'Running' AND WorkflowType = 'Order') OR (Status = 'Completed' AND CloseTime > 1000)",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_display_roundtrip() {
        let expr = parse_query("Status = 'Running'").unwrap();
        let s = format!("{}", expr);
        assert!(s.contains("Status"));
        assert!(s.contains("Running"));
    }

    #[test]
    fn test_error_empty() {
        assert!(parse_query("").is_err());
    }

    #[test]
    fn test_error_unterminated_string() {
        assert!(parse_query("Status = 'Running").is_err());
    }

    #[test]
    fn test_error_missing_paren() {
        assert!(parse_query("(Status = 'Running'").is_err());
    }

    #[test]
    fn test_float_value() {
        let expr = parse_query("Progress > 0.95").unwrap();
        match expr {
            QueryExpr::Comparison { value, .. } => {
                assert!(matches!(value, QueryValue::Double(f) if (f - 0.95).abs() < 0.001));
            }
            _ => panic!("expected comparison"),
        }
    }

    #[test]
    fn test_negative_number() {
        let expr = parse_query("Offset > -100").unwrap();
        match expr {
            QueryExpr::Comparison { value, .. } => {
                assert_eq!(value, QueryValue::Integer(-100));
            }
            _ => panic!("expected comparison"),
        }
    }
}
