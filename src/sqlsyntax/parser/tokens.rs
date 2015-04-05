use super::super::lexer::Token;
use super::{RuleError, RuleResult};

#[derive(Copy, Clone)]
pub struct Tokens<'a> {
    tokens: &'a [Token]
}

impl<'a> Tokens<'a> {
    fn peek_clone(&self) -> Option<Token> {
        if self.tokens.len() > 0 {
            Some(self.tokens[0].clone())
        } else {
            None
        }
    }

    pub fn new(tokens: &'a [Token]) -> Tokens<'a> {
        Tokens {
            tokens: tokens
        }
    }

    pub fn expecting(&self, expecting_message: &'static str) -> RuleError {
        RuleError::ExpectingFirst(expecting_message, self.peek_clone())
    }

    pub fn expect_no_more_tokens(&self) -> RuleResult<()> {
        if self.tokens.len() > 0 {
            Err(self.expecting("no more tokens"))
        } else {
            Ok(())
        }
    }

    pub fn pop_token_expecting(&mut self, token: &Token, expecting_message: &'static str) -> RuleResult<()> {
        if self.pop_if_token(token) { Ok(()) }
        else { Err(self.expecting(expecting_message)) }
    }

    pub fn pop_if_token(&mut self, token: &Token) -> bool {
        if self.tokens.len() > 0 {
            if &self.tokens[0] == token {
                self.tokens = &self.tokens[1..];
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    #[must_use]
    pub fn pop_if_number(&mut self) -> Option<String> {
        if self.tokens.len() > 0 {
            let token = &self.tokens[0];

            if let &Token::Number(ref s) = token {
                let ident = s.clone();
                self.tokens = &self.tokens[1..];
                Some(ident)
            } else {
                None
            }
        } else {
            None
        }
    }

    #[must_use]
    pub fn pop_if_string_literal(&mut self) -> Option<String> {
        if self.tokens.len() > 0 {
            let token = &self.tokens[0];

            if let &Token::StringLiteral(ref s) = token {
                let ident = s.clone();
                self.tokens = &self.tokens[1..];
                Some(ident)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn pop_if_ident(&mut self) -> Option<String> {
        if self.tokens.len() > 0 {
            let token = &self.tokens[0];

            if let &Token::Ident(ref s) = token {
                let ident = s.clone();
                self.tokens = &self.tokens[1..];
                Some(ident)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn pop_ident_expecting(&mut self, expecting_message: &'static str) -> RuleResult<String> {
        if self.tokens.len() > 0 {
            let token = &self.tokens[0];

            if let &Token::Ident(ref s) = token {
                let ident = s.clone();
                self.tokens = &self.tokens[1..];
                Ok(ident)
            } else {
                Err(self.expecting(expecting_message))
            }
        } else {
            Err(self.expecting(expecting_message))
        }
    }

    pub fn pop_number_expecting(&mut self, expecting_message: &'static str) -> RuleResult<String> {
        if self.tokens.len() > 0 {
            let token = &self.tokens[0];

            if let &Token::Number(ref s) = token {
                let ident = s.clone();
                self.tokens = &self.tokens[1..];
                Ok(ident)
            } else {
                Err(self.expecting(expecting_message))
            }
        } else {
            Err(self.expecting(expecting_message))
        }
    }

    pub fn pop_expecting(&mut self, expecting_message: &'static str) -> RuleResult<&'a Token> {
        if self.tokens.len() > 0 {
            let token = &self.tokens[0];
            self.tokens = &self.tokens[1..];
            Ok(token)
        } else {
            Err(self.expecting(expecting_message))
        }
    }
}
