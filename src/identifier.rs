use std::fmt;
use std::borrow::Borrow;

/// An identifier is the name for a database object.
/// Table names, column names, constraint names are identifiers.
///
/// Allowed characters:
///
/// * `a-z`
/// * `A-Z`
/// * `0-9`
/// * `_`
/// * Space (allowed in SQL with quoted identifiers)
///
/// Other rules:
///
/// * Identifiers must have a minimum length of 1.
/// * Identifiers cannot start with a number (0-9) or space.
/// * Identifiers are case insensitive.
///
/// When stored and compared, identifiers must be folded into a canonical,
/// lower-case representation. This process is known as normalization.
#[derive(PartialEq, Eq, Clone)]
pub struct Identifier {
    value: String
}

impl Identifier {
    pub fn new<B>(value: B) -> Option<Identifier>
    where B: Borrow<str>
    {
        match normalize(value.borrow()) {
            Some(s) => Some(Identifier {
                value: s
            }),
            None => None
        }
    }

    /// Converts a string into an identifier without validation.
    /// This should only be called if you're _really_ sure the string is
    /// normalized.
    pub unsafe fn from_string(value: String) -> Identifier {
        debug_assert_eq!(normalize(value.as_slice()), Some(value.clone()));

        Identifier {
            value: value
        }
    }

    pub fn into_inner(self) -> String { self.value }
}

impl Str for Identifier {
    fn as_slice(&self) -> &str { self.value.as_slice() }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str(self.value.as_slice())
    }
}

impl fmt::Debug for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{:?}", self.value)
    }
}

fn normalize(value: &str) -> Option<String> {
    use std::ascii::AsciiExt;

    fn is_valid(value: &str) -> bool {
        if value.len() > 0 {
            let c = value.char_at(0);
            // Test if the first character is not a digit or space
            match c {
                '0'...'9' | ' ' => false,
                _ => {
                    value.chars().all(|c| {
                        match c {
                            'a'...'z' | 'A'...'Z' | '0'...'9' | '_' | ' ' => true,
                            _ => false
                        }
                    })
                }
            }
        } else {
            false
        }
    }

    if is_valid(value) {
        Some(value.chars().map(|c| {
            c.to_ascii_lowercase()
        }).collect())
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use super::Identifier;

    #[test]
    fn test_identifier() {
        fn cmp(a: &'static str, b: &'static str) -> bool {
            Identifier::new(a).unwrap().as_slice() == b
        }

        fn cmp_none(a: &'static str) -> bool {
            Identifier::new(a).is_none()
        }

        assert!(cmp("AbCdEfG", "abcdefg"));
        assert!(cmp("a0123456789", "a0123456789"));
        assert!(cmp("Hello World", "hello world"));
        assert!(cmp_none(""));
        assert!(cmp_none("1a"));
        assert!(cmp_none(" abc "));
        assert!(cmp("_1a", "_1a"));
    }
}
