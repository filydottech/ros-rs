//! `RouterOS` query expressions.
//!
//! Constructors build the ordered query words `RouterOS` sends. The builder
//! validates neither field names nor values: query words are `RouterOS` syntax,
//! and an application may use a field this library cannot know.
//!
//! ```
//! use ros_rs::query::{absent, and, equal};
//!
//! let expr = and(equal("type", "ether"), [absent("comment")]);
//! assert_eq!(expr.words(), vec!["?type=ether", "?-comment", "?#&"]);
//! ```

/// An immutable, ordered `RouterOS` query program.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Expr {
    words: Vec<String>,
}

impl Expr {
    fn new(words: Vec<String>) -> Self {
        Expr { words }
    }

    /// Return a fresh copy of the query words, in send order.
    #[must_use]
    pub fn words(&self) -> Vec<String> {
        self.words.clone()
    }

    /// Consume the expression and return its words, in send order.
    pub(crate) fn into_words(self) -> Vec<String> {
        self.words
    }

    /// Whether the expression contains no words.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.words.is_empty()
    }

    /// Match a field whose value equals `value`.
    pub fn equal(name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        Expr::new(vec![format!("?{}={}", name.as_ref(), value.as_ref())])
    }

    /// Match a field whose value is less than `value`.
    pub fn less_than(name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        Expr::new(vec![format!("?<{}={}", name.as_ref(), value.as_ref())])
    }

    /// Match a field whose value is greater than `value`.
    pub fn greater_than(name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        Expr::new(vec![format!("?>{}={}", name.as_ref(), value.as_ref())])
    }

    /// Match records where `name` exists.
    pub fn exists(name: impl AsRef<str>) -> Self {
        Expr::new(vec![format!("?{}", name.as_ref())])
    }

    /// Match records where `name` is absent.
    pub fn absent(name: impl AsRef<str>) -> Self {
        Expr::new(vec![format!("?-{}", name.as_ref())])
    }

    /// Invert `expression` with `RouterOS`'s postfix `?#!` operator.
    #[must_use]
    pub fn negate(expression: Expr) -> Self {
        let mut words = expression.words;
        words.push("?#!".to_owned());
        Expr::new(words)
    }

    /// Combine `first` and `rest` with `RouterOS`'s postfix `?#&` operator.
    ///
    /// A group needs an operand; a single operand emits no operator.
    pub fn and(first: Expr, rest: impl IntoIterator<Item = Expr>) -> Self {
        Expr::combine("?#&", first, rest)
    }

    /// Combine `first` and `rest` with `RouterOS`'s postfix `?#|` operator.
    ///
    /// A group needs an operand; a single operand emits no operator.
    pub fn or(first: Expr, rest: impl IntoIterator<Item = Expr>) -> Self {
        Expr::combine("?#|", first, rest)
    }

    fn combine(operator: &str, first: Expr, rest: impl IntoIterator<Item = Expr>) -> Self {
        let mut words = first.words;
        let mut count = 0usize;
        for expression in rest {
            words.extend(expression.words);
            count += 1;
        }
        for _ in 0..count {
            words.push(operator.to_owned());
        }
        Expr::new(words)
    }
}

/// Match a field whose value equals `value`.
pub fn equal(name: impl AsRef<str>, value: impl AsRef<str>) -> Expr {
    Expr::equal(name, value)
}

/// Match a field whose value is less than `value`.
pub fn less_than(name: impl AsRef<str>, value: impl AsRef<str>) -> Expr {
    Expr::less_than(name, value)
}

/// Match a field whose value is greater than `value`.
pub fn greater_than(name: impl AsRef<str>, value: impl AsRef<str>) -> Expr {
    Expr::greater_than(name, value)
}

/// Match records where `name` exists.
pub fn exists(name: impl AsRef<str>) -> Expr {
    Expr::exists(name)
}

/// Match records where `name` is absent.
pub fn absent(name: impl AsRef<str>) -> Expr {
    Expr::absent(name)
}

/// Invert `expression` with `RouterOS`'s postfix `?#!` operator.
#[must_use]
pub fn negate(expression: Expr) -> Expr {
    Expr::negate(expression)
}

/// Combine `first` and `rest` with `RouterOS`'s postfix `?#&` operator.
pub fn and(first: Expr, rest: impl IntoIterator<Item = Expr>) -> Expr {
    Expr::and(first, rest)
}

/// Combine `first` and `rest` with `RouterOS`'s postfix `?#|` operator.
pub fn or(first: Expr, rest: impl IntoIterator<Item = Expr>) -> Expr {
    Expr::or(first, rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_leaf_words() {
        assert_eq!(equal("name", "ether1").words(), vec!["?name=ether1"]);
        assert_eq!(less_than("mtu", "1500").words(), vec!["?<mtu=1500"]);
        assert_eq!(greater_than("mtu", "1500").words(), vec!["?>mtu=1500"]);
        assert_eq!(exists("comment").words(), vec!["?comment"]);
        assert_eq!(absent("comment").words(), vec!["?-comment"]);
    }

    #[test]
    fn composes_in_postfix_order() {
        let expression = and(
            equal("type", "ether"),
            [absent("comment"), or(exists("running"), [exists("slave")])],
        );
        assert_eq!(
            expression.words(),
            vec![
                "?type=ether",
                "?-comment",
                "?running",
                "?slave",
                "?#|",
                "?#&",
                "?#&",
            ]
        );
    }

    #[test]
    fn negate_appends_the_postfix_operator() {
        assert_eq!(
            negate(equal("disabled", "false")).words(),
            vec!["?disabled=false", "?#!"]
        );
    }

    #[test]
    fn a_group_with_one_operand_has_no_operator() {
        assert_eq!(and(equal("a", "b"), []).words(), vec!["?a=b"]);
        assert_eq!(or(equal("a", "b"), []).words(), vec!["?a=b"]);
    }

    #[test]
    fn returned_words_are_independent() {
        let expression = equal("name", "ether1");
        let mut words = expression.words();
        words.push("?injected".to_owned());
        assert_eq!(expression.words(), vec!["?name=ether1"]);

        let group = and(expression.clone(), [exists("comment")]);
        let mut words = group.words();
        words.clear();
        assert_eq!(group.words().len(), 3);
        assert_eq!(expression.words(), vec!["?name=ether1"]);
    }
}
