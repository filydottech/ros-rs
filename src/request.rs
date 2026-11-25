//! Outbound command payloads.

use crate::error::Error;
use crate::query::Expr;

/// One outbound `=name=value` word.
///
/// `name` must not be empty, start with a period, or contain an equals sign,
/// and names must not repeat within a request. `value` may be empty, which
/// sends a flag-like `=name=` word.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attribute {
    /// The attribute name, without a leading `=`.
    pub name: String,
    /// The attribute value. May be empty.
    pub value: String,
}

impl Attribute {
    /// Build an attribute.
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Attribute {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// The payload [`crate::Client::command`] and [`crate::Client::stream`] accept.
///
/// Attributes are sent in insertion order, so the wire bytes do not depend on
/// map iteration. A proplist becomes one optional `.proplist=a,b` word. Query
/// words are sent unchanged; `RouterOS` treats query order as significant.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Request {
    attributes: Vec<Attribute>,
    proplist: Vec<String>,
    query: Vec<String>,
}

impl Request {
    /// An empty request.
    #[must_use]
    pub fn new() -> Self {
        Request::default()
    }

    /// Append one outbound attribute word.
    #[must_use]
    pub fn attribute(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.push(Attribute::new(name, value));
        self
    }

    /// Append one attribute value.
    #[must_use]
    pub fn push_attribute(mut self, attribute: Attribute) -> Self {
        self.attributes.push(attribute);
        self
    }

    /// Set the `.proplist` names. An empty list sends no proplist word.
    #[must_use]
    pub fn proplist<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.proplist = names.into_iter().map(Into::into).collect();
        self
    }

    /// Append the words of a query expression.
    #[must_use]
    pub fn query(mut self, expression: Expr) -> Self {
        self.query.extend(expression.into_words());
        self
    }

    /// Append one raw query word, which must start with `?`.
    #[must_use]
    pub fn raw_query(mut self, word: impl Into<String>) -> Self {
        self.query.push(word.into());
        self
    }

    /// The outbound attributes, in send order.
    #[must_use]
    pub fn attributes(&self) -> &[Attribute] {
        &self.attributes
    }

    /// The requested property names.
    #[must_use]
    pub fn proplist_names(&self) -> &[String] {
        &self.proplist
    }

    /// The query words, in send order.
    #[must_use]
    pub fn query_words(&self) -> &[String] {
        &self.query
    }

    /// Validate the request and build the sentence words, without a tag.
    ///
    /// Validation errors name the field and its index, never the caller's
    /// value: a malformed attribute or query can contain a credential.
    pub(crate) fn sentence_words(&self, path: &str) -> Result<Vec<String>, Error> {
        if path.is_empty() {
            return Err(invalid("path is required"));
        }

        let mut words = Vec::with_capacity(
            1 + self.attributes.len() + usize::from(!self.proplist.is_empty()) + self.query.len(),
        );
        words.push(path.to_owned());

        let mut seen = std::collections::BTreeSet::new();
        for (index, attribute) in self.attributes.iter().enumerate() {
            if attribute.name.is_empty() {
                return Err(invalid(format!("attribute {index} has an empty name")));
            }
            if attribute.name.starts_with('.') {
                return Err(invalid(format!(
                    "attribute {index} name starts with a period"
                )));
            }
            if attribute.name.contains('=') {
                return Err(invalid(format!(
                    "attribute {index} name contains an equals sign"
                )));
            }
            let bytes = attribute.value.len() as u64;
            if bytes > u64::from(u32::MAX) {
                return Err(invalid(format!("attribute {index} value is too large")));
            }
            if !seen.insert(attribute.name.as_str()) {
                return Err(invalid(format!(
                    "attribute {index} repeats an earlier name"
                )));
            }
            words.push(format!("={}={}", attribute.name, attribute.value));
        }

        if !self.proplist.is_empty() {
            words.push(format!(".proplist={}", self.proplist.join(",")));
        }
        for (index, query) in self.query.iter().enumerate() {
            if !query.starts_with('?') {
                return Err(invalid(format!(
                    "query {index} must start with a question mark"
                )));
            }
            words.push(query.clone());
        }
        if words.iter().any(String::is_empty) {
            return Err(invalid("request contains an empty word"));
        }
        Ok(words)
    }
}

fn invalid(detail: impl Into<String>) -> Error {
    Error::InvalidRequest {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{and, equal};

    #[test]
    fn builds_words_in_insertion_order() {
        let request = Request::new()
            .attribute("name", "ether1")
            .attribute("disabled", "")
            .proplist(["name", "type"])
            .query(and(equal("type", "ether"), []));
        let words = request.sentence_words("/interface/print").unwrap();
        assert_eq!(
            words,
            vec![
                "/interface/print",
                "=name=ether1",
                "=disabled=",
                ".proplist=name,type",
                "?type=ether",
            ]
        );
    }

    #[test]
    fn requires_a_path() {
        let error = Request::new().sentence_words("").unwrap_err();
        assert!(matches!(error, Error::InvalidRequest { .. }));
    }

    #[test]
    fn rejects_invalid_attribute_names() {
        for request in [
            Request::new().attribute("", "v"),
            Request::new().attribute(".secret", "v"),
            Request::new().attribute("a=b", "v"),
            Request::new().attribute("name", "a").attribute("name", "b"),
        ] {
            let error = request.sentence_words("/x").unwrap_err();
            assert!(matches!(error, Error::InvalidRequest { .. }));
        }
    }

    #[test]
    fn error_messages_name_the_index_not_the_value() {
        let error = Request::new()
            .attribute("name", "hunter2")
            .attribute("name", "hunter2")
            .sentence_words("/x")
            .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("attribute 1"));
        assert!(!message.contains("hunter2"));
    }

    #[test]
    fn rejects_a_raw_query_word_without_a_question_mark() {
        let error = Request::new()
            .raw_query("name=ether1")
            .sentence_words("/x")
            .unwrap_err();
        assert!(matches!(error, Error::InvalidRequest { .. }));
    }

    #[test]
    fn accepts_raw_query_words() {
        let words = Request::new()
            .raw_query("?type=ether")
            .sentence_words("/interface/print")
            .unwrap();
        assert_eq!(words, vec!["/interface/print", "?type=ether"]);
    }

    #[test]
    fn an_empty_proplist_sends_no_word() {
        let words = Request::new()
            .proplist(Vec::<String>::new())
            .sentence_words("/x")
            .unwrap();
        assert_eq!(words, vec!["/x"]);
    }
}
