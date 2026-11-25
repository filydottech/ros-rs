//! Reply-sentence parsing.

use std::collections::BTreeMap;

use super::WireError;
use crate::record::Record;

/// The five reply words `RouterOS` sends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Reply,
    Done,
    Trap,
    Empty,
    Fatal,
}

impl Kind {
    pub(crate) fn word(self) -> &'static str {
        match self {
            Kind::Reply => "!re",
            Kind::Done => "!done",
            Kind::Trap => "!trap",
            Kind::Empty => "!empty",
            Kind::Fatal => "!fatal",
        }
    }
}

/// One parsed reply sentence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Reply {
    pub(crate) kind: Kind,
    /// The routing tag; `None` when the sentence carried no non-empty `.tag=`.
    pub(crate) tag: Option<String>,
    /// `=name=value` words. A repeated name keeps its last value.
    pub(crate) attributes: Record,
    /// `.name=value` words other than `.tag`.
    pub(crate) api_attributes: BTreeMap<String, String>,
    /// Words that are neither attributes nor API attributes. A `!fatal`
    /// reason arrives this way.
    pub(crate) plain: Vec<String>,
    /// The joined `plain` words of a `!fatal` reply.
    pub(crate) fatal_reason: String,
}

impl Reply {
    pub(crate) fn tag(&self) -> Option<&str> {
        self.tag.as_deref()
    }
}

/// Parse one sentence. Only the five reply words are accepted; any other first
/// word is a protocol error.
pub(crate) fn parse_reply(words: &[String]) -> Result<Reply, WireError> {
    let Some(first) = words.first() else {
        return Err(WireError::protocol("empty reply sentence"));
    };
    let kind = match first.as_str() {
        "!re" => Kind::Reply,
        "!done" => Kind::Done,
        "!trap" => Kind::Trap,
        "!empty" => Kind::Empty,
        "!fatal" => Kind::Fatal,
        other => {
            return Err(WireError::protocol(format!("unknown reply word {other:?}")));
        }
    };

    let mut reply = Reply {
        kind,
        tag: None,
        attributes: Record::new(),
        api_attributes: BTreeMap::new(),
        plain: Vec::new(),
        fatal_reason: String::new(),
    };

    for word in &words[1..] {
        if let Some(body) = word.strip_prefix('=') {
            let (name, value) = split_attribute(body, "attribute")?;
            reply.attributes.insert(name, value);
        } else if let Some(body) = word.strip_prefix('.') {
            let (name, value) = split_attribute(body, "API attribute")?;
            if name == "tag" {
                // RouterOS sends an empty .tag= on replies that answer no
                // request; treat that as untagged.
                reply.tag = (!value.is_empty()).then_some(value);
            } else {
                reply.api_attributes.insert(name, value);
            }
        } else {
            reply.plain.push(word.clone());
        }
    }

    if kind == Kind::Fatal {
        reply.fatal_reason = reply.plain.join(" ");
    }
    Ok(reply)
}

fn split_attribute(body: &str, kind: &str) -> Result<(String, String), WireError> {
    let Some(separator) = body.find('=') else {
        if body.is_empty() {
            return Err(WireError::protocol(format!("empty {kind} key")));
        }
        return Ok((body.to_owned(), String::new()));
    };
    let name = &body[..separator];
    if name.is_empty() {
        return Err(WireError::protocol(format!("empty {kind} key")));
    }
    Ok((name.to_owned(), body[separator + 1..].to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn parses_every_kind() {
        for (word, kind) in [
            ("!re", Kind::Reply),
            ("!done", Kind::Done),
            ("!trap", Kind::Trap),
            ("!empty", Kind::Empty),
            ("!fatal", Kind::Fatal),
        ] {
            let reply = parse_reply(&words(&[word])).unwrap();
            assert_eq!(reply.kind, kind);
        }
    }

    #[test]
    fn parses_attributes_and_tags() {
        let reply = parse_reply(&words(&[
            "!re",
            "=name=ether1",
            "=comment=a=b",
            ".tag=7",
            ".section=main",
        ]))
        .unwrap();
        assert_eq!(reply.tag.as_deref(), Some("7"));
        assert_eq!(reply.attributes["name"], "ether1");
        assert_eq!(reply.attributes["comment"], "a=b");
        assert_eq!(reply.api_attributes["section"], "main");
        assert_eq!(reply.plain, [] as [std::string::String; 0]);
    }

    #[test]
    fn repeated_attributes_keep_the_last_value() {
        let reply = parse_reply(&words(&["!done", "=ret=a", "=ret=b"])).unwrap();
        assert_eq!(reply.attributes["ret"], "b");
    }

    #[test]
    fn an_empty_tag_means_untagged() {
        let reply = parse_reply(&words(&["!done", ".tag="])).unwrap();
        assert_eq!(reply.tag, None);
    }

    #[test]
    fn fatal_joins_plain_words_as_the_reason() {
        let reply = parse_reply(&words(&["!fatal", "session", "closed"])).unwrap();
        assert_eq!(reply.fatal_reason, "session closed");
        assert_eq!(reply.plain, words(&["session", "closed"]));
    }

    #[test]
    fn rejects_empty_and_unknown_sentences() {
        assert!(parse_reply(&[]).is_err());
        assert!(parse_reply(&words(&["!unknown"])).is_err());
    }

    #[test]
    fn rejects_attribute_words_with_an_empty_key() {
        assert!(parse_reply(&words(&["!re", "="])).is_err());
        assert!(parse_reply(&words(&["!re", ".="])).is_err());
        assert!(parse_reply(&words(&["!re", "==value"])).is_err());
    }

    #[test]
    fn an_attribute_without_an_equals_sign_has_an_empty_value() {
        let reply = parse_reply(&words(&["!re", "=disabled"])).unwrap();
        assert_eq!(reply.attributes["disabled"], "");
    }
}
