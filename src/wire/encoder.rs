//! Sentence encoder.

use super::WireError;
use super::length::{MAX_WIRE_LENGTH, encode_length};
use super::limits::Limits;

/// Writes `RouterOS` sentences. A word is a UTF-8 payload and its prefix carries
/// the byte length, never the character count.
pub(crate) struct Encoder {
    limits: Limits,
}

impl Encoder {
    pub(crate) fn new(limits: Limits) -> Self {
        Encoder {
            limits: limits.with_defaults(),
        }
    }

    /// Encode one complete sentence: every word prefixed by its length, then
    /// the zero-length terminator.
    ///
    /// Validation happens before any byte is written, so an invalid sentence
    /// produces no output at all.
    pub(crate) fn sentence(&self, words: &[String]) -> Result<Vec<u8>, WireError> {
        self.validate(words)?;
        let total: usize = words.iter().map(String::len).sum();
        let mut out = Vec::with_capacity(total + words.len() + 5);
        Self::append_validated(&mut out, words);
        Ok(out)
    }

    fn validate(&self, words: &[String]) -> Result<(), WireError> {
        if words.is_empty() {
            return Err(WireError::protocol("sentence has no words"));
        }
        if words.len() > self.limits.max_words_per_sentence {
            return Err(WireError::Limit {
                scope: "words per sentence",
                limit: self.limits.max_words_per_sentence as u64,
                observed: words.len() as u64,
            });
        }
        let mut total = 0u64;
        for word in words {
            if word.is_empty() {
                return Err(WireError::protocol("sentence contains an empty word"));
            }
            let bytes = word.len() as u64;
            if bytes > u64::from(u32::MAX) || bytes > self.limits.max_word_bytes as u64 {
                return Err(WireError::Limit {
                    scope: "word",
                    limit: self.limits.max_word_bytes as u64,
                    observed: bytes,
                });
            }
            total += bytes;
            if total > self.limits.max_sentence_bytes as u64 {
                return Err(WireError::Limit {
                    scope: "sentence",
                    limit: self.limits.max_sentence_bytes as u64,
                    observed: total,
                });
            }
        }
        debug_assert!(total <= MAX_WIRE_LENGTH);
        Ok(())
    }

    fn append_validated(dst: &mut Vec<u8>, words: &[String]) {
        for word in words {
            let length = u32::try_from(word.len()).expect("validate bounds every word to u32");
            encode_length(dst, length);
            dst.extend_from_slice(word.as_bytes());
        }
        dst.push(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn encodes_words_and_terminator() {
        let encoder = Encoder::new(Limits::default());
        let bytes = encoder.sentence(&words(&["/interface/print", "=.proplist=name"]));
        let mut expected = Vec::new();
        encode_length(&mut expected, 16);
        expected.extend_from_slice(b"/interface/print");
        encode_length(&mut expected, 15);
        expected.extend_from_slice(b"=.proplist=name");
        expected.push(0);
        assert_eq!(bytes.unwrap(), expected);
    }

    #[test]
    fn measures_utf8_bytes_not_characters() {
        let encoder = Encoder::new(Limits::default());
        let bytes = encoder.sentence(&words(&["café"])).unwrap();
        assert_eq!(bytes[0], 5);
        assert_eq!(&bytes[1..6], "café".as_bytes());
        assert_eq!(bytes[6], 0);
    }

    #[test]
    fn rejects_an_empty_sentence() {
        let encoder = Encoder::new(Limits::default());
        assert!(matches!(
            encoder.sentence(&[]),
            Err(WireError::Protocol { .. })
        ));
    }

    #[test]
    fn rejects_an_empty_word() {
        let encoder = Encoder::new(Limits::default());
        assert!(matches!(
            encoder.sentence(&words(&["/x", ""])),
            Err(WireError::Protocol { .. })
        ));
    }

    #[test]
    fn enforces_the_word_limit() {
        let encoder = Encoder::new(Limits {
            max_word_bytes: 4,
            max_sentence_bytes: 64,
            max_words_per_sentence: 8,
        });
        assert!(matches!(
            encoder.sentence(&words(&["abcde"])),
            Err(WireError::Limit { scope: "word", .. })
        ));
    }

    #[test]
    fn enforces_the_sentence_limit() {
        let encoder = Encoder::new(Limits {
            max_word_bytes: 8,
            max_sentence_bytes: 5,
            max_words_per_sentence: 8,
        });
        assert!(matches!(
            encoder.sentence(&words(&["abc", "def"])),
            Err(WireError::Limit {
                scope: "sentence",
                ..
            })
        ));
    }

    #[test]
    fn enforces_the_word_count_limit() {
        let encoder = Encoder::new(Limits {
            max_word_bytes: 8,
            max_sentence_bytes: 64,
            max_words_per_sentence: 2,
        });
        assert!(matches!(
            encoder.sentence(&words(&["a", "b", "c"])),
            Err(WireError::Limit {
                scope: "words per sentence",
                ..
            })
        ));
    }
}
