//! Incremental sentence decoder.
//!
//! The decoder accepts every possible TCP split: a prefix, a payload, and a
//! sentence terminator may each arrive in pieces. It owns no socket and never
//! reads on its own.

use super::WireError;
use super::length::decode_length;
use super::limits::Limits;

/// Turns an incremental byte stream into sentences.
///
/// A decoding failure is sticky: the byte position is no longer known, so
/// every later call reports the same error. Payloads are decoded as UTF-8;
/// invalid sequences become U+FFFD, matching the TypeScript client's default.
pub(crate) struct Decoder {
    limits: Limits,
    buffer: Vec<u8>,
    word: Vec<u8>,
    word_length: usize,
    words: Vec<String>,
    sentence_bytes: usize,
    failure: Option<WireError>,
}

impl Decoder {
    pub(crate) fn new(limits: Limits) -> Self {
        Decoder {
            limits: limits.with_defaults(),
            buffer: Vec::new(),
            word: Vec::new(),
            word_length: 0,
            words: Vec::new(),
            sentence_bytes: 0,
            failure: None,
        }
    }

    /// Add `chunk` and return every sentence it completed, in wire order.
    ///
    /// A sentence is its list of word payloads; the terminator is not returned
    /// and an empty sentence is dropped.
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Result<Vec<Vec<String>>, WireError> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        self.buffer.extend_from_slice(chunk);

        let mut sentences = Vec::new();
        let mut offset = 0;
        loop {
            if self.word_length == 0 {
                let prefix = match decode_length(&self.buffer[offset..]) {
                    Ok(Some(prefix)) => prefix,
                    Ok(None) => break,
                    Err(error) => return Err(self.fail(error)),
                };
                offset += prefix.size;
                if let Err(error) = self.begin_word(prefix.length) {
                    return Err(self.fail(error));
                }
                if self.word_length == 0 {
                    if !self.words.is_empty() {
                        sentences.push(std::mem::take(&mut self.words));
                        self.sentence_bytes = 0;
                    }
                    continue;
                }
            }

            let available = (self.buffer.len() - offset).min(self.word_length);
            if available == 0 {
                break;
            }
            self.word
                .extend_from_slice(&self.buffer[offset..offset + available]);
            offset += available;
            self.word_length -= available;
            if self.word_length == 0 {
                self.words
                    .push(String::from_utf8_lossy(&self.word).into_owned());
                self.word.clear();
            }
        }

        if offset > 0 {
            self.buffer.drain(..offset);
        }
        if self.buffer.is_empty() {
            self.buffer = Vec::new();
        }
        Ok(sentences)
    }

    fn begin_word(&mut self, length: u32) -> Result<(), WireError> {
        if length == 0 {
            self.word_length = 0;
            return Ok(());
        }
        let bytes = u64::from(length);
        if bytes > self.limits.max_word_bytes as u64 {
            return Err(WireError::Limit {
                scope: "word",
                limit: self.limits.max_word_bytes as u64,
                observed: bytes,
            });
        }
        let bytes = usize::try_from(bytes)
            .map_err(|_error| WireError::protocol("word length does not fit this platform"))?;
        self.sentence_bytes += bytes;
        if self.sentence_bytes as u64 > self.limits.max_sentence_bytes as u64 {
            return Err(WireError::Limit {
                scope: "sentence",
                limit: self.limits.max_sentence_bytes as u64,
                observed: self.sentence_bytes as u64,
            });
        }
        if self.words.len() + 1 > self.limits.max_words_per_sentence {
            return Err(WireError::Limit {
                scope: "words per sentence",
                limit: self.limits.max_words_per_sentence as u64,
                observed: (self.words.len() + 1) as u64,
            });
        }
        self.word_length = bytes;
        Ok(())
    }

    fn fail(&mut self, error: WireError) -> WireError {
        self.failure = Some(error.clone());
        self.words = Vec::new();
        self.word = Vec::new();
        self.buffer = Vec::new();
        self.word_length = 0;
        self.sentence_bytes = 0;
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::length::encode_length;

    fn frame(words: &[&[u8]]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for word in words {
            let length = u32::try_from(word.len()).expect("test words fit a length prefix");
            encode_length(&mut bytes, length);
            bytes.extend_from_slice(word);
        }
        bytes.push(0);
        bytes
    }

    fn strings(words: &[&str]) -> Vec<String> {
        words.iter().map(|word| (*word).to_owned()).collect()
    }

    #[test]
    fn decodes_a_sentence_in_one_chunk() {
        let mut decoder = Decoder::new(Limits::default());
        let bytes = frame(&[b"/interface/print", b"=name=ether1"]);
        let sentences = decoder.feed(&bytes).unwrap();
        assert_eq!(
            sentences,
            vec![strings(&["/interface/print", "=name=ether1"])]
        );
    }

    #[test]
    fn accepts_every_split_boundary() {
        let bytes = frame(&["café".as_bytes(), b"=comment=\xc3\xa9"]);
        for cut in 0..=bytes.len() {
            let mut decoder = Decoder::new(Limits::default());
            let mut sentences = decoder.feed(&bytes[..cut]).unwrap();
            sentences.extend(decoder.feed(&bytes[cut..]).unwrap());
            assert_eq!(
                sentences,
                vec![strings(&["café", "=comment=é"])],
                "cut {cut}"
            );
        }
    }

    #[test]
    fn accepts_one_byte_at_a_time() {
        let bytes = frame(&[b"!done", b"=ret=*1"]);
        let mut decoder = Decoder::new(Limits::default());
        let mut sentences = Vec::new();
        for byte in &bytes {
            sentences.extend(decoder.feed(std::slice::from_ref(byte)).unwrap());
        }
        assert_eq!(sentences, vec![strings(&["!done", "=ret=*1"])]);
    }

    #[test]
    fn decodes_multiple_sentences_per_chunk() {
        let mut bytes = frame(&[b"!re"]);
        bytes.extend(frame(&[b"!done"]));
        let mut decoder = Decoder::new(Limits::default());
        let sentences = decoder.feed(&bytes).unwrap();
        assert_eq!(sentences, vec![strings(&["!re"]), strings(&["!done"])]);
    }

    #[test]
    fn ignores_empty_sentences() {
        let mut decoder = Decoder::new(Limits::default());
        assert_eq!(
            decoder.feed(&[0]).unwrap(),
            [] as [std::vec::Vec<std::string::String>; 0]
        );
        let bytes = frame(&[b"!re"]);
        let sentences = decoder.feed(&bytes).unwrap();
        assert_eq!(sentences, vec![strings(&["!re"])]);
    }

    #[test]
    fn preserves_null_bytes() {
        let payload = [b'a', 0, b'b'];
        let mut decoder = Decoder::new(Limits::default());
        let sentences = decoder.feed(&frame(&[&payload])).unwrap();
        assert_eq!(sentences[0][0].as_bytes(), b"a\0b");
    }

    #[test]
    fn rejects_an_oversized_prefix_before_the_payload_arrives() {
        let mut decoder = Decoder::new(Limits {
            max_word_bytes: 4,
            max_sentence_bytes: 64,
            max_words_per_sentence: 8,
        });
        let mut bytes = Vec::new();
        encode_length(&mut bytes, 5);
        let error = decoder.feed(&bytes).unwrap_err();
        assert!(matches!(
            error,
            WireError::Limit {
                scope: "word",
                limit: 4,
                observed: 5
            }
        ));
    }

    #[test]
    fn enforces_sentence_bytes_and_word_count() {
        let mut decoder = Decoder::new(Limits {
            max_word_bytes: 8,
            max_sentence_bytes: 4,
            max_words_per_sentence: 8,
        });
        let error = decoder.feed(&frame(&[b"abc", b"de"])).unwrap_err();
        assert!(matches!(
            error,
            WireError::Limit {
                scope: "sentence",
                ..
            }
        ));

        let mut decoder = Decoder::new(Limits {
            max_word_bytes: 8,
            max_sentence_bytes: 64,
            max_words_per_sentence: 2,
        });
        let error = decoder.feed(&frame(&[b"a", b"b", b"c"])).unwrap_err();
        assert!(matches!(
            error,
            WireError::Limit {
                scope: "words per sentence",
                ..
            }
        ));
    }

    #[test]
    fn a_failure_is_sticky() {
        let mut decoder = Decoder::new(Limits::default());
        assert!(decoder.feed(&[0xf1]).is_err());
        let again = decoder.feed(&frame(&[b"!done"])).unwrap_err();
        assert!(matches!(again, WireError::Protocol { .. }));
    }

    #[test]
    fn a_reserved_control_byte_is_a_protocol_error() {
        let mut decoder = Decoder::new(Limits::default());
        let error = decoder.feed(&[0xff]).unwrap_err();
        assert!(matches!(error, WireError::Protocol { .. }));
    }

    #[test]
    fn keeps_a_partial_payload_across_feeds() {
        let payload = b"=comment=split";
        let bytes = frame(&[payload]);
        let mut decoder = Decoder::new(Limits::default());
        assert_eq!(
            decoder.feed(&bytes[..4]).unwrap(),
            [] as [std::vec::Vec<std::string::String>; 0]
        );
        let sentences = decoder.feed(&bytes[4..]).unwrap();
        assert_eq!(sentences, vec![strings(&["=comment=split"])]);
    }
}
