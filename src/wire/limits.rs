//! Codec limits shared by the encoder and decoder.

pub(crate) const DEFAULT_MAX_WORD_BYTES: usize = 32 << 20;
pub(crate) const DEFAULT_MAX_SENTENCE_BYTES: usize = 64 << 20;
pub(crate) const DEFAULT_MAX_WORDS_PER_SENTENCE: usize = 4096;

/// Bounds one word, one sentence, and the number of words a sentence may hold.
///
/// A zero field takes its default, so the transport can pass a public
/// configuration directly. Values are validated by [`crate::Config`] before a
/// connection is opened; the codec additionally guards against a payload that
/// a length prefix cannot carry.
///
/// The shared `max_` prefix is deliberate: every field is an upper bound.
#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Limits {
    pub(crate) max_word_bytes: usize,
    pub(crate) max_sentence_bytes: usize,
    pub(crate) max_words_per_sentence: usize,
}

impl Limits {
    pub(crate) fn new(
        max_word_bytes: usize,
        max_sentence_bytes: usize,
        max_words_per_sentence: usize,
    ) -> Self {
        Limits {
            max_word_bytes,
            max_sentence_bytes,
            max_words_per_sentence,
        }
    }

    pub(crate) fn with_defaults(self) -> Self {
        Limits {
            max_word_bytes: if self.max_word_bytes == 0 {
                DEFAULT_MAX_WORD_BYTES
            } else {
                self.max_word_bytes
            },
            max_sentence_bytes: if self.max_sentence_bytes == 0 {
                DEFAULT_MAX_SENTENCE_BYTES
            } else {
                self.max_sentence_bytes
            },
            max_words_per_sentence: if self.max_words_per_sentence == 0 {
                DEFAULT_MAX_WORDS_PER_SENTENCE
            } else {
                self.max_words_per_sentence
            },
        }
    }
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_word_bytes: DEFAULT_MAX_WORD_BYTES,
            max_sentence_bytes: DEFAULT_MAX_SENTENCE_BYTES,
            max_words_per_sentence: DEFAULT_MAX_WORDS_PER_SENTENCE,
        }
    }
}
