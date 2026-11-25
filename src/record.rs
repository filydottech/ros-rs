//! Public record and command-result types.
//!
//! Every value the router sends is a string on the wire. A number, a boolean,
//! or a duration arrives in its textual form and is never coerced here.

use std::collections::BTreeMap;

/// The attributes of one reply sentence.
///
/// A repeated attribute name keeps its last value, matching `RouterOS`. The map
/// order is attribute-name order; the wire order of a record's attributes is
/// not part of the protocol.
pub type Record = BTreeMap<String, String>;

/// One completed command.
///
/// `records` preserves the arrival order of every `!re` reply. `done` holds the
/// attributes of the terminating `!done` reply.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommandResult {
    /// Every `!re` record, in arrival order.
    pub records: Vec<Record>,
    /// The attributes of the terminating `!done` reply.
    pub done: Record,
}

impl CommandResult {
    /// The id `RouterOS` assigned to an object the command created.
    ///
    /// A successful `/add` answers `!done =ret=*1`, and that id is the only
    /// handle on the new object. This accessor reads it without inferring
    /// anything from the path: a command that returns no `ret` yields `None`,
    /// which lets a caller that created something fail instead of assuming the
    /// mutation took effect.
    #[must_use]
    pub fn created_id(&self) -> Option<&str> {
        self.done.get("ret").map(String::as_str)
    }
}
