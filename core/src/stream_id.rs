//! Messages like events and commands are written to and read from streams. To
//! write and read from streams, the subject stream is identified by its name.
//!
//! A stream name not only identifies the stream, but also its purpose. A stream
//! name is a string that optionally includes an ID that is prefixed by a dash
//! (-) character, and may also include category *types* that indicate even
//! further specialized uses of the stream. The part of the stream preceding the
//! dash is the *category*, and the part following the dash is the ID.
//!
//! # Entity Stream Name
//!
//! An *entity* stream name contains all of the events for one specific entity.
//! For example, an `Account` entity with an ID of `123` would have the name,
//! `account-123`.
//!
//! # Category Stream Name
//!
//! A *category* stream name does not have an ID. For example, the stream name
//! for the category of all accounts is `account`.
//!
//! # Example Stream Names
//!
//! `account`
//!
//! Account category stream name. The name of the stream that has events for all
//! accounts.
//!
//! `account-123`
//!
//! Account entity stream name. The name of the stream that has events only for
//! the particular account with the ID 123.
//!
//! `account:command`
//!
//! Account command category stream name, or account command stream name for
//! short. This is a category stream name with a command type. This stream has
//! all commands for all accounts.
//!
//! `account:command-123`
//!
//! Account entity command stream name. This stream has all of the commands
//! specifically for the account with the ID 123.
//!
//! `account:command+position`
//!
//! Account command position category stream name. A consumer that is reading
//! commands from the account:command stream will periodically write the
//! position number of the last command processed to the position stream so that
//! all commands from all time do not have to be re-processed when the consumer
//! is restarted.
//!
//! `account:snapshot-123`
//!
//! Account entity snapshot stream name. Entity snapshots are periodically
//! recorded to disk as a performance optimization that eliminates the need to
//! project an event stream from its first-ever recorded event when entity is
//! not already in the in-memory cache.

use std::{
    borrow, fmt, ops,
    str::{self, Split},
};

use serde::de::{self, Visitor};
use serde::{Deserialize, Serialize};

/// A stream name containing a category, and optionally an ID.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    inner: String,
}

impl StreamId {
    /// Category type separator.
    ///
    /// When a stream category contains a category type, it is separated by a
    /// colon (`:`) character.
    ///
    /// # Example
    ///
    /// `category:command`
    pub const CATEGORY_TYPE_SEPARATOR: char = ':';
    /// Compound type separator.
    ///
    /// When one or more category types are present, they are separated by a
    /// plus (`+`) character.
    ///
    /// # Example
    ///
    /// `category:command+snapshot`
    pub const COMPOUNT_TYPE_SEPARATOR: char = '+';
    /// ID separator.
    ///
    /// When a stream name contains an ID, it is separated by a
    /// colon (`-`) character.
    ///
    /// Only the first `-` is the separator, and all other `-` characters in an
    /// ID are valid.
    ///
    /// # Example
    ///
    /// `category-id`
    pub const ID_SEPARATOR: char = '-';
    /// Compound ID separator.
    ///
    /// When multiple IDs are present, they are separated by a plus (`+`)
    /// character.
    ///
    /// # Example
    ///
    /// `account1+account2`
    pub const COMPOUND_ID_SEPARATOR: char = '+';

    /// Creates a new StreamID.
    pub fn new(s: impl Into<String>) -> Self {
        StreamId { inner: s.into() }
    }

    /// Creates a new StreamID from a category and id.
    pub fn new_from_parts(category: &str, id: impl fmt::Display) -> Self {
        StreamId::new(format!("{category}{}{id}", Self::ID_SEPARATOR))
    }

    /// Returns the inner string.
    pub fn into_inner(self) -> String {
        self.inner
    }

    /// Returns an iter over the ids.
    pub fn ids(&self) -> Split<'_, char> {
        self.inner
            .split_once(Self::ID_SEPARATOR)
            .map(|(_, ids)| ids)
            .unwrap_or("")
            .split(Self::COMPOUND_ID_SEPARATOR)
    }

    /// Returns the cardinal ID.
    ///
    /// This is the first ID. If there is only one ID present, that is the
    /// cardinal ID.
    pub fn cardinal_id(&self) -> &str {
        self.ids().next().unwrap()
    }

    /// Returns whether a `stream_id` is a category.
    pub fn is_category(&self) -> bool {
        self.inner.contains(Self::ID_SEPARATOR)
    }

    /// Returns the category part of a `stream_id`.
    pub fn category(&self) -> &str {
        self.inner
            .split_once(Self::ID_SEPARATOR)
            .map(|(category, _)| category)
            .unwrap_or(&self.inner)
    }

    /// Returns the category part of a `stream_id`.
    pub fn types(&self) -> Split<'_, char> {
        self.category()
            .split_once(Self::CATEGORY_TYPE_SEPARATOR)
            .map(|(_, types)| types)
            .unwrap_or("")
            .split(Self::COMPOUNT_TYPE_SEPARATOR)
    }

    /// Returns the category part of a `stream_id`.
    pub fn entity_name(&self) -> &str {
        self.inner
            .split_once(Self::ID_SEPARATOR)
            .map(|(category, _)| category)
            .unwrap_or(&self.inner)
    }

    // /// Normalizes a category into camelCase.
    // pub fn normalize_category(&mut self) {
    //     let entity_name = self.entity_name();
    //     self.inner = format!("{entity_name}{}", self.inner.split_at(entity_name.len()).1);
    // }

    /// Replaces the id, or inserts one if it doesn't exist in the stream id.
    pub fn set_id(&mut self, id: &str) {
        self.inner.truncate(self.category().len() + 1);
        if !self
            .inner
            .chars()
            .last()
            .map(|c| c == Self::ID_SEPARATOR)
            .unwrap_or(false)
        {
            self.inner.push(Self::ID_SEPARATOR);
        }
        self.inner.push_str(id);
    }

    /// Pushes a type to the category if it doesn't already exist.
    pub fn push_type(&mut self, ty: &str) {
        if self.types().any(|existing_ty| existing_ty == ty) {
            return;
        }
        let category = self.category();
        let category_len = category.len();
        let separator = if self.inner.contains(Self::CATEGORY_TYPE_SEPARATOR) {
            Self::COMPOUNT_TYPE_SEPARATOR
        } else {
            Self::CATEGORY_TYPE_SEPARATOR
        };
        self.inner.insert(category_len, separator);
        self.inner.insert_str(category_len + 1, ty);
    }
}

impl From<StreamId> for String {
    fn from(stream_id: StreamId) -> Self {
        stream_id.into_inner()
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl PartialEq<String> for StreamId {
    fn eq(&self, other: &String) -> bool {
        self.inner.eq(other)
    }
}

impl PartialEq<str> for StreamId {
    fn eq(&self, other: &str) -> bool {
        self.inner.eq(other)
    }
}

impl PartialEq<&str> for StreamId {
    fn eq(&self, other: &&str) -> bool {
        self.inner.eq(other)
    }
}

impl PartialEq<StreamId> for String {
    fn eq(&self, other: &StreamId) -> bool {
        self.eq(&other.inner)
    }
}

impl PartialEq<StreamId> for str {
    fn eq(&self, other: &StreamId) -> bool {
        self.eq(&other.inner)
    }
}

impl PartialEq<StreamId> for &str {
    fn eq(&self, other: &StreamId) -> bool {
        self.eq(&other.inner)
    }
}

impl AsRef<str> for StreamId {
    fn as_ref(&self) -> &str {
        &self.inner
    }
}

impl AsRef<String> for StreamId {
    fn as_ref(&self) -> &String {
        &self.inner
    }
}

impl ops::Deref for StreamId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl borrow::Borrow<str> for StreamId {
    fn borrow(&self) -> &str {
        &self.inner
    }
}

impl Serialize for StreamId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_ref())
    }
}

impl<'de> Deserialize<'de> for StreamId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StreamNameVisitor;

        impl<'de> Visitor<'de> for StreamNameVisitor {
            type Value = StreamId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("StreamName")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StreamId {
                    inner: v.to_string(),
                })
            }
        }

        deserializer.deserialize_str(StreamNameVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let stream_name = StreamId::new("account:position-123+456");
        assert_eq!(stream_name.inner, "account:position-123+456");
    }

    #[test]
    fn test_ids() {
        let stream_name = StreamId::new("category:command-123+456");
        let ids: Vec<&str> = stream_name.ids().collect();
        assert_eq!(ids, vec!["123", "456"]);
    }

    #[test]
    fn test_cardinal_id() {
        let stream_name = StreamId::new("category:command-123+456");
        assert_eq!(stream_name.cardinal_id(), "123");
    }

    #[test]
    fn test_is_category() {
        let stream_name_with_id = StreamId::new("category:command-123+456");
        let stream_name_no_id = StreamId::new("category:command");
        assert!(stream_name_with_id.is_category());
        assert!(!stream_name_no_id.is_category());
    }

    #[test]
    fn test_category() {
        let stream_name = StreamId::new("category:command-123+456");
        assert_eq!(stream_name.category(), "category:command");
    }

    #[test]
    fn test_entity_name() {
        let stream_name = StreamId::new("category:command-123+456");
        assert_eq!(stream_name.entity_name(), "category:command");
    }

    // #[test]
    // fn test_normalize_category() {
    //     let mut stream_name = StreamID::new("Category:Command-123+456");
    //     stream_name.normalize_category();
    //     assert_eq!(stream_name.inner, "Category-123+456");
    // }

    #[test]
    fn test_set_id_from_category() {
        let mut stream_name = StreamId::new("category");
        stream_name.set_id("123");
        assert_eq!(stream_name.inner, "category-123");
    }

    #[test]
    fn test_replace_id_with_set_id() {
        let mut stream_name = StreamId::new("category-123");
        stream_name.set_id("456");
        assert_eq!(stream_name.inner, "category-456");
    }

    #[test]
    fn test_push_type_to_simple_category() {
        let mut stream_name = StreamId::new("account");
        stream_name.push_type("position");
        assert_eq!(stream_name.inner, "account:position");
    }

    #[test]
    fn test_push_type_to_category_with_type() {
        let mut stream_name = StreamId::new("account:position");
        stream_name.push_type("snapshot");
        assert_eq!(stream_name.inner, "account:position+snapshot");
    }

    #[test]
    fn test_push_type_to_category_with_multiple_types() {
        let mut stream_name = StreamId::new("account:position+management");
        stream_name.push_type("snapshot");
        assert_eq!(stream_name.inner, "account:position+management+snapshot");
    }

    #[test]
    fn test_push_type_to_category_with_id() {
        let mut stream_name = StreamId::new("account-123");
        stream_name.push_type("position");
        assert_eq!(stream_name.inner, "account:position-123");
    }

    #[test]
    fn test_push_type_to_category_with_type_and_id() {
        let mut stream_name = StreamId::new("account:position-123");
        stream_name.push_type("snapshot");
        assert_eq!(stream_name.inner, "account:position+snapshot-123");
    }

    #[test]
    fn test_push_type_to_category_already_exists() {
        let mut stream_name = StreamId::new("account:position-123");
        stream_name.push_type("position");
        assert_eq!(stream_name.inner, "account:position-123");
    }
}
