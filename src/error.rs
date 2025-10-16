use std::{io, str::FromStr, sync::Arc, time::Duration};

use once_cell::sync::Lazy;
use regex::Regex;
use sierradb_client::{CurrentVersion, ExpectedVersion, SierraError};
use thiserror::Error;
use uuid::Uuid;

use crate::StreamId;

#[derive(Debug, Error)]
pub enum ExecuteError<E> {
    #[error("{0:?}")]
    Handle(E),
    #[error("command service not running")]
    CommandServiceNotRunning,
    #[error("command service stopped")]
    CommandServiceStopped,
    #[error(transparent)]
    Database(#[from] SierraError),
    #[error("entity '{category}-{id}' actor not running")]
    EntityActorNotRunning {
        category: &'static str,
        id: Arc<str>,
    },
    #[error("entity '{category}-{id}' actor stopped")]
    EntityActorStopped {
        category: &'static str,
        id: Arc<str>,
    },
    #[error("event store actor not running")]
    EventStoreActorNotRunning,
    #[error("event store actor stopped")]
    EventStoreActorStopped,
    // #[error("idempotency violation")]
    // IdempotencyViolation,
    #[error(transparent)]
    SerializeEvent(ciborium::ser::Error<io::Error>),
    #[error("failed to serialize metadata: {0}")]
    SerializeMetadata(ciborium::ser::Error<io::Error>),
    #[error("expected '{stream_id}' version {expected} but got {current}")]
    IncorrectExpectedVersion {
        stream_id: StreamId,
        current: CurrentVersion,
        expected: ExpectedVersion,
    },
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error("too many write conflicts for stream '{stream_id}'")]
    TooManyConflicts { stream_id: StreamId },
    #[error("transaction aborted and was not completed")]
    TransactionAborted,
    #[error("entity '{entity}' has existing partition key {existing} which does not match the one set {new}")]
    PartitionKeyMismatch {
        entity: &'static str,
        existing: Uuid,
        new: Uuid,
    },
    #[error("rate limit exceeded")]
    RateLimitExceeded {
        max_requests: u32,
        window_duration: Duration,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedStream {
    pub partition_key: Uuid,
    pub stream_id: StreamId,
    pub current: CurrentVersion,
    pub expected: ExpectedVersion,
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Invalid format: expected pattern not found")]
    InvalidFormat,
    #[error("Invalid UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),
    #[error("Invalid version number: {0}")]
    InvalidVersionNumber(#[from] std::num::ParseIntError),
    #[error("Regex error: {0}")]
    RegexError(#[from] regex::Error),
}

pub fn parse_stream_version_string(input: &str) -> Result<ParsedStream, ParseError> {
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"current stream version is (\S+) but expected (\S+) for partition key ([0-9a-fA-F-]+) and stream id (.+)").unwrap()
    });

    let captures = RE.captures(input).ok_or(ParseError::InvalidFormat)?;

    // Extract captured groups
    let current_version_str = captures.get(1).ok_or(ParseError::InvalidFormat)?.as_str();
    let expected_version_str = captures.get(2).ok_or(ParseError::InvalidFormat)?.as_str();
    let partition_key_str = captures.get(3).ok_or(ParseError::InvalidFormat)?.as_str();
    let stream_id = StreamId::new(
        captures
            .get(4)
            .ok_or(ParseError::InvalidFormat)?
            .as_str()
            .to_string(),
    );

    // Parse components
    let current = CurrentVersion::from_str(current_version_str)?;
    let expected = ExpectedVersion::from_str(expected_version_str)?;
    let partition_key = Uuid::parse_str(partition_key_str)?;

    Ok(ParsedStream {
        current,
        expected,
        partition_key,
        stream_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_example_string() {
        let input = "current stream version is 2 but expected empty for partition key 6a7a8ed5-d09e-57e7-abf7-a038c01d4b53 and stream id fiz";

        let result = parse_stream_version_string(input).unwrap();

        assert_eq!(result.current, CurrentVersion::Current(2));
        assert_eq!(result.expected, ExpectedVersion::Empty);
        assert_eq!(
            result.partition_key,
            Uuid::parse_str("6a7a8ed5-d09e-57e7-abf7-a038c01d4b53").unwrap()
        );
        assert_eq!(result.stream_id, "fiz");
    }

    #[test]
    fn test_parse_different_versions() {
        let test_cases = vec![
            ("current stream version is empty but expected any for partition key 6a7a8ed5-d09e-57e7-abf7-a038c01d4b53 and stream id test", 
             CurrentVersion::Empty, ExpectedVersion::Any),
            ("current stream version is 42 but expected exists for partition key 6a7a8ed5-d09e-57e7-abf7-a038c01d4b53 and stream id test", 
             CurrentVersion::Current(42), ExpectedVersion::Exists),
            ("current stream version is 0 but expected 5 for partition key 6a7a8ed5-d09e-57e7-abf7-a038c01d4b53 and stream id test", 
             CurrentVersion::Current(0), ExpectedVersion::Exact(5)),
        ];

        for (input, expected_current, expected_expected) in test_cases {
            let result = parse_stream_version_string(input).unwrap();
            assert_eq!(result.current, expected_current);
            assert_eq!(result.expected, expected_expected);
        }
    }

    #[test]
    fn test_parse_complex_stream_id() {
        let input = "current stream version is 1 but expected 2 for partition key 6a7a8ed5-d09e-57e7-abf7-a038c01d4b53 and stream id my-complex_stream.id/with/slashes";

        let result = parse_stream_version_string(input).unwrap();

        assert_eq!(result.stream_id, "my-complex_stream.id/with/slashes");
    }

    #[test]
    fn test_invalid_format() {
        let input = "this is not the right format";
        let result = parse_stream_version_string(input);
        assert!(matches!(result, Err(ParseError::InvalidFormat)));
    }

    #[test]
    fn test_invalid_uuid() {
        let input = "current stream version is 1 but expected 2 for partition key not-a-uuid and stream id test";
        let result = parse_stream_version_string(input);
        assert!(matches!(result, Err(ParseError::InvalidUuid(_))));
    }
}
