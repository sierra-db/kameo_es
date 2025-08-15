use std::{fmt, io};

use async_stream::try_stream;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use redis::{aio::MultiplexedConnection, RedisError};
use serde::Serialize;
use sierradb_client::{
    stream_partition_key, AsyncTypedCommands, EMAppendEvent, ErrorCode, Event, ExpectedVersion,
    MultiAppendInfo, SierraError,
};
use uuid::Uuid;

use crate::{Error, EventType, GenericValue, StreamId};

#[derive(Clone, Debug)]
pub struct EventStore {
    conn: MultiplexedConnection,
}

impl EventStore {
    pub fn new(client: MultiplexedConnection) -> Self {
        EventStore { conn: client }
    }

    pub async fn get_stream_events<'a>(
        &'a mut self,
        stream_id: &'a str,
        mut stream_version: u64,
    ) -> impl Stream<Item = Result<Vec<Event>, RedisError>> + 'a {
        try_stream! {
            loop {
                let batch = self
                    .conn
                    .escan(stream_id, stream_version, None, Some(100))
                    .await?;

                let last_stream_version = batch.events.last().map(|event| event.stream_version);
                yield batch.events;

                match last_stream_version {
                    Some(last_version) => {
                        stream_version = last_version + 1;
                        if !batch.has_more {
                            break;
                        }
                    }
                    None => {
                        debug_assert_eq!(batch.has_more, false);
                        break;
                    }
                }

            }
        }
    }

    pub async fn append_to_stream<E, M>(
        &mut self,
        partition_key: Uuid,
        events: Vec<E>,
        expected_version: ExpectedVersion,
        metadata: M,
        timestamp: DateTime<Utc>,
    ) -> Result<MultiAppendInfo, AppendEventsError<Vec<E>, M>>
    where
        E: EventType + Serialize + Send + Sync + 'static,
        M: Serialize + Send + Sync + 'static,
    {
        let mut metadata_buf = Vec::new();
        ciborium::into_writer(&metadata, &mut metadata_buf)?;

        let new_events = events
            .iter()
            .enumerate()
            .map(|(i, event)| {
                let mut payload = Vec::new();
                ciborium::into_writer(&event, &mut payload)?;

                let expected_version = match expected_version {
                    ExpectedVersion::Any => ExpectedVersion::Any,
                    ExpectedVersion::Exists => ExpectedVersion::Exists,
                    ExpectedVersion::Empty => match i {
                        0 => ExpectedVersion::Empty,
                        n => ExpectedVersion::Exact(n as u64 - 1),
                    },
                    ExpectedVersion::Exact(version) => ExpectedVersion::Exact(version + i as u64),
                };

                Ok(EMAppendEvent::new(stream_id.as_ref(), event.event_type())
                    .payload(payload)
                    .metadata(&metadata_buf)
                    .expected_version(expected_version)
                    .timestamp(timestamp.into()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        self.conn
            .emappend(stream_partition_key(&stream_id), &new_events)
            .await
            .map_err(|err| match SierraError::from_redis_error(&err) {
                SierraError::Protocol {
                    code: ErrorCode::WrongVer,
                    message,
                } => {
                    let (current, expected) = parse_sequence_error(&message).unwrap();
                    AppendEventsError::IncorrectExpectedVersion {
                        stream_id,
                        current,
                        expected,
                        events,
                        metadata,
                    }
                }
                err => AppendEventsError::Database(err),
            })
    }

    pub async fn append_to_streams(
        &mut self,
        stream_events: Vec<AppendEvents<(&'static str, GenericValue), GenericValue>>,
    ) -> Result<
        Vec<(u64, DateTime<Utc>)>,
        AppendEventsError<
            Vec<AppendEvents<(&'static str, GenericValue), GenericValue>>,
            GenericValue,
        >,
    > {
        let mut streams = Vec::with_capacity(stream_events.len());
        for batch in &stream_events {
            let mut metadata = Vec::new();
            ciborium::into_writer(&batch.metadata, &mut metadata)?;
            let events = batch
                .events
                .iter()
                .map(|(event_name, event)| {
                    let mut event_data = Vec::new();
                    ciborium::into_writer(&event, &mut event_data)?;
                    Ok(NewEvent {
                        event_name: event_name.to_string(),
                        event_data,
                        metadata: metadata.clone(),
                    })
                })
                .collect::<Result<_, ciborium::ser::Error<io::Error>>>()?;

            streams.push(StreamEvents {
                stream_id: batch.stream_id.clone().into_inner(),
                expected_version: Some(batch.expected_version.into()),
                events,
                timestamp: Some(Timestamp {
                    seconds: batch.timestamp.timestamp(),
                    nanos: batch
                        .timestamp
                        .timestamp_subsec_nanos()
                        .try_into()
                        .map_err(|_| AppendEventsError::InvalidTimestamp)?,
                }),
            });
        }

        let res = self
            .conn
            .append_to_multiple_streams(AppendToMultipleStreamsRequest { streams })
            .await;
        let res = match res {
            Ok(res) => res.into_inner(),
            Err(status) => match status.code() {
                Code::FailedPrecondition => {
                    let stream_id = StreamId::new(
                        status
                            .metadata()
                            .get("stream_id")
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_string(),
                    );

                    let current = status.metadata().get("current").unwrap();
                    let current = match current.to_str().unwrap() {
                        "-1" => CurrentVersion::NoStream,
                        s => CurrentVersion::Current(s.parse::<u64>().unwrap()),
                    };

                    let expected = status.metadata().get("expected").unwrap();
                    let expected = match expected.to_str().unwrap() {
                        "any" => ExpectedVersion::Any,
                        "stream_exists" => ExpectedVersion::StreamExists,
                        "no_stream" => ExpectedVersion::NoStream,
                        s => ExpectedVersion::Exact(s.parse::<u64>().unwrap()),
                    };

                    let (events, metadata) = stream_events.into_iter().fold(
                        (Vec::new(), None),
                        |(mut events, mut metadata), append| {
                            if append.stream_id == stream_id {
                                metadata = Some(append.metadata);
                            } else {
                                events.push(append);
                            }
                            (events, metadata)
                        },
                    );

                    return Err(AppendEventsError::IncorrectExpectedVersion {
                        stream_id,
                        current,
                        expected,
                        events,
                        metadata: metadata.unwrap(),
                    });
                }
                _ => return Err(AppendEventsError::Database(status)),
            },
        };

        let results = res
            .streams
            .into_iter()
            .map(|res| {
                let timestamp = res.timestamp.unwrap();
                (
                    res.first_id,
                    DateTime::from_timestamp(
                        timestamp.seconds,
                        timestamp.nanos.try_into().unwrap(),
                    )
                    .unwrap()
                    .to_utc(),
                )
            })
            .collect();

        Ok(results)
    }

    pub async fn get_last_event_id(&mut self) -> Result<Option<u64>, Status> {
        Ok(self
            .conn
            .get_last_event_id(GetLastEventIdRequest {})
            .await?
            .into_inner()
            .last_event_id)
    }
}

#[derive(Debug)]
pub struct AppendEvents<E, M> {
    pub stream_id: StreamId,
    pub events: Vec<E>,
    pub expected_version: ExpectedVersion,
    pub metadata: M,
    pub timestamp: DateTime<Utc>,
}

#[derive(Error)]
pub enum AppendEventsError<E, M> {
    #[error(transparent)]
    Database(SierraError),
    #[error("expected '{stream_id}' version {expected} but got {current}")]
    IncorrectExpectedVersion {
        stream_id: StreamId,
        current: CurrentVersion,
        expected: ExpectedVersion,
        events: E,
        metadata: M,
    },
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error(transparent)]
    SerializeEvent(#[from] ciborium::ser::Error<io::Error>),
}

impl<E, M> fmt::Debug for AppendEventsError<E, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(db) => f.debug_tuple("Database").field(db).finish(),
            Self::IncorrectExpectedVersion {
                stream_id,
                current,
                expected,
                ..
            } => f
                .debug_struct("IncorrectExpectedVersion")
                .field("stream_id", stream_id)
                .field("current", current)
                .field("expected", expected)
                .finish(),
            Self::InvalidTimestamp => f.debug_struct("InvalidTimestamp").finish(),
            Self::SerializeEvent(ev) => f.debug_tuple("SerializeEvent").field(ev).finish(),
        }
    }
}

fn parse_sequence_error(message: &str) -> Option<(Option<u64>, ExpectedVersion)> {
    // Find "current partition sequence is "
    let current_start = message.find("current partition sequence is ")?;
    let current_start = current_start + "current partition sequence is ".len();

    // Find " but expected "
    let but_expected_pos = message.find(" but expected ")?;
    let current_str = &message[current_start..but_expected_pos];

    // Parse current value
    let current = match current_str {
        "<empty>" => None,
        s => Some(s.parse::<u64>().ok()?),
    };

    // Find expected value after " but expected "
    let expected_start = but_expected_pos + " but expected ".len();
    let expected_str = &message[expected_start..];
    let expected = match expected_str {
        "any" => ExpectedVersion::Any,
        "exists" => ExpectedVersion::Exists,
        "empty" => ExpectedVersion::Empty,
        s => ExpectedVersion::Exact(s.parse::<u64>().ok()?),
    };

    Some((current, expected))
}
