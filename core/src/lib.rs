mod stream_id;
pub mod test_utils;

pub use kameo_es_macros::{CommandName, EventType};
pub use stream_id::StreamId;

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt, ops,
    str::FromStr,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use ciborium::Value;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use uuid::Uuid;

pub trait Entity: Default + Send + 'static {
    type ID: FromStr + fmt::Display + Send + Sync;
    type Event: EventType + Clone + Serialize + DeserializeOwned + Send + Sync;
    type Metadata: Serialize + DeserializeOwned + Clone + Default + Unpin + Send + Sync + 'static;

    // The *snake_case* name of the entity category. This *must not* contain hyphens.
    fn category() -> &'static str;
}

pub trait Command<C: CommandName>: Entity {
    type Error: fmt::Debug + Send + Sync + 'static;

    fn handle(&self, cmd: C, ctx: Context<'_, Self>) -> Result<Vec<Self::Event>, Self::Error>;

    /// Returns true if this command should be skipped.
    /// This includes:
    /// - Already processed (exact duplicate)
    /// - Out of order (older version than what we've seen)
    /// - State-based idempotency
    fn is_idempotent(&self, _cmd: &C, ctx: Context<'_, Self>) -> bool {
        ctx.should_skip() || self.is_state_idempotent(_cmd, ctx)
    }

    /// Override for state-based idempotency checks.
    fn is_state_idempotent(&self, _cmd: &C, _ctx: Context<'_, Self>) -> bool {
        false
    }

    /// Rate limiting behaviour for this command. Returns None by default.
    fn rate_limit(&self) -> Option<RateLimit> {
        None
    }
}

pub trait CommandName {
    // The *snake_case* name of the command.
    //
    // This is typically used for idempotency checks via causation metadata.
    fn command_name() -> &'static str;
}

pub trait Apply
where
    Self: Entity,
{
    fn apply(&mut self, event: Self::Event, metadata: Metadata<Self::Metadata>);
}

pub trait EventType {
    fn event_type(&self) -> &'static str;
}

pub struct Context<'a, E>
where
    E: Entity,
{
    pub metadata: &'a Metadata<E::Metadata>,
    pub causation_tracking: &'a HashMap<StreamId, (u64, HashSet<Cow<'static, str>>)>,
    pub time: DateTime<Utc>,
    pub executed_at: Instant,
}

impl<'a, E> Context<'a, E>
where
    E: Entity,
{
    /// Returns true if this command should be skipped due to causation
    pub fn should_skip(&self) -> bool {
        match self.check_causation() {
            CausationCheck::Ok | CausationCheck::NoCausation => false,
            CausationCheck::AlreadyProcessed | CausationCheck::OutOfOrder { .. } => true,
        }
    }

    /// Detailed check if you need it for logging/debugging
    pub fn check_causation(&self) -> CausationCheck {
        match self.metadata.causation_event.as_ref() {
            None => CausationCheck::NoCausation,
            Some(causation_event) => {
                match self.causation_tracking.get(&causation_event.stream_id) {
                    None => CausationCheck::Ok, // Never seen this stream before
                    Some((max_version, commands_at_max)) => {
                        if causation_event.stream_version < *max_version {
                            // Out of order - reject
                            CausationCheck::OutOfOrder {
                                received: causation_event.stream_version,
                                max_seen: *max_version,
                            }
                        } else if causation_event.stream_version == *max_version {
                            // Same version - check if command was already processed
                            if self
                                .metadata
                                .causation_command
                                .as_ref()
                                .is_some_and(|cmd| commands_at_max.contains(&**cmd))
                            {
                                CausationCheck::AlreadyProcessed
                            } else {
                                CausationCheck::Ok
                            }
                        } else {
                            // New higher version - will clear the command set
                            CausationCheck::Ok
                        }
                    }
                }
            }
        }
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.time + self.executed_at.elapsed()
    }
}

impl<'a, E> Clone for Context<'a, E>
where
    E: Entity,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, E> Copy for Context<'a, E> where E: Entity {}

impl<'a, E> fmt::Debug for Context<'a, E>
where
    E: Entity,
    E::Metadata: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context")
            .field("metadata", &self.metadata)
            .field("causation_tracking", &self.causation_tracking)
            .field("time", &self.time)
            .field("executed_at", &self.executed_at)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CausationCheck {
    Ok,
    NoCausation,
    AlreadyProcessed,
    OutOfOrder { received: u64, max_seen: u64 },
}

#[derive(Clone, Debug)]
pub struct Event<E = GenericValue, M = GenericValue> {
    pub id: Uuid,
    pub partition_key: Uuid,
    pub partition_id: u16,
    pub transaction_id: Uuid,
    pub partition_sequence: u64,
    pub stream_version: u64,
    pub stream_id: StreamId,
    pub name: String,
    pub data: E,
    pub metadata: Metadata<M>,
    pub timestamp: DateTime<Utc>,
}

impl Event {
    pub fn as_entity<E>(
        self,
    ) -> Result<Event<E::Event, E::Metadata>, (Box<Event>, ciborium::value::Error)>
    where
        E: Entity,
    {
        let data = match self.data.0.deserialized() {
            Ok(data) => data,
            Err(err) => {
                return Err((Box::new(self), err));
            }
        };

        let metadata = match self.metadata.cast() {
            Ok(metadata) => metadata,
            Err(CastMetadataError { err, metadata }) => {
                return Err((
                    Box::new(Event {
                        id: self.id,
                        partition_key: self.partition_key,
                        partition_id: self.partition_id,
                        transaction_id: self.transaction_id,
                        partition_sequence: self.partition_sequence,
                        stream_version: self.stream_version,
                        stream_id: self.stream_id,
                        name: self.name,
                        data: self.data,
                        metadata: *metadata,
                        timestamp: self.timestamp,
                    }),
                    err,
                ))
            }
        };

        Ok(Event {
            id: self.id,
            partition_key: self.partition_key,
            partition_id: self.partition_id,
            transaction_id: self.transaction_id,
            partition_sequence: self.partition_sequence,
            stream_version: self.stream_version,
            stream_id: self.stream_id,
            name: self.name,
            data,
            metadata,
            timestamp: self.timestamp,
        })
    }
}

impl<E, M> Event<E, M> {
    #[inline]
    pub fn entity_id<Ent>(&self) -> Result<Ent::ID, <Ent::ID as FromStr>::Err>
    where
        Ent: Entity,
    {
        self.stream_id.cardinal_id().parse()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Metadata<T> {
    #[serde(rename = "ccmd", skip_serializing_if = "Option::is_none")]
    pub causation_command: Option<Cow<'static, str>>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub causation_event: Option<EventCausation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}

impl<T> Metadata<T> {
    pub fn is_empty(&self) -> bool {
        self.causation_command.is_none() && self.causation_event.is_none() && self.data.is_none()
    }

    pub fn with_data<U>(self, data: U) -> Metadata<U> {
        Metadata {
            causation_command: self.causation_command,
            causation_event: self.causation_event,
            data: Some(data),
        }
    }
}

impl Metadata<GenericValue> {
    pub fn cast<U>(self) -> Result<Metadata<U>, CastMetadataError>
    where
        U: DeserializeOwned + Default,
    {
        let data = match &self.data {
            Some(GenericValue(Value::Null)) => Some(U::default()),
            Some(data) => match data.0.deserialized() {
                Ok(data) => Some(data),
                Err(err) => {
                    return Err(CastMetadataError {
                        err,
                        metadata: Box::new(self),
                    });
                }
            },
            None => None,
        };
        Ok(Metadata {
            causation_command: self.causation_command,
            causation_event: self.causation_event,
            data,
        })
    }
}

impl<T> ops::Deref for Metadata<T> {
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> ops::DerefMut for Metadata<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventCausation {
    #[serde(rename = "ceid")]
    pub event_id: Uuid,
    #[serde(rename = "csid")]
    pub stream_id: StreamId,
    #[serde(rename = "csv")]
    pub stream_version: u64,
}

#[derive(Debug)]
pub struct CastMetadataError {
    pub err: ciborium::value::Error,
    pub metadata: Box<Metadata<GenericValue>>,
}

impl fmt::Display for CastMetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to cast metadata: {}", self.err)
    }
}

impl std::error::Error for CastMetadataError {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GenericValue(pub Value);

impl Default for GenericValue {
    fn default() -> Self {
        GenericValue(Value::Null)
    }
}

impl ops::Deref for GenericValue {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for GenericValue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct RateLimit {
    pub max_requests: u32,
    pub window_duration: Duration,
}
