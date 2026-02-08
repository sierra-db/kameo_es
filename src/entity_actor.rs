use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt, io,
    ops::{ControlFlow, Deref},
    time::{Instant, SystemTime},
};

use chrono::{DateTime, Utc};
use kameo::prelude::*;
use kameo_es_core::{CommandName, EventType};
use redis::aio::MultiplexedConnection;
use sierradb_client::{
    AsyncTypedCommands, CurrentVersion, EMAppendEvent, ErrorCode, ExpectedVersion, SierraError,
};
use thiserror::Error;
use tracing::{debug, error, instrument, warn};
use uuid::Uuid;

use crate::{
    command_service::{AppendedEvent, ExecuteResult},
    error::{parse_stream_version_string, ExecuteError, ParsedStream},
    transaction::{AbortTransaction, BeginTransaction, CommitTransaction, ResetTransaction},
    Apply, Command, Entity, Metadata, StreamId,
};

pub struct EntityActor<E: Entity + Apply> {
    conn: MultiplexedConnection,
    partition_key: Uuid,
    stream_id: StreamId,
    state: EntityActorState<E>,
    conflict_reties: usize,
}

#[derive(Actor)]
pub struct EntityTransactionActor<E: Entity + Apply> {
    conn: MultiplexedConnection,
    partition_key: Uuid,
    stream_id: StreamId,
    original_state: EntityActorState<E>,
    state: EntityActorState<E>,
    committed: bool,
}

#[derive(Clone)]
struct EntityActorState<E> {
    entity: E,
    version: CurrentVersion,
    causation_tracking: HashMap<StreamId, (u64, HashSet<Cow<'static, str>>)>,
    command_history: Vec<Option<(Cow<'static, str>, SystemTime)>>,
}

impl<E> EntityActorState<E>
where
    E: Entity + Apply,
{
    fn apply(
        &mut self,
        event: E::Event,
        stream_id: &StreamId,
        stream_version: u64,
        metadata: Metadata<E::Metadata>,
        timestamp: SystemTime,
    ) {
        assert_eq!(
            self.version.next(),
            stream_version,
            "expected stream version {} but got {stream_version} for stream {stream_id}",
            self.version.next(),
        );
        let causation_command = metadata.causation_command.clone();
        let causation_event = metadata.causation_event.clone();

        self.entity.apply(event, metadata);
        self.version = CurrentVersion::Current(stream_version);

        self.command_history
            .push(causation_command.clone().map(|cmd| (cmd, timestamp)));

        if let Some((causation_command, causation_event)) = causation_command.zip(causation_event) {
            match self.causation_tracking.entry(causation_event.stream_id) {
                Entry::Occupied(mut entry) => {
                    let (max_ver, commands) = entry.get_mut();
                    if causation_event.stream_version > *max_ver {
                        // New version - clear old commands and update
                        *max_ver = causation_event.stream_version;
                        commands.clear();
                        commands.insert(causation_command);
                    } else if causation_event.stream_version == *max_ver {
                        // Same version - just add the command
                        commands.insert(causation_command);
                    }
                    // Older versions are rejected earlier, so we don't handle them here
                }
                Entry::Vacant(entry) => {
                    let mut commands = HashSet::new();
                    commands.insert(causation_command);
                    entry.insert((causation_event.stream_version, commands));
                }
            }
        }
    }

    fn execute<C>(
        &mut self,
        id: &E::ID,
        command: C,
        metadata: &Metadata<E::Metadata>,
        expected_version: ExpectedVersion,
        time: DateTime<Utc>,
        executed_at: Instant,
    ) -> Result<Option<Vec<E::Event>>, ExecuteError<E::Error>>
    where
        E: Command<C>,
        C: CommandName,
    {
        if !expected_version.is_satisfied_by(self.version) {
            return Err(ExecuteError::IncorrectExpectedVersion {
                stream_id: StreamId::new_from_parts(E::category(), id),
                current: self.version,
                expected: expected_version,
            });
        }

        if let Some(rate_limit) = self.entity.rate_limit() {
            let now = SystemTime::now();
            let window_start = now - rate_limit.window_duration;
            let command_name = C::command_name();

            // Count matching commands within the window
            let count = self
                .command_history
                .iter()
                .filter_map(|entry| entry.as_ref()) // Skip None entries
                .filter(|(name, timestamp)| name == command_name && *timestamp > window_start)
                .count();

            if count >= rate_limit.max_requests as usize {
                return Err(ExecuteError::RateLimitExceeded {
                    max_requests: rate_limit.max_requests,
                    window_duration: rate_limit.window_duration,
                });
            }
        }

        let ctx = crate::Context {
            metadata,
            causation_tracking: &self.causation_tracking,
            time,
            executed_at,
        };
        let is_idempotent = self.entity.is_idempotent(&command, ctx);
        if is_idempotent {
            return Ok(None);
        }

        let events = self
            .entity
            .handle(command, ctx)
            .map_err(ExecuteError::Handle)?;

        Ok(Some(events))
    }

    async fn resync_with_db(
        &mut self,
        conn: &mut MultiplexedConnection,
        stream_id: &StreamId,
        partition_key: Uuid,
    ) -> Result<(), SierraError> {
        loop {
            let original_version = self.version;
            let from_version = self.version.next();

            let batch = conn
                .escan_with_partition_key(
                    stream_id,
                    partition_key,
                    from_version,
                    None,
                    Some(10_000),
                )
                .await?;

            for event in batch.events {
                assert_eq!(
                    self.version.next(),
                    event.stream_version,
                    "expected stream version {} but got {} for stream {}",
                    self.version.next(),
                    event.stream_version,
                    event.stream_id,
                );
                let ent_event = ciborium::from_reader(event.payload.as_slice()).map_err(|err| {
                    SierraError::Protocol {
                        code: ErrorCode::ReadErr,
                        message: Some(format!("failed to deserialize payload: {err}")),
                    }
                })?;
                let metadata: Metadata<E::Metadata> = if event.metadata.is_empty() {
                    Metadata {
                        causation_command: None,
                        causation_event: None,
                        data: None,
                    }
                } else {
                    ciborium::from_reader(event.metadata.as_slice()).map_err(|err| {
                        SierraError::Protocol {
                            code: ErrorCode::ReadErr,
                            message: Some(format!("failed to deserialize metadata: {err}")),
                        }
                    })?
                };
                self.apply(
                    ent_event,
                    stream_id,
                    event.stream_version,
                    metadata,
                    event.timestamp,
                );
            }

            if !batch.has_more {
                break;
            }

            if self.version == original_version {
                error!("existing resync loop to potential infinite loop");
                break;
            }
        }

        Ok(())
    }
}

impl<E: Entity + Apply> EntityActor<E> {
    pub fn new(
        conn: MultiplexedConnection,
        partition_key: Uuid,
        stream_id: StreamId,
        entity: E,
    ) -> Self {
        EntityActor {
            conn,
            partition_key,
            stream_id,
            state: EntityActorState {
                entity,
                version: CurrentVersion::Empty,
                causation_tracking: HashMap::new(),
                command_history: Vec::new(),
            },
            conflict_reties: 5,
        }
    }

    async fn resync_with_db(&mut self) -> Result<(), SierraError> {
        self.state
            .resync_with_db(&mut self.conn, &self.stream_id, self.partition_key)
            .await
    }

    async fn append_events(
        &mut self,
        events: Vec<E::Event>,
        metadata: Metadata<E::Metadata>,
        timestamp: DateTime<Utc>,
    ) -> Result<Vec<AppendedEvent<E::Event>>, AppendEventsError<Vec<E::Event>, Metadata<E::Metadata>>>
    {
        let mut metadata_buf = Vec::new();
        if !metadata.is_empty() {
            ciborium::into_writer(&metadata, &mut metadata_buf)?;
        }

        let expected_version = self.state.version.as_expected_version();
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

                Ok(
                    EMAppendEvent::new(self.stream_id.deref(), event.event_type())
                        .payload(payload)
                        .metadata(&metadata_buf)
                        .expected_version(expected_version)
                        .timestamp(timestamp.into()),
                )
            })
            .collect::<Result<Vec<_>, AppendEventsError<_, _>>>()?;

        let info = match self.conn.emappend(self.partition_key, &new_events).await {
            Ok(info) => info,
            Err(err) => return Err(AppendEventsError::from_sierra_err(err, events, metadata)),
        };

        let starting_version = self.state.version.next();
        let mut version = starting_version;

        for event in &events {
            self.state.apply(
                event.clone(),
                &self.stream_id,
                version,
                metadata.clone(),
                timestamp.into(),
            );
            version += 1;
        }

        let appended = events
            .into_iter()
            .zip(info.events)
            .map(|(event, event_info)| AppendedEvent {
                event,
                event_id: event_info.event_id,
                partition_id: event_info.partition_id,
                partition_sequence: event_info.partition_sequence,
                stream_version: event_info.stream_version,
                timestamp: event_info.timestamp.into(),
            })
            .collect();

        Ok(appended)
    }
}

impl<E> Actor for EntityActor<E>
where
    E: Entity + Apply,
{
    type Args = Self;
    type Error = anyhow::Error;

    fn name() -> &'static str {
        "EntityActor"
    }

    async fn on_start(
        mut args: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        args.resync_with_db().await?;
        Ok(args)
    }

    async fn on_panic(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
        error!("entity actor panicked: {err}");
        Ok(ControlFlow::Break(ActorStopReason::Panicked(err)))
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        match reason {
            ActorStopReason::Normal => {}
            ActorStopReason::LinkDied { .. } => {}
            reason => {
                error!("entity actor stopped: {reason}");
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Execute<I, C, M> {
    pub id: I,
    pub command: C,
    pub partition_key: Uuid,
    pub metadata: Metadata<M>,
    pub expected_version: ExpectedVersion,
    pub time: DateTime<Utc>,
    pub executed_at: Instant,
    pub dry_run: bool,
}

impl<E, C> Message<Execute<E::ID, C, E::Metadata>> for EntityActor<E>
where
    E: Entity + Command<C> + Apply,
    E::ID: fmt::Debug,
    E::Metadata: fmt::Debug,
    C: CommandName + fmt::Debug + Clone + Send + 'static,
{
    type Reply = Result<ExecuteResult<E>, ExecuteError<E::Error>>;

    #[instrument(name = "handle_execute", skip(self, _ctx))]
    async fn handle(
        &mut self,
        mut exec: Execute<E::ID, C, E::Metadata>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if exec.partition_key != self.partition_key {
            return Err(ExecuteError::PartitionKeyMismatch {
                entity: E::category(),
                existing: self.partition_key,
                new: exec.partition_key,
            });
        }

        let mut attempt = 0;
        loop {
            let res = self.state.execute(
                &exec.id,
                exec.command.clone(),
                &exec.metadata,
                exec.expected_version,
                exec.time,
                exec.executed_at,
            );
            match res {
                Ok(Some(events)) => {
                    if events.is_empty() {
                        return Ok(ExecuteResult::Executed(vec![]));
                    }

                    if exec.dry_run {
                        let starting_version = self.state.version.next();
                        return Ok(ExecuteResult::Executed(
                            events
                                .into_iter()
                                .enumerate()
                                .map(|(i, event)| AppendedEvent {
                                    event,
                                    event_id: Uuid::new_v4(),
                                    partition_id: 0,
                                    partition_sequence: 0,
                                    stream_version: starting_version + i as u64,
                                    timestamp: exec.time,
                                })
                                .collect(),
                        ));
                    }

                    // Append to event store directly
                    match self.append_events(events, exec.metadata, exec.time).await {
                        Ok(appended) => return Ok(ExecuteResult::Executed(appended)),
                        Err(AppendEventsError::IncorrectExpectedVersion {
                            stream_id,
                            current,
                            expected,
                            metadata,
                            ..
                        }) => {
                            debug!(%stream_id, %current, %expected, "write conflict");
                            if attempt == self.conflict_reties {
                                return Err(ExecuteError::TooManyConflicts { stream_id });
                            }

                            self.resync_with_db().await?;

                            attempt += 1;
                            exec.metadata = metadata;
                        }
                        Err(err) => return Err(err.into()),
                    }
                }
                Ok(None) => {
                    return Ok(ExecuteResult::Idempotent {
                        current_version: self.state.version,
                    })
                }
                Err(err) => return Err(err),
            }
        }
    }
}

impl<E, C> Message<Execute<E::ID, C, E::Metadata>> for EntityTransactionActor<E>
where
    E: Entity + Command<C> + Apply,
    E::ID: fmt::Debug,
    E::Metadata: fmt::Debug,
    C: CommandName + fmt::Debug + Clone + Send + 'static,
{
    type Reply = Result<ExecuteResult<E>, ExecuteError<E::Error>>;

    #[instrument(name = "handle_execute", skip(self, ctx))]
    async fn handle(
        &mut self,
        exec: Execute<E::ID, C, E::Metadata>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if exec.partition_key != self.partition_key {
            return Err(ExecuteError::PartitionKeyMismatch {
                entity: E::category(),
                existing: self.partition_key,
                new: exec.partition_key,
            });
        }

        let res = self.state.execute(
            &exec.id,
            exec.command.clone(),
            &exec.metadata,
            exec.expected_version,
            exec.time,
            exec.executed_at,
        )?;
        match res {
            Some(events) => {
                // Apply to temporary state
                let starting_version = self.state.version;
                let mut version = self.state.version.next();

                for event in events.clone() {
                    self.state.apply(
                        event,
                        &self.stream_id,
                        version,
                        exec.metadata.clone(),
                        exec.time.into(),
                    );
                    version += 1;
                }

                Ok(ExecuteResult::PendingTransaction {
                    entity_actor_ref: ctx.actor_ref().clone(),
                    events,
                    expected_version: starting_version.as_expected_version(),
                })
            }
            None => Ok(ExecuteResult::Idempotent {
                current_version: self.state.version,
            }),
        }
    }
}

impl<E> Message<BeginTransaction> for EntityActor<E>
where
    E: Entity + Apply + Clone,
{
    type Reply = DelegatedReply<ActorRef<EntityTransactionActor<E>>>;

    #[instrument(name = "handle_begin_transaction", skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: BeginTransaction,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let prepared = EntityTransactionActor::prepare();
        let delegated_reply = ctx.reply(prepared.actor_ref().clone());

        let res = prepared
            .run(EntityTransactionActor {
                conn: self.conn.clone(),
                partition_key: self.partition_key,
                stream_id: self.stream_id.clone(),
                original_state: self.state.clone(),
                state: self.state.clone(),
                committed: false,
            })
            .await;
        match res {
            Ok((tx, ActorStopReason::Normal)) => {
                if tx.committed {
                    self.state = tx.state;
                } else {
                    debug!("ignoring transaction due to it being uncommitted");
                }
            }
            Ok((_, reason)) => {
                warn!("ignoring transaction due to abnormal stop reason: {reason}");
            }
            Err(err) => {
                warn!("ignoring transaction due to actor panicking: {err}");
            }
        }

        delegated_reply
    }
}

impl<E> Message<CommitTransaction> for EntityTransactionActor<E>
where
    E: Entity + Apply,
{
    type Reply = ();

    #[instrument(name = "handle_commit_transaction", skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: CommitTransaction,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.committed = true;
        ctx.stop();
    }
}

impl<E> Message<ResetTransaction> for EntityTransactionActor<E>
where
    E: Entity + Apply + Clone,
{
    type Reply = anyhow::Result<()>;

    #[instrument(name = "handle_reset_transaction", skip(self, _ctx))]
    async fn handle(
        &mut self,
        msg: ResetTransaction,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.state
            .resync_with_db(&mut self.conn, &self.stream_id, self.partition_key)
            .await?;
        self.state = self.original_state.clone();

        Ok(())
    }
}

impl<E> Message<AbortTransaction> for EntityTransactionActor<E>
where
    E: Entity + Apply,
{
    type Reply = ();

    #[instrument(name = "handle_abort_transaction", skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: AbortTransaction,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ctx.stop();
    }
}

#[derive(Error)]
pub enum AppendEventsError<E, M> {
    #[error(transparent)]
    Database(SierraError),
    #[error("expected '{stream_id}' version {expected} but got {current}")]
    IncorrectExpectedVersion {
        partition_key: Uuid,
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

impl<E, M> AppendEventsError<E, M> {
    pub fn from_sierra_err(err: impl Into<SierraError>, events: E, metadata: M) -> Self {
        match err.into() {
            SierraError::Protocol {
                code: ErrorCode::WrongVer,
                message,
            } => {
                let ParsedStream {
                    partition_key,
                    stream_id,
                    current,
                    expected,
                } = parse_stream_version_string(message.as_ref().unwrap()).unwrap();
                AppendEventsError::IncorrectExpectedVersion {
                    partition_key,
                    stream_id,
                    current,
                    expected,
                    events,
                    metadata,
                }
            }
            err => AppendEventsError::Database(err),
        }
    }
}

impl<E, Ev, M> From<AppendEventsError<Ev, M>> for ExecuteError<E> {
    fn from(err: AppendEventsError<Ev, M>) -> Self {
        match err {
            AppendEventsError::Database(err) => ExecuteError::Database(err),
            AppendEventsError::IncorrectExpectedVersion {
                stream_id,
                current,
                expected,
                ..
            } => ExecuteError::IncorrectExpectedVersion {
                stream_id,
                current,
                expected,
            },
            AppendEventsError::InvalidTimestamp => ExecuteError::InvalidTimestamp,
            AppendEventsError::SerializeEvent(err) => ExecuteError::SerializeEvent(err),
        }
    }
}

impl<M, E, Ev, Me> From<SendError<M, AppendEventsError<Ev, Me>>> for ExecuteError<E> {
    fn from(err: SendError<M, AppendEventsError<Ev, Me>>) -> Self {
        match err {
            SendError::ActorNotRunning(_) => ExecuteError::EventStoreActorNotRunning,
            SendError::ActorStopped => ExecuteError::EventStoreActorStopped,
            SendError::MailboxFull(_) => unreachable!("sending is always awaited"),
            SendError::HandlerError(err) => err.into(),
            SendError::Timeout(_) => unreachable!("no timeouts are used in the event store"),
        }
    }
}
