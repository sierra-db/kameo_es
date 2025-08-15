use std::{
    collections::VecDeque,
    fmt, io,
    ops::{ControlFlow, Deref},
    time::Instant,
};

use chrono::{DateTime, Utc};
use im::HashMap;
use kameo::{
    actor::{ActorRef, WeakActorRef},
    error::{ActorStopReason, Infallible, PanicError, SendError},
    message::{Context, DynMessage, Message},
    reply::{BoxReplySender, DelegatedReply},
    Actor,
};
use kameo_es_core::EventType;
use redis::aio::MultiplexedConnection;
use sierradb_client::{
    AsyncTypedCommands, CurrentVersion, EMAppendEvent, ErrorCode, ExpectedVersion, SierraError,
};
use thiserror::Error;
use tracing::{debug, error, instrument};
use uuid::Uuid;

use crate::{
    command_service::{AppendedEvent, ExecuteResult},
    error::ExecuteError,
    transaction::{AbortTransaction, BeginTransaction, CommitTransaction, ResetTransaction},
    Apply, CausationMetadata, Command, Entity, Metadata, StreamId,
};

pub struct EntityActor<E: Entity + Apply + Clone> {
    conn: MultiplexedConnection,
    partition_key: Uuid,
    stream_id: StreamId,
    state: EntityActorState<E>,
    transaction: Option<(usize, EntityActorState<E>)>,
    conflict_reties: usize,
    buffered_commands: VecDeque<(
        Box<dyn DynMessage<EntityActor<E>>>,
        ActorRef<Self>,
        Option<BoxReplySender>,
    )>,
    is_processing_buffered_commands: bool,
}

#[derive(Clone)]
struct EntityActorState<E> {
    entity: E,
    version: CurrentVersion,
    correlation_id: Uuid,
    last_causations: HashMap<Uuid, CausationMetadata>,
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
    ) {
        assert_eq!(
            match self.version {
                CurrentVersion::Current(version) => version + 1,
                CurrentVersion::Empty => 0,
            },
            stream_version,
            "expected stream version {} but got {stream_version} for stream {stream_id}",
            match self.version {
                CurrentVersion::Current(version) => version + 1,
                CurrentVersion::Empty => 0,
            },
        );
        let causation = metadata.causation.clone();
        if self.correlation_id.is_nil() {
            assert_eq!(
                self.version,
                CurrentVersion::Empty,
                "expected correlation id to be nil only if the stream doesn't exist"
            );
            self.correlation_id = metadata.correlation_id;
        }
        self.entity.apply(event, metadata);
        self.version = CurrentVersion::Current(stream_version);

        if let Some(causation) = causation {
            self.last_causations
                .insert(causation.correlation_id, causation);
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
    {
        if !expected_version.is_satisfied_by(self.version) {
            return Err(ExecuteError::IncorrectExpectedVersion {
                stream_id: StreamId::new_from_parts(E::name(), id),
                current: self.version,
                expected: expected_version,
            });
        }

        let ctx = crate::Context {
            metadata: &metadata,
            last_causation: metadata
                .causation
                .as_ref()
                .and_then(|causation| self.last_causations.get(&causation.correlation_id)),
            time,
            executed_at,
        };
        let is_idempotent = self.entity.is_idempotent(&command, ctx);
        if !is_idempotent {
            return Ok(None);
        }

        let events = self
            .entity
            .handle(command, ctx)
            .map_err(ExecuteError::Handle)?;

        Ok(Some(events))
    }

    fn hydrate_metadata_correlation_id<F>(
        &mut self,
        metadata: &mut Metadata<E::Metadata>,
    ) -> Result<(), ExecuteError<F>> {
        match (
            !self.correlation_id.is_nil(),
            !metadata.correlation_id.is_nil(),
        ) {
            (true, true) => {
                // Correlation ID exists, make sure they match
                if &self.correlation_id != &metadata.correlation_id {
                    return Err(ExecuteError::CorrelationIDMismatch {
                        existing: self.correlation_id,
                        new: metadata.correlation_id,
                    });
                }
            }
            (true, false) => {
                // Correlation ID exists, use the existing one
                metadata.correlation_id = self.correlation_id;
            }
            (false, true) => {
                // Correlation ID doesn't exist, make sure there's no current stream version
                if &self.version != &CurrentVersion::Empty {
                    return Err(ExecuteError::CorrelationIDNotSetOnExistingEntity);
                }
            }
            (false, false) => {
                // Correlation ID doesn't exist, and none defined
                metadata.correlation_id = Uuid::new_v4();
            }
        }

        Ok(())
    }
}

impl<E: Entity + Apply + Clone> EntityActor<E> {
    pub fn new(
        entity: E,
        partition_key: Uuid,
        stream_id: StreamId,
        conn: MultiplexedConnection,
    ) -> Self {
        EntityActor {
            conn,
            partition_key,
            stream_id,
            state: EntityActorState {
                entity,
                version: CurrentVersion::Empty,
                correlation_id: Uuid::nil(),
                last_causations: HashMap::new(),
            },
            transaction: None,
            conflict_reties: 5,
            buffered_commands: VecDeque::new(),
            is_processing_buffered_commands: false,
        }
    }

    fn current_state(&mut self) -> &mut EntityActorState<E> {
        self.transaction
            .as_mut()
            .map(|(_, entity)| entity)
            .unwrap_or(&mut self.state)
    }

    async fn resync_with_db(&mut self) -> anyhow::Result<()>
    where
        E: Clone,
    {
        loop {
            let original_version = self.state.version;
            let from_version = match self.state.version {
                CurrentVersion::Current(version) => version + 1,
                CurrentVersion::Empty => 0,
            };

            let batch = self
                .conn
                .escan(&self.stream_id, from_version, None, Some(100))
                .await?;

            for event in batch.events {
                assert_eq!(
                    match self.state.version {
                        CurrentVersion::Current(version) => version + 1,
                        CurrentVersion::Empty => 0,
                    },
                    event.stream_version,
                    "expected stream version {} but got {} for stream {}",
                    match self.state.version {
                        CurrentVersion::Current(version) => version + 1,
                        CurrentVersion::Empty => 0,
                    },
                    event.stream_version,
                    event.stream_id,
                );
                let ent_event = ciborium::from_reader(event.payload.as_slice())?;
                let metadata: Metadata<E::Metadata> =
                    ciborium::from_reader(event.metadata.as_slice())?;
                self.state
                    .apply(ent_event, &self.stream_id, event.stream_version, metadata);
            }

            if !batch.has_more {
                break;
            }

            if self.state.version == original_version {
                error!("existing resync loop to potential infinite loop");
                break;
            }
        }

        if let Some((_, tx_state)) = &mut self.transaction {
            *tx_state = self.state.clone();
        }

        Ok(())
    }

    async fn append_events(
        &mut self,
        events: Vec<E::Event>,
        metadata: Metadata<E::Metadata>,
        timestamp: DateTime<Utc>,
    ) -> Result<Vec<AppendedEvent<E::Event>>, AppendEventsError<Vec<E::Event>, Metadata<E::Metadata>>>
    {
        let mut metadata_buf = Vec::new();
        ciborium::into_writer(&metadata, &mut metadata_buf)?;

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
            Err(err) => {
                return match SierraError::from_redis_error(&err) {
                    SierraError::Protocol {
                        code: ErrorCode::WrongVer,
                        message,
                    } => {
                        let (current, expected) = parse_sequence_error(&message).unwrap();
                        Err(AppendEventsError::IncorrectExpectedVersion {
                            stream_id: self.stream_id.clone(),
                            current,
                            expected,
                            events,
                            metadata,
                        })
                    }
                    err => Err(AppendEventsError::Database(err)),
                }
            }
        };

        let starting_version = match self.state.version {
            CurrentVersion::Current(v) => v + 1,
            CurrentVersion::Empty => 0,
        };
        let mut version = starting_version;

        for event in &events {
            self.state
                .apply(event.clone(), &self.stream_id, version, metadata.clone());
            version += 1;
        }

        let appended = events
            .into_iter()
            .zip(info.events)
            .map(|(event, event_info)| AppendedEvent {
                event,
                event_id: event_info.event_id,
                stream_version: event_info.stream_version,
                timestamp: event_info.timestamp.into(),
            })
            .collect();

        Ok(appended)
    }

    fn buffer_begin_transaction(
        &mut self,
        mut msg: BeginTransaction,
        ctx: &mut Context<Self, DelegatedReply<Result<ActorRef<EntityActor<E>>, Infallible>>>,
    ) -> DelegatedReply<Result<ActorRef<EntityActor<E>>, Infallible>> {
        let (delegated_reply, reply_sender) = ctx.reply_sender();
        msg.is_buffered = true;
        self.buffered_commands.push_back((
            Box::new(msg),
            ctx.actor_ref().clone(),
            reply_sender.map(|tx| tx.boxed()),
        ));
        delegated_reply
    }

    fn buffer_command<C>(
        &mut self,
        mut exec: Execute<E::ID, C, E::Metadata>,
        ctx: &mut Context<Self, DelegatedReply<Result<ExecuteResult<E>, ExecuteError<E::Error>>>>,
    ) -> DelegatedReply<Result<ExecuteResult<E>, ExecuteError<E::Error>>>
    where
        E: Command<C>,
        E::ID: fmt::Debug,
        E::Metadata: fmt::Debug,
        C: fmt::Debug + Clone + Send + 'static,
    {
        let (delegated_reply, reply_sender) = ctx.reply_sender();
        exec.is_buffered = true;
        self.buffered_commands.push_back((
            Box::new(exec),
            ctx.actor_ref().clone(),
            reply_sender.map(|tx| tx.boxed()),
        ));
        delegated_reply
    }
}

impl<E> Actor for EntityActor<E>
where
    E: Entity + Apply + Clone,
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
        _reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        error!("entity actor stopped");
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
    pub tx_id: Option<usize>,
    pub is_buffered: bool,
}

impl<E, C> Message<Execute<E::ID, C, E::Metadata>> for EntityActor<E>
where
    E: Entity + Command<C> + Apply + Clone,
    E::ID: fmt::Debug,
    E::Metadata: fmt::Debug,
    C: fmt::Debug + Clone + Send + 'static,
{
    type Reply = DelegatedReply<Result<ExecuteResult<E>, ExecuteError<E::Error>>>;

    #[instrument(name = "handle_execute", skip(self, ctx))]
    async fn handle(
        &mut self,
        mut exec: Execute<E::ID, C, E::Metadata>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if exec.partition_key != self.partition_key {
            return ctx.reply(Err(ExecuteError::PartitionKeyMismatch {
                existing: self.partition_key,
                new: exec.partition_key,
            }));
        }

        if !exec.is_buffered && self.is_processing_buffered_commands {
            return self.buffer_command(exec, ctx);
        }

        match (&self.transaction, exec.tx_id) {
            (None, None) => {} // No transaction, continue processing
            (None, Some(_)) => {
                panic!("command being executed without beginning a transaction");
            }
            (Some(_), None) => {
                // Existing transaction, buffer
                return self.buffer_command(exec, ctx);
            }
            (Some((existing_tx_id, _)), Some(new_tx_id)) => {
                // Existing transaction, ensure its the same one otherwise buffer
                if existing_tx_id != &new_tx_id {
                    return self.buffer_command(exec, ctx);
                }
            }
        }

        let mut attempt = 0;
        loop {
            let state = self.current_state();
            if let Err(err) = state.hydrate_metadata_correlation_id::<E::Error>(&mut exec.metadata)
            {
                return ctx.reply(Err(err));
            }
            let res = state.execute(
                &exec.id,
                exec.command.clone(),
                &exec.metadata,
                exec.expected_version,
                exec.time,
                exec.executed_at,
            );
            match res {
                Ok(Some(events)) => {
                    match &mut self.transaction {
                        Some((_, state)) => {
                            // Apply to temporary state
                            let starting_version = state.version;
                            let mut version = match state.version {
                                CurrentVersion::Current(v) => v + 1,
                                CurrentVersion::Empty => 0,
                            };

                            for event in events.clone() {
                                state.apply(event, &self.stream_id, version, exec.metadata.clone());
                                version += 1;
                            }

                            return ctx.reply(Ok(ExecuteResult::PendingTransaction {
                                entity_actor_ref: ctx.actor_ref().clone(),
                                events,
                                expected_version: starting_version.as_expected_version(),
                                correlation_id: exec.metadata.correlation_id,
                            }));
                        }
                        None => {
                            if events.is_empty() {
                                return ctx.reply(Ok(ExecuteResult::Executed(vec![])));
                            }

                            // Append to event store directly
                            match self.append_events(events, exec.metadata, exec.time).await {
                                Ok(appended) => {
                                    return ctx.reply(Ok(ExecuteResult::Executed(appended)))
                                }
                                Err(AppendEventsError::IncorrectExpectedVersion {
                                    stream_id,
                                    current,
                                    expected,
                                    metadata,
                                    ..
                                }) => {
                                    debug!(%stream_id, %current, %expected, "write conflict");
                                    if attempt == self.conflict_reties {
                                        return ctx.reply(Err(ExecuteError::TooManyConflicts {
                                            stream_id,
                                        }));
                                    }

                                    attempt += 1;
                                    exec.metadata = metadata;
                                }
                                Err(err) => return ctx.reply(Err(err.into())),
                            }
                        }
                    }
                }
                Ok(None) => return ctx.reply(Ok(ExecuteResult::Idempotent)),
                Err(err) => return ctx.reply(Err(err)),
            }
        }
    }
}

#[derive(Debug)]
struct ProcessNextBufferedCommand;

impl<E> Message<ProcessNextBufferedCommand> for EntityActor<E>
where
    E: Entity + Apply + Clone,
{
    type Reply = ();

    #[instrument(name = "handle_process_next_buffered_command", skip(self, ctx))]
    async fn handle(
        &mut self,
        _msg: ProcessNextBufferedCommand,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.is_processing_buffered_commands = true;

        if let Some((exec, actor_ref, tx)) = self.buffered_commands.pop_front() {
            if let Err(err) = exec.handle_dyn(self, actor_ref, tx).await {
                std::panic::resume_unwind(Box::new(err));
            }
        }

        // If more commands are buffered, enqueue another processing message
        if !self.buffered_commands.is_empty() {
            let _ = ctx.actor_ref().tell(ProcessNextBufferedCommand);
        } else {
            self.is_processing_buffered_commands = false;
        }
    }
}

impl<E> Message<BeginTransaction> for EntityActor<E>
where
    E: Entity + Apply + Clone,
{
    type Reply = DelegatedReply<Result<ActorRef<EntityActor<E>>, Infallible>>;

    #[instrument(name = "handle_prepare_transaction", skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: BeginTransaction,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if !msg.is_buffered && self.is_processing_buffered_commands {
            // Message is no buffered and we are processing buffered commands, so we'll buffer this
            return self.buffer_begin_transaction(msg, ctx);
        }

        match &self.transaction {
            Some((existing_tx_id, _)) => {
                if existing_tx_id == &msg.tx_id {
                    panic!("attempted to begin the same transaction multiple times");
                }

                // We're already in a transaction, so we'll buffer this transaction begin request
                self.buffer_begin_transaction(msg, ctx)
            }
            None => {
                // We're not in a transaction, so we can begin a new one
                self.transaction = Some((msg.tx_id, self.state.clone()));
                ctx.reply(Ok(ctx.actor_ref().clone()))
            }
        }
    }
}

impl<E> Message<CommitTransaction> for EntityActor<E>
where
    E: Entity + Apply + Clone,
{
    type Reply = ();

    #[instrument(name = "handle_commit_transaction", skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: CommitTransaction,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let Some((tx_id, state)) = self.transaction.take() {
            if tx_id == msg.tx_id {
                self.state = state;
            } else {
                error!("tried to commit transaction with wrong txn id");
                return;
            }
        }

        // Start processing buffered commands incrementally
        if !self.is_processing_buffered_commands && !self.buffered_commands.is_empty() {
            let _ = ctx.actor_ref().tell(ProcessNextBufferedCommand);
        }
    }
}

impl<E> Message<ResetTransaction> for EntityActor<E>
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
        self.resync_with_db().await?;
        if let Some((tx_id, state)) = &mut self.transaction {
            if tx_id == &msg.tx_id {
                *state = self.state.clone();
            } else {
                error!("tried to commit transaction with wrong txn id");
            }
        }
        Ok(())
    }
}

impl<E> Message<AbortTransaction> for EntityActor<E>
where
    E: Entity + Apply + Clone,
{
    type Reply = ();

    #[instrument(name = "handle_abort_transaction", skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: AbortTransaction,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self
            .transaction
            .as_ref()
            .map(|(tx_id, _)| tx_id == &msg.tx_id)
            .unwrap_or(false)
        {
            self.transaction = None;
        }

        // Start processing buffered commands incrementally
        if !self.is_processing_buffered_commands && !self.buffered_commands.is_empty() {
            let _ = ctx.actor_ref().tell(ProcessNextBufferedCommand);
        }
    }
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

fn parse_sequence_error(message: &str) -> Option<(CurrentVersion, ExpectedVersion)> {
    // Find "current partition sequence is "
    let current_start = message.find("current partition sequence is ")?;
    let current_start = current_start + "current partition sequence is ".len();

    // Find " but expected "
    let but_expected_pos = message.find(" but expected ")?;
    let current_str = &message[current_start..but_expected_pos];

    // Parse current value
    let current = match current_str {
        "<empty>" => CurrentVersion::Empty,
        s => CurrentVersion::Current(s.parse::<u64>().ok()?),
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
