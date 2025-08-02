use std::{collections::VecDeque, fmt, ops::ControlFlow, time::Instant};

use chrono::{DateTime, Utc};
use eventus::{CurrentVersion, ExpectedVersion};
use futures::StreamExt;
use im::HashMap;
use kameo::{
    actor::{ActorRef, WeakActorRef},
    error::{ActorStopReason, Infallible, PanicError, SendError},
    message::{Context, DynMessage, Message},
    reply::{BoxReplySender, DelegatedReply},
    Actor,
};
use tonic::Status;
use tracing::{debug, error, instrument};
use uuid::Uuid;

use crate::{
    command_service::{AppendedEvent, ExecuteResult},
    error::ExecuteError,
    event_store::{AppendEventsError, EventStore},
    transaction::{AbortTransaction, BeginTransaction, CommitTransaction, ResetTransaction},
    Apply, CausationMetadata, Command, Entity, Metadata, StreamID,
};

pub struct EntityActor<E: Entity + Apply + Clone> {
    event_store: EventStore,
    stream_id: StreamID,
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
        stream_id: &StreamID,
        stream_version: u64,
        metadata: Metadata<E::Metadata>,
    ) {
        assert_eq!(
            match self.version {
                CurrentVersion::Current(version) => version + 1,
                CurrentVersion::NoStream => 0,
            },
            stream_version,
            "expected stream version {} but got {stream_version} for stream {stream_id}",
            match self.version {
                CurrentVersion::Current(version) => version + 1,
                CurrentVersion::NoStream => 0,
            },
        );
        let causation = metadata.causation.clone();
        if self.correlation_id.is_nil() {
            assert_eq!(
                self.version,
                CurrentVersion::NoStream,
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
        if !expected_version.validate(self.version) {
            return Err(ExecuteError::IncorrectExpectedVersion {
                stream_id: StreamID::new_from_parts(E::name(), id),
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
                if &self.version != &CurrentVersion::NoStream {
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
    pub fn new(entity: E, stream_id: StreamID, eventus_client: EventStore) -> Self {
        EntityActor {
            event_store: eventus_client,
            stream_id,
            state: EntityActorState {
                entity,
                version: CurrentVersion::NoStream,
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
        let from_version = match self.state.version {
            CurrentVersion::Current(version) => version + 1,
            CurrentVersion::NoStream => 0,
        };

        let mut stream = self
            .event_store
            .get_stream_events(self.stream_id.clone(), from_version)
            .await?;

        while let Some(res) = stream.next().await {
            let batch = res?;
            for event in batch {
                assert_eq!(
                    match self.state.version {
                        CurrentVersion::Current(version) => version + 1,
                        CurrentVersion::NoStream => 0,
                    },
                    event.stream_version,
                    "expected stream version {} but got {} for stream {}",
                    match self.state.version {
                        CurrentVersion::Current(version) => version + 1,
                        CurrentVersion::NoStream => 0,
                    },
                    event.stream_version,
                    event.stream_id,
                );
                let ent_event = ciborium::from_reader(event.event_data.as_ref())?;
                let metadata: Metadata<E::Metadata> =
                    ciborium::from_reader(event.metadata.as_ref())?;
                self.state
                    .apply(ent_event, &self.stream_id, event.stream_version, metadata);
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
        let (starting_event_id, timestamp) = self
            .event_store
            .append_to_stream(
                self.stream_id.clone(),
                events.clone(),
                self.state.version.as_expected_version(),
                metadata.clone(),
                timestamp,
            )
            .await?;
        let starting_version = match self.state.version {
            CurrentVersion::Current(v) => v + 1,
            CurrentVersion::NoStream => 0,
        };
        let mut version = starting_version;

        for event in &events {
            self.state
                .apply(event.clone(), &self.stream_id, version, metadata.clone());
            version += 1;
        }

        let appended = events
            .into_iter()
            .enumerate()
            .map(|(i, event)| AppendedEvent {
                event,
                event_id: starting_event_id + i as u64,
                stream_version: starting_version + i as u64,
                timestamp,
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
                                CurrentVersion::NoStream => 0,
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

impl<M, E> From<SendError<M, Status>> for ExecuteError<E> {
    fn from(err: SendError<M, Status>) -> Self {
        match err {
            SendError::ActorNotRunning(_) => ExecuteError::EventStoreActorNotRunning,
            SendError::ActorStopped => ExecuteError::EventStoreActorStopped,
            SendError::MailboxFull(_) => unreachable!("sending is always awaited"),
            SendError::HandlerError(err) => err.into(),
            SendError::Timeout(_) => unreachable!("no timeouts are used in the event store"),
        }
    }
}
