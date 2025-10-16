use std::{
    any, borrow::Cow, collections::HashMap, fmt, future::IntoFuture, marker::PhantomData, mem,
    num::NonZeroUsize, ops::ControlFlow, time::Instant, vec::IntoIter,
};

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use futures::{future::BoxFuture, FutureExt};
use kameo::prelude::*;
use kameo_es_core::CommandName;
use lru::LruCache;
use redis::aio::MultiplexedConnection;
use sierradb_client::{
    stream_partition_key, AsyncTypedCommands, CurrentVersion, EMAppendEvent, ErrorCode,
    ExpectedVersion, SierraError,
};
use tracing::{debug, error};
use uuid::Uuid;

use crate::{
    entity_actor::{self, EntityActor, EntityTransactionActor},
    error::{parse_stream_version_string, ExecuteError, ParsedStream},
    transaction::{self, Transaction, TransactionOutcome},
    Apply, Command, Entity, Event, EventCausation, EventType, Metadata, StreamId,
};

#[derive(Clone, Debug)]
pub struct CommandService {
    actor_ref: ActorRef<CommandServiceActor>,
    conn: MultiplexedConnection,
}

impl CommandService {
    /// Creates a new command service using an event store client connection and default worker count.
    #[inline]
    pub fn new(conn: MultiplexedConnection) -> Self {
        let actor_ref = CommandServiceActor::spawn(CommandServiceActor {
            conn: conn.clone(),
            entities: LruCache::new(NonZeroUsize::new(20_000).unwrap()),
        });

        CommandService { actor_ref, conn }
    }

    /// Returns a reference to the inner event store.
    #[inline]
    pub fn conn(&mut self) -> &mut MultiplexedConnection {
        &mut self.conn
    }

    /// Starts a transaction.
    pub async fn transaction<C, A>(
        &mut self,
        mut f: impl FnMut(Transaction<'_>) -> BoxFuture<'_, anyhow::Result<TransactionOutcome<C, A>>>,
    ) -> anyhow::Result<TransactionOutcome<C, A>> {
        let mut entities = HashMap::new();
        let mut appends = Vec::new();
        let mut attempt = 0;
        loop {
            let mut partition_key = None;
            let tx = Transaction::new(self, &mut partition_key, &mut entities, &mut appends);
            let res = f(tx).await;
            match res {
                Ok(TransactionOutcome::Commit(val)) => {
                    if appends.is_empty() {
                        Transaction::new(self, &mut partition_key, &mut entities, &mut appends)
                            .committed();
                        return Ok(TransactionOutcome::Commit(val));
                    }
                    let final_partition_key =
                        partition_key.expect("partition key should be set if we have appends");

                    let current_appends = mem::take(&mut appends);

                    // Attempt the transaction
                    match self
                        .conn
                        .emappend(final_partition_key, &current_appends)
                        .await
                    {
                        Ok(_) => {
                            Transaction::new(self, &mut partition_key, &mut entities, &mut appends)
                                .committed();
                            return Ok(TransactionOutcome::Commit(val));
                        }
                        Err(err) => match SierraError::from(err) {
                            SierraError::Protocol {
                                code: ErrorCode::WrongVer,
                                message,
                            } => {
                                let ParsedStream {
                                    partition_key: err_partition_key,
                                    stream_id,
                                    current,
                                    expected,
                                } = parse_stream_version_string(message.as_ref().unwrap()).unwrap();

                                debug!(partition_key = %err_partition_key, %stream_id, %current, %expected, "write conflict");
                                if attempt >= 3 {
                                    Transaction::new(
                                        self,
                                        &mut partition_key,
                                        &mut entities,
                                        &mut appends,
                                    )
                                    .abort();
                                    return Err(anyhow!("too many conflict retries"));
                                }

                                attempt += 1;
                                Transaction::new(
                                    self,
                                    &mut partition_key,
                                    &mut entities,
                                    &mut appends,
                                )
                                .reset();
                            }
                            err => {
                                Transaction::new(
                                    self,
                                    &mut partition_key,
                                    &mut entities,
                                    &mut appends,
                                )
                                .abort();
                                return Err(err.into());
                            }
                        },
                    }
                }
                Ok(TransactionOutcome::Abort(val)) => {
                    Transaction::new(self, &mut partition_key, &mut entities, &mut appends).abort();
                    return Ok(TransactionOutcome::Abort(val));
                }
                Err(err) => {
                    Transaction::new(self, &mut partition_key, &mut entities, &mut appends).abort();
                    return Err(err);
                }
            }
        }
    }
}

/// The command service routes commands to spawned entity actors per stream id.
struct CommandServiceActor {
    conn: MultiplexedConnection,
    entities: LruCache<StreamId, (ActorId, Box<dyn any::Any + Send + Sync + 'static>)>,
}

impl Actor for CommandServiceActor {
    type Args = Self;
    type Error = anyhow::Error;

    fn name() -> &'static str {
        "CommandServiceActor"
    }

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(args)
    }

    async fn on_panic(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
        error!("command service actor errored: {err}");
        Ok(ControlFlow::Continue(())) // Restart
    }

    async fn on_link_died(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        id: ActorId,
        _reason: ActorStopReason,
    ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
        let keys_to_remove: Vec<_> = self
            .entities
            .iter()
            .filter_map(|(key, (existing_id, _))| {
                if *existing_id == id {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        for key in keys_to_remove {
            self.entities.pop(&key);
        }

        Ok(ControlFlow::Continue(()))
    }
}

pub trait ExecuteExt<'a, E, C, T>
where
    E: Entity + Command<C>,
    C: CommandName,
{
    type Transaction;

    fn execute(cmd_service: T, id: E::ID, command: C) -> Execute<'a, E, C, Self::Transaction>;
}

impl<'a, E, C> ExecuteExt<'a, E, C, &'a CommandService> for E
where
    E: Entity + Command<C> + Apply + Default + Sync,
    E::Event: Clone,
    E::Error: fmt::Debug + Send + Sync,
    C: CommandName + Clone + Send + 'static,
{
    type Transaction = ();

    fn execute(cmd_service: &'a CommandService, id: E::ID, command: C) -> Execute<'a, E, C, ()> {
        Execute::new(cmd_service, id, command, ())
    }
}

impl<'a, 'b, E, C> ExecuteExt<'a, E, C, &'a mut Transaction<'b>> for E
where
    E: Entity + Command<C> + Apply + Default + Sync,
    E::Event: Clone,
    E::Error: fmt::Debug + Send + Sync,
    C: CommandName + Clone + Send + 'static,
    'b: 'a,
{
    type Transaction = &'a mut Transaction<'b>;

    fn execute(
        tx: &'a mut Transaction<'b>,
        id: E::ID,
        command: C,
    ) -> Execute<'a, E, C, &'a mut Transaction<'b>> {
        Execute::new(tx.cmd_service, id, command, tx)
    }
}

#[derive(Debug)]
pub enum ExecuteResult<E: Entity + Apply> {
    /// The command was executed with the resulting events.
    Executed(Vec<AppendedEvent<E::Event>>),
    /// The command was executed and pending commit.
    PendingTransaction {
        entity_actor_ref: ActorRef<EntityTransactionActor<E>>,
        events: Vec<E::Event>,
        expected_version: ExpectedVersion,
    },
    /// The command was executed, but no new events due to idempotency
    Idempotent { current_version: CurrentVersion },
}

impl<E: Entity + Apply> ExecuteResult<E> {
    pub fn is_idempotent(&self) -> bool {
        matches!(self, ExecuteResult::Idempotent { .. })
    }
}

pub enum ExecuteResultIter<E: Entity + Apply> {
    Executed(IntoIter<AppendedEvent<E::Event>>),
    PendingTransaction(IntoIter<E::Event>),
    Idempotent,
}

impl<E: Entity + Apply> Iterator for ExecuteResultIter<E> {
    type Item = E::Event;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ExecuteResultIter::Executed(events) => events.next().map(|append| append.event),
            ExecuteResultIter::PendingTransaction(events) => events.next(),
            ExecuteResultIter::Idempotent => None,
        }
    }
}

impl<E> ExecuteResult<E>
where
    E: Entity + Apply,
{
    pub fn len(&self) -> usize {
        match self {
            ExecuteResult::Executed(events) => events.len(),
            ExecuteResult::PendingTransaction { events, .. } => events.len(),
            ExecuteResult::Idempotent { .. } => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            ExecuteResult::Executed(events) => events.is_empty(),
            ExecuteResult::PendingTransaction { events, .. } => events.is_empty(),
            ExecuteResult::Idempotent { .. } => true,
        }
    }
}

impl<E> IntoIterator for ExecuteResult<E>
where
    E: Entity + Apply,
{
    type Item = E::Event;
    type IntoIter = ExecuteResultIter<E>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            ExecuteResult::Executed(events) => ExecuteResultIter::Executed(events.into_iter()),
            ExecuteResult::PendingTransaction { events, .. } => {
                ExecuteResultIter::PendingTransaction(events.into_iter())
            }
            ExecuteResult::Idempotent { .. } => ExecuteResultIter::Idempotent,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AppendedEvent<E> {
    pub event: E,
    pub event_id: Uuid,
    pub partition_id: u16,
    pub partition_sequence: u64,
    pub stream_version: u64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug)]
pub struct Execute<'a, E, C, T>
where
    E: Entity,
{
    cmd_service: &'a CommandService,
    id: E::ID,
    command: C,
    partition_key: Option<Uuid>,
    metadata: Metadata<E::Metadata>,
    expected_version: ExpectedVersion,
    time: DateTime<Utc>,
    dry_run: bool,
    executed_at: Instant,
    transaction: T,
    phantom: PhantomData<E>,
}

impl<'a, E, C, T> Execute<'a, E, C, T>
where
    E: Entity + Command<C>,
    C: CommandName,
{
    fn new(cmd_service: &'a CommandService, id: E::ID, command: C, tx: T) -> Self {
        Execute {
            cmd_service,
            id,
            command,
            partition_key: None,
            metadata: Metadata {
                causation_command: Some(Cow::Borrowed(C::command_name())),
                causation_event: None,
                data: None,
            },
            expected_version: ExpectedVersion::Any,
            time: Utc::now(),
            dry_run: false,
            executed_at: Instant::now(),
            transaction: tx,
            phantom: PhantomData,
        }
    }

    pub fn partition_key(mut self, partition_key: Uuid) -> Self {
        self.partition_key = Some(partition_key);
        self
    }

    pub fn partition_key_from_stream_id(mut self, stream_id: &StreamId) -> Self {
        self.partition_key = Some(stream_partition_key(stream_id));
        self
    }

    pub fn caused_by(mut self, causation_metadata: EventCausation) -> Self {
        self.metadata.causation_event = Some(EventCausation {
            event_id: causation_metadata.event_id,
            stream_id: causation_metadata.stream_id,
            stream_version: causation_metadata.stream_version,
        });
        self
    }

    pub fn caused_by_event<F, N>(self, event: &Event<F, N>) -> Self {
        self.caused_by(EventCausation {
            event_id: event.id,
            stream_id: event.stream_id.clone(),
            stream_version: event.stream_version,
        })
    }

    pub fn metadata(mut self, metadata: E::Metadata) -> Self {
        self.metadata = self.metadata.with_data(metadata);
        self
    }

    pub fn expected_version(mut self, expected: ExpectedVersion) -> Self {
        self.expected_version = expected;
        self
    }

    pub fn current_time(mut self, time: DateTime<Utc>) -> Self {
        self.time = time;
        self
    }

    /// Prevents any events from being appended to the event store.
    ///
    /// The returned `AppendedEvent`s will not have correct values for event_id, partition_id, partition_sequence.
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }
}

impl<'a, E, C> IntoFuture for Execute<'a, E, C, ()>
where
    E: Entity + Command<C> + Apply + Clone,
    E::ID: fmt::Debug,
    E::Metadata: fmt::Debug,
    C: CommandName + fmt::Debug + Clone + Send + Sync + 'static,
{
    type Output = Result<ExecuteResult<E>, ExecuteError<E::Error>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            self.cmd_service
                .actor_ref
                .ask(ExecuteMsg {
                    id: self.id,
                    command: self.command,
                    partition_key: self.partition_key,
                    metadata: self.metadata.clone(),
                    expected_version: self.expected_version,
                    time: self.time,
                    dry_run: self.dry_run,
                    executed_at: self.executed_at,
                    phantom: PhantomData::<E>,
                })
                .send()
                .await
                .map_err(|err| match err {
                    SendError::ActorNotRunning(_) => ExecuteError::CommandServiceNotRunning,
                    SendError::ActorStopped => ExecuteError::CommandServiceStopped,
                    SendError::MailboxFull(_) => {
                        unreachable!("messages aren't sent to the command service with try_")
                    }
                    SendError::HandlerError(err) => err,
                    SendError::Timeout(_) => {
                        unreachable!("messages aren't sent to the command service with timeouts")
                    }
                })
        }
        .boxed()
    }
}

impl<'a, 'b, E, C> IntoFuture for Execute<'a, E, C, &'a mut Transaction<'b>>
where
    E: Entity + Command<C> + Apply + Clone,
    E::ID: fmt::Debug + Clone,
    E::Metadata: fmt::Debug,
    C: CommandName + fmt::Debug + Clone + Send + Sync + 'static,
{
    type Output = Result<ExecuteResult<E>, ExecuteError<E::Error>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            let stream_id = StreamId::new_from_parts(E::category(), &self.id);
            let partition_key = self.partition_key.unwrap_or_else(|| {
                stream_partition_key(&StreamId::new_from_parts(E::category(), &self.id))
            });

            match self.transaction.partition_key {
                Some(tx_partition_key) if &partition_key != tx_partition_key => {
                    return Err(ExecuteError::PartitionKeyMismatch {
                        entity: E::category(),
                        existing: *tx_partition_key,
                        new: partition_key,
                    })
                }
                Some(_) => {}
                None => {
                    *self.transaction.partition_key = Some(partition_key);
                }
            }

            let entity_actor_ref = if !self.transaction.is_registered(&stream_id) {
                // Register this entity so it knows its in a transaction
                let entity_actor_ref = self
                    .cmd_service
                    .actor_ref
                    .ask(BeginTransaction {
                        partition_key,
                        id: self.id.clone(),
                        phantom: PhantomData::<E>,
                    })
                    .send()
                    .await
                    .map_err(|err| match err {
                        SendError::ActorNotRunning(_) => ExecuteError::CommandServiceNotRunning,
                        SendError::ActorStopped => ExecuteError::CommandServiceStopped,
                        SendError::MailboxFull(_) => {
                            unreachable!("messages aren't sent to the command service with try_")
                        }
                        SendError::HandlerError(_) => unreachable!(),
                        SendError::Timeout(_) => {
                            unreachable!(
                                "messages aren't sent to the command service with timeouts"
                            )
                        }
                    })?;
                self.transaction
                    .register_entity(stream_id.clone(), Box::new(entity_actor_ref.clone()));
                entity_actor_ref
            } else {
                self.transaction.lookup_entity(&stream_id).unwrap()
            };

            let res = entity_actor_ref
                .ask(entity_actor::Execute {
                    id: self.id,
                    command: self.command,
                    partition_key,
                    metadata: self.metadata.clone(),
                    expected_version: self.expected_version,
                    time: self.time,
                    executed_at: self.executed_at,
                    dry_run: self.dry_run,
                })
                .send()
                .await
                .map_err(|err| match err {
                    SendError::ActorNotRunning(_) => ExecuteError::CommandServiceNotRunning,
                    SendError::ActorStopped => ExecuteError::CommandServiceStopped,
                    SendError::MailboxFull(_) => {
                        unreachable!("messages aren't sent to the command service with try_")
                    }
                    SendError::HandlerError(err) => err,
                    SendError::Timeout(_) => {
                        unreachable!("messages aren't sent to the command service with timeouts")
                    }
                })?;
            match res {
                ExecuteResult::Executed(_) => panic!("expected pending transaction response"),
                ExecuteResult::PendingTransaction {
                    entity_actor_ref,
                    events,
                    expected_version,
                } => {
                    let mut metadata_buf = Vec::new();
                    if !self.metadata.is_empty() {
                        ciborium::into_writer(&self.metadata, &mut metadata_buf).map_err(
                            |err| {
                                ExecuteError::SerializeMetadata(ciborium::ser::Error::Value(
                                    err.to_string(),
                                ))
                            },
                        )?;
                    }

                    for event in &events {
                        let mut payload = Vec::new();
                        ciborium::into_writer(&event, &mut payload)
                            .map_err(ExecuteError::SerializeEvent)?;
                        self.transaction.append(
                            EMAppendEvent::new(stream_id.to_string(), event.event_type())
                                .payload(payload)
                                .metadata(metadata_buf.clone())
                                .expected_version(self.expected_version)
                                .timestamp(self.time.into()),
                        );
                    }

                    Ok(ExecuteResult::PendingTransaction {
                        entity_actor_ref,
                        events,
                        expected_version,
                    })
                }
                ExecuteResult::Idempotent { current_version } => {
                    Ok(ExecuteResult::Idempotent { current_version })
                }
            }
        }
        .boxed()
    }
}

impl CommandServiceActor {
    async fn get_or_start_entity_actor<E>(
        &mut self,
        cmd_service_actor_ref: &ActorRef<Self>,
        partition_key: Uuid,
        stream_id: StreamId,
    ) -> ActorRef<EntityActor<E>>
    where
        E: Entity + Apply,
    {
        match self.entities.get(&stream_id) {
            Some((_, actor_ref)) => actor_ref
                .downcast_ref::<ActorRef<EntityActor<E>>>()
                .cloned()
                .unwrap(),
            None => {
                let entity_ref = EntityActor::spawn_link(
                    cmd_service_actor_ref,
                    EntityActor::new(
                        self.conn.clone(),
                        partition_key,
                        stream_id.clone(),
                        E::default(),
                    ),
                )
                .await;

                self.entities
                    .push(stream_id, (entity_ref.id(), Box::new(entity_ref.clone())));

                entity_ref
            }
        }
    }
}

#[derive(Debug)]
struct ExecuteMsg<E, C>
where
    E: Entity,
{
    id: E::ID,
    command: C,
    partition_key: Option<Uuid>,
    metadata: Metadata<E::Metadata>,
    expected_version: ExpectedVersion,
    time: DateTime<Utc>,
    dry_run: bool,
    executed_at: Instant,
    phantom: PhantomData<E>,
}

impl<E, C> Message<ExecuteMsg<E, C>> for CommandServiceActor
where
    E: Command<C> + Apply + Clone,
    E::ID: fmt::Debug,
    E::Metadata: fmt::Debug,
    C: CommandName + fmt::Debug + Clone + Send + Sync + 'static,
{
    type Reply = DelegatedReply<Result<ExecuteResult<E>, ExecuteError<E::Error>>>;

    async fn handle(
        &mut self,
        msg: ExecuteMsg<E, C>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let stream_id = StreamId::new_from_parts(E::category(), &msg.id);
        let partition_key = msg.partition_key.unwrap_or_else(|| {
            stream_partition_key(&StreamId::new_from_parts(E::category(), &msg.id))
        });
        let entity_ref = self
            .get_or_start_entity_actor::<E>(ctx.actor_ref(), partition_key, stream_id)
            .await;

        let (delegated_reply, reply_sender) = ctx.reply_sender();
        let exec = entity_actor::Execute {
            id: msg.id,
            command: msg.command,
            partition_key,
            expected_version: msg.expected_version,
            metadata: msg.metadata,
            time: msg.time,
            dry_run: msg.dry_run,
            executed_at: msg.executed_at,
        };
        match reply_sender {
            Some(tx) => {
                let _ = entity_ref.ask(exec).forward(tx).await;
            }
            None => {
                let _ = entity_ref.tell(exec).send().await;
            }
        }

        delegated_reply
    }
}

struct BeginTransaction<E>
where
    E: Entity,
{
    partition_key: Uuid,
    id: E::ID,
    phantom: PhantomData<E>,
}

impl<E> Message<BeginTransaction<E>> for CommandServiceActor
where
    E: Entity + Apply + Clone,
{
    type Reply = DelegatedReply<ActorRef<EntityTransactionActor<E>>>;

    async fn handle(
        &mut self,
        msg: BeginTransaction<E>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let stream_id = StreamId::new_from_parts(E::category(), &msg.id);
        let entity_ref = self
            .get_or_start_entity_actor::<E>(ctx.actor_ref(), msg.partition_key, stream_id)
            .await;

        let (delegated_reply, reply_sender) = ctx.reply_sender();
        match reply_sender {
            Some(tx) => {
                let _ = entity_ref
                    .ask(transaction::BeginTransaction)
                    .forward(tx)
                    .await;
            }
            None => {
                let _ = entity_ref.tell(transaction::BeginTransaction).send().await;
            }
        }

        delegated_reply
    }
}
