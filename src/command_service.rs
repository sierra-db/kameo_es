use std::{
    any, collections::HashMap, fmt, future::IntoFuture, marker::PhantomData, ops::ControlFlow,
    time::Instant, vec::IntoIter,
};

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use ciborium::Value;
use eventus::{
    server::{eventstore::event_store_client::EventStoreClient, ClientAuthInterceptor},
    ExpectedVersion,
};
use futures::{future::BoxFuture, FutureExt};
use kameo::{
    actor::{ActorId, ActorRef, WeakActorRef},
    error::{ActorStopReason, Infallible, PanicError, SendError},
    message::{Context, Message},
    reply::DelegatedReply,
    Actor,
};
use tonic::{service::interceptor::InterceptedService, transport::Channel};
use tracing::{debug, error};
use uuid::Uuid;

use crate::{
    entity_actor::{self, EntityActor},
    error::ExecuteError,
    event_store::{AppendEvents, AppendEventsError, EventStore},
    transaction::{self, Transaction, TransactionOutcome},
    Apply, CausationMetadata, Command, Entity, Event, EventType, GenericValue, Metadata, StreamID,
};

#[derive(Clone, Debug)]
pub struct CommandService {
    actor_ref: ActorRef<CommandServiceActor>,
    event_store: EventStore,
}

impl CommandService {
    /// Creates a new command service using an event store client connection and default worker count.
    #[inline]
    pub fn new(
        client: EventStoreClient<InterceptedService<Channel, ClientAuthInterceptor>>,
    ) -> Self {
        let event_store = EventStore::new(client);
        let actor_ref = CommandServiceActor::spawn(CommandServiceActor {
            event_store: event_store.clone(),
            entities: HashMap::new(),
        });

        CommandService {
            actor_ref,
            event_store,
        }
    }

    /// Returns a reference to the inner event store.
    #[inline]
    pub fn event_store(&mut self) -> &mut EventStore {
        &mut self.event_store
    }

    /// Starts a transaction.
    pub async fn transaction<C, A>(
        &mut self,
        mut f: impl FnMut(Transaction<'_>) -> BoxFuture<'_, anyhow::Result<TransactionOutcome<C, A>>>,
    ) -> anyhow::Result<TransactionOutcome<C, A>> {
        let id = Transaction::get_id();
        let mut entities = HashMap::new();
        let mut appends = Vec::new();
        let mut attempt = 0;
        loop {
            let tx = Transaction::new(id, self, &mut entities, &mut appends);
            let res = f(tx).await;
            match res {
                Ok(TransactionOutcome::Commit(val)) => {
                    let current_appends: Vec<_> = appends
                        .drain(..)
                        .filter(|append| !append.events.is_empty())
                        .collect();
                    if current_appends.is_empty() {
                        Transaction::new(id, self, &mut entities, &mut appends).committed();
                        return Ok(TransactionOutcome::Commit(val));
                    }

                    // Attempt the transaction
                    let res = self.event_store.append_to_streams(current_appends).await;
                    match res {
                        Ok(_) => {
                            Transaction::new(id, self, &mut entities, &mut appends).committed();
                            return Ok(TransactionOutcome::Commit(val));
                        }
                        Err(AppendEventsError::IncorrectExpectedVersion {
                            stream_id,
                            current,
                            expected,
                            ..
                        }) => {
                            debug!(%stream_id, %current, %expected, "write conflict");
                            if attempt >= 3 {
                                Transaction::new(id, self, &mut entities, &mut appends).abort();
                                return Err(anyhow!("too many conflict retries"));
                            }

                            attempt += 1;
                            Transaction::new(id, self, &mut entities, &mut appends).reset();
                        }
                        Err(err) => {
                            Transaction::new(id, self, &mut entities, &mut appends).abort();
                            return Err(err.into());
                        }
                    }
                }
                Ok(TransactionOutcome::Abort(val)) => {
                    Transaction::new(id, self, &mut entities, &mut appends).abort();
                    return Ok(TransactionOutcome::Abort(val));
                }
                Err(err) => {
                    Transaction::new(id, self, &mut entities, &mut appends).abort();
                    return Err(err.into());
                }
            }
        }
    }
}

/// The command service routes commands to spawned entity actors per stream id.
struct CommandServiceActor {
    event_store: EventStore,
    entities: HashMap<StreamID, (ActorId, Box<dyn any::Any + Send + Sync + 'static>)>,
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

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        error!("command service actor stopped: {reason:?}");
        Ok(())
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
        self.entities
            .retain(|_, (existing_id, _)| *existing_id != id);
        Ok(ControlFlow::Continue(()))
    }
}

pub trait ExecuteExt<'a, E, C, T>
where
    E: Entity + Command<C>,
{
    type Transaction;

    fn execute(cmd_service: T, id: E::ID, command: C) -> Execute<'a, E, C, Self::Transaction>;
}

impl<'a, E, C> ExecuteExt<'a, E, C, &'a CommandService> for E
where
    E: Entity + Command<C> + Apply + Default + Sync,
    E::Event: Clone,
    E::Error: fmt::Debug + Send + Sync,
    C: Clone + Send + 'static,
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
    C: Clone + Send + 'static,
    'b: 'a,
{
    type Transaction = &'a mut Transaction<'b>;

    fn execute(
        tx: &'a mut Transaction<'b>,
        id: E::ID,
        command: C,
    ) -> Execute<'a, E, C, &'a mut Transaction<'b>> {
        Execute::new(&tx.cmd_service, id, command, tx)
    }
}

#[derive(Debug)]
pub enum ExecuteResult<E: Entity + Apply + Clone> {
    /// The command was executed with the resulting events.
    Executed(Vec<AppendedEvent<E::Event>>),
    /// The command was executed and pending commit.
    PendingTransaction {
        entity_actor_ref: ActorRef<EntityActor<E>>,
        events: Vec<E::Event>,
        expected_version: ExpectedVersion,
        correlation_id: Uuid,
    },
    /// The command was executed, but no new events due to idempotency
    Idempotent,
}

pub enum ExecuteResultIter<E: Entity + Apply + Clone> {
    Executed(IntoIter<AppendedEvent<E::Event>>),
    PendingTransaction(IntoIter<E::Event>),
    Idempotent,
}

impl<E: Entity + Apply + Clone> Iterator for ExecuteResultIter<E> {
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
    E: Entity + Apply + Clone,
{
    pub fn len(&self) -> usize {
        match self {
            ExecuteResult::Executed(events) => events.len(),
            ExecuteResult::PendingTransaction { events, .. } => events.len(),
            ExecuteResult::Idempotent => 0,
        }
    }

    pub fn into_iter(self) -> ExecuteResultIter<E> {
        match self {
            ExecuteResult::Executed(events) => ExecuteResultIter::Executed(events.into_iter()),
            ExecuteResult::PendingTransaction { events, .. } => {
                ExecuteResultIter::PendingTransaction(events.into_iter())
            }
            ExecuteResult::Idempotent => ExecuteResultIter::Idempotent,
        }
    }
}

#[derive(Debug)]
pub struct AppendedEvent<E> {
    pub event: E,
    pub event_id: u64,
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
    metadata: Metadata<E::Metadata>,
    expected_version: ExpectedVersion,
    time: DateTime<Utc>,
    executed_at: Instant,
    transaction: T,
    phantom: PhantomData<E>,
}

impl<'a, E, C, T> Execute<'a, E, C, T>
where
    E: Entity,
{
    fn new(cmd_service: &'a CommandService, id: E::ID, command: C, tx: T) -> Self {
        Execute {
            cmd_service,
            id,
            command,
            metadata: Metadata::default(),
            expected_version: ExpectedVersion::Any,
            time: Utc::now(),
            executed_at: Instant::now(),
            transaction: tx,
            phantom: PhantomData,
        }
    }

    pub fn caused_by(mut self, causation_metadata: CausationMetadata) -> Self {
        self.metadata.causation = Some(causation_metadata);
        self
    }

    pub fn caused_by_event<F, N>(self, event: &Event<F, N>) -> Self {
        self.caused_by(CausationMetadata {
            event_id: event.id,
            stream_id: event.stream_id.clone(),
            stream_version: event.stream_version,
            correlation_id: event.metadata.correlation_id,
        })
    }

    pub fn correlation_id(mut self, id: Uuid) -> Self {
        self.metadata.correlation_id = id;
        self
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
}

impl<'a, E, C> IntoFuture for Execute<'a, E, C, ()>
where
    E: Entity + Command<C> + Apply + Clone,
    E::ID: fmt::Debug,
    E::Metadata: fmt::Debug,
    C: fmt::Debug + Clone + Send + Sync + 'static,
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
                    metadata: self.metadata.clone(),
                    expected_version: self.expected_version,
                    time: self.time,
                    executed_at: self.executed_at,
                    tx_id: None,
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
    C: fmt::Debug + Clone + Send + Sync + 'static,
{
    type Output = Result<ExecuteResult<E>, ExecuteError<E::Error>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        async move {
            let stream_id = StreamID::new_from_parts(E::name(), &self.id);
            if !self.transaction.is_registered(&stream_id) {
                // Register this entity so it knows its in a transaction
                let entity_actor_ref = self
                    .cmd_service
                    .actor_ref
                    .ask(BeginTransaction {
                        id: self.id.clone(),
                        tx_id: self.transaction.id(),
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
                    .register_entity(stream_id.clone(), Box::new(entity_actor_ref));
            };

            let res = self
                .cmd_service
                .actor_ref
                .ask(ExecuteMsg {
                    id: self.id,
                    command: self.command,
                    metadata: self.metadata.clone(),
                    expected_version: self.expected_version,
                    time: self.time,
                    executed_at: self.executed_at,
                    tx_id: Some(self.transaction.id()),
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
                })?;
            match res {
                ExecuteResult::Executed(_) => panic!("expected pending transaction response"),
                ExecuteResult::PendingTransaction {
                    entity_actor_ref,
                    events,
                    expected_version,
                    correlation_id,
                } => {
                    self.metadata.correlation_id = correlation_id;
                    let metadata =
                        GenericValue(Value::serialized(&self.metadata).map_err(|err| {
                            ExecuteError::SerializeMetadata(ciborium::ser::Error::Value(
                                err.to_string(),
                            ))
                        })?);

                    let generic_events = events
                        .iter()
                        .map(|event| {
                            let event_type = event.event_type();
                            let event = GenericValue(Value::serialized(event)?);
                            Ok((event_type, event))
                        })
                        .collect::<anyhow::Result<_>>()
                        .map_err(|err| {
                            ExecuteError::SerializeMetadata(ciborium::ser::Error::Value(
                                err.to_string(),
                            ))
                        })?;

                    self.transaction.append(AppendEvents {
                        stream_id,
                        events: generic_events,
                        expected_version,
                        metadata,
                        timestamp: self.time,
                    });

                    Ok(ExecuteResult::PendingTransaction {
                        entity_actor_ref,
                        events,
                        expected_version,
                        correlation_id,
                    })
                }
                ExecuteResult::Idempotent => Ok(ExecuteResult::Idempotent),
            }
        }
        .boxed()
    }
}

impl CommandServiceActor {
    async fn get_or_start_entity_actor<E>(
        &mut self,
        cmd_service_actor_ref: &ActorRef<Self>,
        stream_id: StreamID,
    ) -> ActorRef<EntityActor<E>>
    where
        E: Entity + Apply + Clone,
    {
        match self.entities.get(&stream_id) {
            Some((_, actor_ref)) => actor_ref
                .downcast_ref::<ActorRef<EntityActor<E>>>()
                .cloned()
                .unwrap(),
            None => {
                let entity_ref = EntityActor::spawn_link(
                    &cmd_service_actor_ref,
                    EntityActor::new(E::default(), stream_id.clone(), self.event_store.clone()),
                )
                .await;

                self.entities
                    .insert(stream_id, (entity_ref.id(), Box::new(entity_ref.clone())));

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
    metadata: Metadata<E::Metadata>,
    expected_version: ExpectedVersion,
    time: DateTime<Utc>,
    executed_at: Instant,
    tx_id: Option<usize>,
    phantom: PhantomData<E>,
}

impl<E, C> Message<ExecuteMsg<E, C>> for CommandServiceActor
where
    E: Command<C> + Apply + Clone,
    E::ID: fmt::Debug,
    E::Metadata: fmt::Debug,
    C: fmt::Debug + Clone + Send + Sync + 'static,
{
    type Reply = DelegatedReply<Result<ExecuteResult<E>, ExecuteError<E::Error>>>;

    async fn handle(
        &mut self,
        msg: ExecuteMsg<E, C>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let stream_id = StreamID::new_from_parts(E::name(), &msg.id);
        let entity_ref = self
            .get_or_start_entity_actor::<E>(&ctx.actor_ref(), stream_id)
            .await;

        let (delegated_reply, reply_sender) = ctx.reply_sender();
        let exec = entity_actor::Execute {
            id: msg.id,
            command: msg.command,
            expected_version: msg.expected_version,
            metadata: msg.metadata,
            time: msg.time,
            executed_at: msg.executed_at,
            tx_id: msg.tx_id,
            is_buffered: false,
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
    id: E::ID,
    tx_id: usize,
    phantom: PhantomData<E>,
}

impl<E> Message<BeginTransaction<E>> for CommandServiceActor
where
    E: Entity + Apply + Clone,
{
    type Reply = DelegatedReply<Result<ActorRef<EntityActor<E>>, Infallible>>;

    async fn handle(
        &mut self,
        msg: BeginTransaction<E>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let stream_id = StreamID::new_from_parts(E::name(), &msg.id);
        let entity_ref = self
            .get_or_start_entity_actor::<E>(&ctx.actor_ref(), stream_id)
            .await;

        let (delegated_reply, reply_sender) = ctx.reply_sender();
        let begin = transaction::BeginTransaction {
            tx_id: msg.tx_id,
            is_buffered: false,
        };
        match reply_sender {
            Some(tx) => {
                let _ = entity_ref.ask(begin).forward(tx).await;
            }
            None => {
                let _ = entity_ref.tell(begin).send().await;
            }
        }

        delegated_reply
    }
}
