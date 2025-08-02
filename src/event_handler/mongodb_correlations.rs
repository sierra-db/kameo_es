use std::{collections::HashMap, fmt, marker::PhantomData, ops::ControlFlow, sync::Arc};

use kameo::{
    actor::{ActorID, ActorRef, WeakActorRef},
    error::{ActorStopReason, PanicError},
    message::{Context, Message},
    reply::DelegatedReply,
    Actor,
};
use mongodb::{
    bson::doc,
    error::{ErrorKind, WriteError, WriteFailure},
    Client, Collection,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tracing::{debug, error};
use uuid::Uuid;

use crate::Event;

use super::{
    mongodb::{MongoEventProcessorError, Session},
    CompositeEventHandler, EventHandler, EventHandlerError, EventProcessor,
};

pub struct Coordinator<E: 'static, H: Send + 'static> {
    client: Client,
    checkpoints_collection: Collection<Checkpoint>,
    handler: H,
    correlations: HashMap<Uuid, ActorRef<CorrelationWorker<E, H>>>,
    semaphore: Arc<Semaphore>,
    flush_interval: u64,
}

impl<E: 'static, H: Send + 'static> Coordinator<E, H> {
    pub fn new(client: Client, checkpoints_collection: Collection<Checkpoint>, handler: H) -> Self {
        Coordinator {
            client,
            checkpoints_collection,
            handler,
            correlations: HashMap::new(),
            semaphore: Arc::new(Semaphore::new(50)),
            flush_interval: 100,
        }
    }

    /// The number of ignored events to process before performing a flush.
    pub fn flush_interval(mut self, num_ignored_events: u64) -> Self {
        self.flush_interval = num_ignored_events;
        self
    }

    /// Gets a reference to the inner handler.
    pub fn handler(&mut self) -> &mut H {
        &mut self.handler
    }
}

impl<E, H> EventProcessor<E, H> for ActorRef<Coordinator<E, H>>
where
    E: 'static,
    H: EventHandler<Session>
        + CompositeEventHandler<E, Session, MongoEventProcessorError>
        + Clone
        + Send
        + 'static,
    H::Error: fmt::Debug + Unpin + Sync,
{
    type Context = Session;
    type Error = MongoEventProcessorError;

    async fn start_from(&self) -> Result<u64, Self::Error> {
        Ok(0)
        // Ok(self.last_handled_event.map(|n| n + 1).unwrap_or(0))
    }

    async fn process_event(
        &mut self,
        event: Event,
    ) -> Result<(), EventHandlerError<Self::Error, <H as EventHandler<Self::Context>>::Error>> {
        self.tell(HandleEvent(event)).send().await.unwrap();
        Ok(())
    }
}

impl<E: 'static, H: Send + 'static> Actor for Coordinator<E, H> {
    type Args = Self;
    type Error = anyhow::Error;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(args)
    }

    async fn on_link_died(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        id: ActorID,
        _reason: ActorStopReason,
    ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
        self.correlations.retain(|_, worker| worker.id() != id);
        Ok(ControlFlow::Continue(()))
    }
}

struct HandleEvent(Event);

impl<E, H> Message<HandleEvent> for Coordinator<E, H>
where
    E: 'static,
    H: EventHandler<Session>
        + CompositeEventHandler<E, Session, MongoEventProcessorError>
        + Clone
        + Send
        + 'static,
    H::Error: fmt::Debug + Unpin + Sync,
{
    type Reply = DelegatedReply<
        Result<
            (),
            EventHandlerError<MongoEventProcessorError, <H as EventHandler<Session>>::Error>,
        >,
    >;

    async fn handle(
        &mut self,
        HandleEvent(event): HandleEvent,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let correlation_worker_ref = self
            .correlations
            .entry(event.metadata.correlation_id)
            .or_insert_with(|| {
                let worker_ref = CorrelationWorker::spawn(CorrelationWorker {
                    correlation_id: event.metadata.correlation_id,
                    client: self.client.clone(),
                    checkpoints_collection: self.checkpoints_collection.clone(),
                    handler: self.handler.clone(),
                    semaphore: self.semaphore.clone(),
                    last_handled_event: None,
                    flush_interval: self.flush_interval,
                    phantom: PhantomData,
                });
                // worker_ref
                //     .link_child(&ctx.actor_ref())
                //     .now_or_never()
                //     .unwrap();

                worker_ref
            });

        let (delegated_reply, reply_sender) = ctx.reply_sender();
        match reply_sender {
            Some(reply_sender) => {
                correlation_worker_ref
                    .ask(HandleEvent(event))
                    .forward(reply_sender)
                    .await
                    .unwrap();
            }
            None => {
                correlation_worker_ref
                    .tell(HandleEvent(event))
                    .send()
                    .await
                    .unwrap();
            }
        }

        delegated_reply
    }
}

struct CorrelationWorker<E, H> {
    correlation_id: Uuid,
    client: Client,
    checkpoints_collection: Collection<Checkpoint>,
    handler: H,
    semaphore: Arc<Semaphore>,
    last_handled_event: Option<u64>,
    flush_interval: u64,
    phantom: PhantomData<fn() -> E>,
}

impl<E: 'static, H: Send + 'static> Actor for CorrelationWorker<E, H> {
    type Args = Self;
    type Error = anyhow::Error;

    async fn on_start(
        mut state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        let checkpoint = state
            .checkpoints_collection
            .find_one(doc! { "_id": bson::Uuid::from(state.correlation_id) })
            .await?;
        state.last_handled_event = checkpoint.map(|cp| cp.last_event_id);

        Ok(state)
    }

    async fn on_panic(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
        error!("{err}");
        Ok(ControlFlow::Continue(()))
    }
}

impl<E, H> Message<HandleEvent> for CorrelationWorker<E, H>
where
    E: 'static,
    H: EventHandler<Session>
        + CompositeEventHandler<E, Session, MongoEventProcessorError>
        + Send
        + 'static,
    H::Error: fmt::Debug + Sync,
{
    type Reply = Result<
        (),
        EventHandlerError<MongoEventProcessorError, <H as EventHandler<Session>>::Error>,
    >;

    async fn handle(
        &mut self,
        HandleEvent(event): HandleEvent,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let _permit = self.semaphore.acquire().await.unwrap();

        let mut session = self.client.start_session().await?;
        session.start_transaction().await?;
        let mut session = Session::new(session);

        let event_id = event.id;
        let res = self.handler.composite_handle(&mut session, event).await;
        if res.is_err() {
            let abort_res = session.abort_transaction().await;
            res?;
            abort_res?;
        }

        let Session { mut session, dirty } = session;

        let events_since_last_flush = self
            .last_handled_event
            .map(|last| event_id - last)
            .unwrap_or(u64::MAX);
        if dirty || events_since_last_flush > self.flush_interval {
            match self.last_handled_event {
                Some(last_handled_event) => {
                    let res = self.checkpoints_collection
                    .update_one(
                        doc! { "_id": bson::Uuid::from(self.correlation_id), "last_event_id": last_handled_event as i64 },
                        doc! { "$set": { "last_event_id": last_handled_event as i64 } },
                    )
                    .session(&mut session)
                    .await?;
                    if res.matched_count == 0 {
                        return Err(EventHandlerError::Processor(
                            MongoEventProcessorError::UnexpectedLastEventId {
                                expected: Some(last_handled_event),
                            },
                        ));
                    }
                }
                None => {
                    let res = self
                        .checkpoints_collection
                        .insert_one(Checkpoint {
                            correlation_id: self.correlation_id,
                            last_event_id: event_id,
                        })
                        .session(&mut session)
                        .await;
                    match res {
                        Ok(_) => {}
                        Err(err) => match err.kind.as_ref() {
                            ErrorKind::Write(WriteFailure::WriteError(WriteError {
                                code, ..
                            })) if *code == 11000 => {
                                return Err(EventHandlerError::Processor(
                                    MongoEventProcessorError::UnexpectedLastEventId {
                                        expected: None,
                                    },
                                ));
                            }
                            _ => return Err(err.into()),
                        },
                    }
                }
            }

            let mut attempt = 1;
            loop {
                let res = session.commit_transaction().await;
                match res {
                    Ok(()) => {
                        self.last_handled_event = Some(event_id);
                        return Ok(());
                    }
                    Err(err) => match err.kind.as_ref() {
                        ErrorKind::Write(WriteFailure::WriteError(WriteError { code, .. }))
                            if *code == 112 =>
                        {
                            if attempt >= 1 {
                                return Err(err.into());
                            }
                            attempt += 1;
                        }
                        _ => return Err(err.into()),
                    },
                }
            }
        } else {
            debug!("ignoring event");
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Checkpoint {
    #[serde(rename = "_id", with = "bson::serde_helpers::uuid_1_as_binary")]
    correlation_id: Uuid,
    last_event_id: u64,
}
