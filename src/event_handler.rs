pub mod file;
pub mod in_memory;
// #[cfg(feature = "mongodb")]
// pub mod mongodb;
#[cfg(feature = "mongodb")]
pub mod mongodb_bulk;
// #[cfg(feature = "mongodb")]
// pub mod mongodb_correlations;
// #[cfg(feature = "polodb")]
// pub mod polodb;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

use std::{collections::HashMap, marker::PhantomData};

use futures::Future;
use redis::RedisError;
use sierradb_client::{EventSubscription, SierraError, SierraMessage, SubscriptionManager};
use thiserror::Error;
use tracing::{debug, trace};

use crate::{event_from_sierra, Entity, Event, TryFromSierraEventError};

pub trait EventProcessor<E, H>
where
    Self: Send,
    H: EventHandler<Self::Context>,
{
    type Context: Send;
    type Error: Send;

    /// Which event to start streaming from.
    fn start_from(&self) -> impl Future<Output = Result<HashMap<u16, u64>, Self::Error>>;

    /// Processes an event, which should internally call the event handler.
    fn process_event(
        &mut self,
        event: Event,
    ) -> impl Future<Output = Result<(), EventHandlerError<Self::Error, H::Error>>> + Send;
}

/// An event handler.
pub trait EventHandler<C>: Send {
    type Error: Send;

    /// Handles an event, typically as a fallback when no entities were matched.
    fn handle(
        &mut self,
        _ctx: &mut C,
        _event: Event,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move { Ok(()) }
    }

    fn flush(&mut self, _ctx: &mut C) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move { Ok(()) }
    }
}

/// An event handler for an entity.
pub trait EntityEventHandler<E, C>: EventHandler<C>
where
    E: Entity,
{
    /// Handles an event for an entity.
    fn handle(
        &mut self,
        ctx: &mut C,
        id: E::ID,
        event: Event<E::Event, E::Metadata>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A trait for handling events based on a tuple of entities, where each entity is checked against the event category
/// in order until a match is found, which will then be handled using the `EntityEventHandler`.
pub trait CompositeEventHandler<E, C, PE>
where
    Self: EventHandler<C> + Sized,
{
    /// Handles an event, determining which entity it belongs to, falling back to the `EventHandler` implementation.
    fn composite_handle(
        &mut self,
        ctx: &mut C,
        event: Event,
    ) -> impl Future<Output = Result<(), EventHandlerError<PE, Self::Error>>> + Send;
}

/// A helper trait for creating an event handler stream.
pub trait EventHandlerStreamBuilder: Sized + 'static {
    fn event_handler_stream<P, H>(
        manager: &mut SubscriptionManager,
        processor: &mut P,
    ) -> impl Future<Output = Result<EventHandlerStream<Self>, EventHandlerError<P::Error, H::Error>>>
    where
        P: EventProcessor<Self, H>,
        H: EventHandler<P::Context>;
}

impl<E: 'static> EventHandlerStreamBuilder for E {
    async fn event_handler_stream<P, H>(
        manager: &mut SubscriptionManager,
        processor: &mut P,
    ) -> Result<EventHandlerStream<Self>, EventHandlerError<P::Error, H::Error>>
    where
        P: EventProcessor<Self, H>,
        H: EventHandler<P::Context>,
    {
        EventHandlerStream::new(manager, processor).await
    }
}

/// An error which occurs when handling an event.
#[derive(Debug, Error)]
pub enum EventHandlerError<P, H> {
    #[error("failed to deserialize event '{event}' for entity '{entity}': {err}")]
    DeserializeEvent {
        entity: &'static str,
        event: String,
        err: ciborium::value::Error,
    },
    #[error(transparent)]
    Sierra(#[from] SierraError),
    #[error("failed to parse entity id: {0}")]
    ParseID(String),
    #[error("{0}")]
    Processor(P),
    #[error("{0}")]
    Handler(H),
    #[error(transparent)]
    EventFromSierra(#[from] TryFromSierraEventError),
}

impl<P, H> From<RedisError> for EventHandlerError<P, H> {
    fn from(err: RedisError) -> Self {
        EventHandlerError::Sierra(err.into())
    }
}

/// A stream which processes events using an `EventProcessor`.
pub struct EventHandlerStream<E> {
    subscription: EventSubscription,
    events_since_ack: u64,
    phantom: PhantomData<fn() -> E>,
}

impl<E> EventHandlerStream<E> {
    async fn new<P, H>(
        manager: &mut SubscriptionManager,
        processor: &mut P,
    ) -> Result<Self, EventHandlerError<P::Error, H::Error>>
    where
        E: 'static,
        P: EventProcessor<E, H>,
        H: EventHandler<P::Context>,
    {
        let start_from = processor
            .start_from()
            .await
            .map_err(EventHandlerError::Processor)?;
        let subscription = manager
            .subscribe_to_all_partitions_flexible(start_from, Some(0), Some(10_000))
            .await?;

        Ok(EventHandlerStream {
            subscription,
            events_since_ack: 0,
            phantom: PhantomData,
        })
    }

    pub async fn process_next<P, H>(
        &mut self,
        processor: &mut P,
    ) -> Option<Result<(), EventHandlerError<P::Error, H::Error>>>
    where
        E: 'static,
        P: EventProcessor<E, H>,
        H: EventHandler<P::Context>,
    {
        match self.next().await? {
            Ok(event) => Some(event.process(processor).await),
            Err(err) => Some(Err(err.into())),
        }
    }

    pub async fn run<P, H>(
        &mut self,
        processor: &mut P,
    ) -> Result<(), EventHandlerError<P::Error, H::Error>>
    where
        E: 'static,
        P: EventProcessor<E, H>,
        H: EventHandler<P::Context>,
    {
        while let Some(unprocessed_event) = self.next().await.transpose()? {
            unprocessed_event.process(processor).await?;
        }
        Ok(())
    }

    pub async fn next(&mut self) -> Option<Result<UnprocessedEvent<E>, NextEventError>> {
        while let Some(event) = self.subscription.next_message().await {
            match event {
                SierraMessage::Event { event, cursor } => {
                    self.events_since_ack += 1;
                    if self.events_since_ack >= 8_000 {
                        trace!("acknowledging up to cursor {cursor}");
                        if let Err(err) = self.subscription.acknowledge_up_to_cursor(cursor).await {
                            return Some(Err(err.into()));
                        }
                        self.events_since_ack = 0;
                    }

                    let event = match event_from_sierra(event) {
                        Ok(event) => event,
                        Err(err) => return Some(Err(err.into())),
                    };
                    return Some(Ok(UnprocessedEvent::new(event)));
                }
                SierraMessage::SubscriptionConfirmed { .. } => {}
            }
        }

        None
    }
}

#[derive(Debug, Error)]
pub enum NextEventError {
    #[error(transparent)]
    Sierra(#[from] SierraError),
    #[error(transparent)]
    DeserializeEvent(#[from] TryFromSierraEventError),
}

impl From<RedisError> for NextEventError {
    fn from(err: RedisError) -> Self {
        NextEventError::Sierra(err.into())
    }
}

impl<P, H> From<NextEventError> for EventHandlerError<P, H> {
    fn from(err: NextEventError) -> Self {
        match err {
            NextEventError::Sierra(err) => EventHandlerError::Sierra(err),
            NextEventError::DeserializeEvent(err) => EventHandlerError::EventFromSierra(err),
        }
    }
}

#[must_use = "the event has not been processed yet"]
pub struct UnprocessedEvent<E> {
    pub event: Event,
    phantom: PhantomData<fn() -> E>,
}

impl<E> UnprocessedEvent<E> {
    fn new(event: Event) -> Self {
        UnprocessedEvent {
            event,
            phantom: PhantomData,
        }
    }

    pub async fn process<P, H>(
        self,
        processor: &mut P,
    ) -> Result<(), EventHandlerError<P::Error, H::Error>>
    where
        P: EventProcessor<E, H>,
        H: EventHandler<P::Context>,
    {
        debug!(
            "{:>2}:{:>6} {:<32} {:>6} > {}",
            self.event.partition_id,
            self.event.partition_sequence,
            self.event.stream_id,
            self.event.stream_version,
            self.event.name
        );
        processor.process_event(self.event).await
    }
}

macro_rules! impl_composite_event_handler {
    (
        $( ( $( $ent:ident ),* ), )+
    ) => {
        $(
            impl_composite_event_handler!( $( $ent ),* );
        )+
    };
    ( $( $( $ent:ident ),+ )? ) => {
        impl<H, C, PE $( , $( $ent ),+ )?> CompositeEventHandler<( $( $( $ent, )+ )? ), C, PE> for H
        where
            H: EventHandler<C> + Sized,
            C: Send,
            PE: Send,
            $( $(
                H: EntityEventHandler<$ent, C>,
                $ent: Entity,
            )+ )?
        {
            async fn composite_handle(
                &mut self,
                ctx: &mut C,
                event: Event,
            ) -> Result<(), EventHandlerError<PE, Self::Error>> {
                $(
                    let category = event.stream_id.category();
                    $(
                        if category == $ent::category() {
                            EntityEventHandler::<$ent, C>::handle(
                                self,
                                ctx,
                                event.entity_id::<$ent>().map_err(|_| {
                                    EventHandlerError::ParseID(event.stream_id.cardinal_id().to_string())
                                })?,
                                event.as_entity::<$ent>().map_err(|(event, err)| {
                                    EventHandlerError::DeserializeEvent {
                                        entity: $ent::category(),
                                        event: event.name,
                                        err,
                                    }
                                })?,
                            )
                            .await
                            .map_err(EventHandlerError::Handler)
                        } else
                    )+
                )?

                {
                    EventHandler::handle(self, ctx, event)
                        .await
                        .map_err(EventHandlerError::Handler)
                }
            }
        }
    };
}

impl_composite_event_handler![
    (),
    (E1),
    (E1, E2),
    (E1, E2, E3),
    (E1, E2, E3, E4),
    (E1, E2, E3, E4, E5),
    (E1, E2, E3, E4, E5, E6),
    (E1, E2, E3, E4, E5, E6, E7),
    (E1, E2, E3, E4, E5, E6, E7, E8),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9, E10),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15),
    (E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16),
];
