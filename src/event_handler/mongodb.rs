use std::{collections::HashMap, ops};

use mongodb::{
    bson::{bson, doc},
    Client, ClientSession, Collection,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::debug;

use crate::Event;

use super::{CompositeEventHandler, EventHandler, EventHandlerError, EventProcessor};

pub struct Session {
    pub(crate) session: ClientSession,
    pub(crate) dirty: bool,
}

impl Session {
    pub fn new(session: ClientSession) -> Self {
        Session {
            session,
            dirty: false,
        }
    }

    pub async fn commit(mut self) -> Result<(), mongodb::error::Error> {
        self.session.commit_transaction().await
    }
}

impl ops::Deref for Session {
    type Target = ClientSession;

    fn deref(&self) -> &Self::Target {
        &self.session
    }
}

impl ops::DerefMut for Session {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.session
    }
}

pub struct MongoDBEventProcessor<H> {
    client: Client,
    checkpoints_collection: Collection<Checkpoint>,
    projection_id: String,
    handler: H,
    last_handled_sequences: HashMap<u16, u64>,
    flush_interval: u64,
}

impl<H> MongoDBEventProcessor<H> {
    pub async fn new(
        client: Client,
        checkpoints_collection: Collection<Checkpoint>,
        projection_id: impl Into<String>,
        handler: H,
    ) -> anyhow::Result<Self> {
        let projection_id = projection_id.into();

        let last_handled_sequences = checkpoints_collection
            .find_one(doc! { "_id": &projection_id })
            .await?
            .map(|checkpoint: Checkpoint| checkpoint.last_handled_sequences)
            .unwrap_or_default();

        Ok(MongoDBEventProcessor {
            client,
            checkpoints_collection,
            projection_id,
            handler,
            last_handled_sequences,
            flush_interval: 100,
        })
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

    pub fn last_handled_sequences(&self) -> &HashMap<u16, u64> {
        &self.last_handled_sequences
    }
}

impl<E, H> EventProcessor<E, H> for MongoDBEventProcessor<H>
where
    H: EventHandler<Session> + CompositeEventHandler<E, Session, MongoEventProcessorError>,
{
    type Context = Session;
    type Error = MongoEventProcessorError;

    async fn start_from(&self) -> Result<HashMap<u16, u64>, Self::Error> {
        Ok(self
            .last_handled_sequences
            .iter()
            .map(|(partition_id, sequence)| (*partition_id, sequence + 1))
            .collect())
    }

    async fn process_event(
        &mut self,
        event: Event,
    ) -> Result<(), EventHandlerError<Self::Error, <H as EventHandler<Self::Context>>::Error>> {
        let event_id = event.id;

        let mut session = self.client.start_session().await?;
        session.start_transaction().await?;
        let mut session = Session::new(session);

        let res = self.handler.composite_handle(&mut session, event).await;
        if res.is_err() {
            let abort_res = session.abort_transaction().await;
            res?;
            abort_res?;
        }

        let Session { mut session, dirty } = session;

        let events_since_last_flush = self
            .last_handled_sequences
            .map(|last| event_id - last)
            .unwrap_or(u64::MAX);
        if dirty || events_since_last_flush > self.flush_interval {
            let expected_last_event_id = match self.last_handled_sequences {
                Some(last) => bson!({ "$eq": last as i64 }),
                None => bson!({ "$exists": false }),
            };
            let update_result = self.checkpoints_collection
                .update_one(
                    doc! { "_id": &self.projection_id, "last_event_id": &expected_last_event_id },
                    doc! { "$set": { "last_event_id": event_id as i64 }, "$setOnInsert": { "_id": &self.projection_id } },
                )
                .upsert(true)
                .session(&mut session)
                .await;
            match update_result {
                Ok(update_result) => {
                    if update_result.modified_count == 0 && event_id > 0 {
                        session.abort_transaction().await?;
                        return Err(EventHandlerError::Processor(
                            MongoEventProcessorError::UnexpectedLastEventId {
                                expected: self.last_handled_sequences,
                            },
                        ));
                    }
                }
                Err(err) => return Err(err.into()),
            }

            session.commit_transaction().await?;

            self.last_handled_sequences = Some(event_id);
        } else {
            debug!(event_id = %event_id, "ignoring event");
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Checkpoint {
    #[serde(rename = "_id")]
    pub id: String,
    pub last_handled_sequences: HashMap<u16, u64>,
}

#[derive(Debug, Error)]
pub enum MongoEventProcessorError {
    #[error("unexpected last event id, expected {expected:?}")]
    UnexpectedLastEventId { expected: Option<u64> },
    #[error(transparent)]
    Mongodb(#[from] mongodb::error::Error),
}

impl<H> From<mongodb::error::Error> for EventHandlerError<MongoEventProcessorError, H> {
    fn from(err: mongodb::error::Error) -> Self {
        EventHandlerError::Processor(MongoEventProcessorError::Mongodb(err))
    }
}
