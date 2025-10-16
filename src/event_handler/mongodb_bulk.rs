use std::{
    collections::{BTreeMap, HashMap},
    mem,
    num::ParseIntError,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use bson::to_bson;
use mongodb::{
    bson::doc,
    error::{ErrorKind, WriteError, WriteFailure},
    options::{
        DeleteManyModel, DeleteOneModel, InsertOneModel, ReplaceOneModel, UpdateManyModel,
        UpdateOneModel, WriteModel,
    },
    Client, ClientSession, Collection,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{sync::Mutex, task::JoinHandle};
use tracing::{error, info};

use crate::Event;

use super::{CompositeEventHandler, EventHandler, EventHandlerError, EventProcessor};

#[derive(Debug, Default)]
pub struct Models {
    models: Vec<WriteModel>,
}

impl Models {
    pub fn new() -> Self {
        Models { models: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Models {
            models: Vec::with_capacity(capacity),
        }
    }

    pub fn insert_one(&mut self, model: InsertOneModel) {
        self.models.push(WriteModel::InsertOne(model));
    }

    pub fn update_one(&mut self, model: UpdateOneModel) {
        self.models.push(WriteModel::UpdateOne(model));
    }

    pub fn update_many(&mut self, model: UpdateManyModel) {
        self.models.push(WriteModel::UpdateMany(model));
    }

    pub fn replace_one(&mut self, model: ReplaceOneModel) {
        self.models.push(WriteModel::ReplaceOne(model));
    }

    pub fn delete_one(&mut self, model: DeleteOneModel) {
        self.models.push(WriteModel::DeleteOne(model));
    }

    pub fn delete_many(&mut self, model: DeleteManyModel) {
        self.models.push(WriteModel::DeleteMany(model));
    }

    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }

    pub fn len(&self) -> usize {
        self.models.len()
    }
}

pub struct MongoDBBulkEventProcessor<H> {
    client: Client,
    checkpoints_collection: Collection<Checkpoint>,
    projection_id: String,
    handler: H,
    models: Models,
    last_handled_sequences: HashMap<u16, u64>,
    last_flushed: Arc<Mutex<(Instant, HashMap<u16, u64>)>>,
    flush_interval_time: Duration,
    flush_interval_models: usize,
    flush_interval_events: u64,
}

impl<H> MongoDBBulkEventProcessor<H> {
    pub async fn new(
        client: Client,
        checkpoints_collection: Collection<Checkpoint>,
        projection_id: impl Into<String>,
        handler: H,
    ) -> anyhow::Result<Self> {
        let projection_id = projection_id.into();

        let last_handled_sequences: HashMap<u16, u64> = checkpoints_collection
            .find_one(doc! { "_id": &projection_id })
            .await?
            .map(|checkpoint: Checkpoint| {
                checkpoint
                    .sequences
                    .into_iter()
                    .map(|(partition_id, sequence)| {
                        Ok::<_, ParseIntError>((u16::from_str(&partition_id)?, sequence))
                    })
                    .collect::<Result<_, _>>()
            })
            .transpose()
            .context("failed to parse partition id in checkpoints collection")?
            .unwrap_or_default();

        Ok(MongoDBBulkEventProcessor {
            client,
            checkpoints_collection,
            projection_id,
            handler,
            models: Models::with_capacity(500),
            last_handled_sequences: last_handled_sequences.clone(),
            last_flushed: Arc::new(Mutex::new((Instant::now(), last_handled_sequences))),
            flush_interval_time: Duration::from_secs(10),
            flush_interval_models: 100_000,
            flush_interval_events: 2_000,
        })
    }

    /// The number of seconds since last flush before attempting to flush again.
    pub fn flush_interval_time(mut self, period: Duration) -> Self {
        self.flush_interval_time = period;
        self
    }

    /// The number of models queued before attempting to flush.
    pub fn flush_interval_models(mut self, models_count: usize) -> Self {
        self.flush_interval_models = models_count;
        self
    }

    /// The number of events since the last flush before attempting to flush again.
    pub fn flush_interval_events(mut self, events_count: u64) -> Self {
        self.flush_interval_events = events_count;
        self
    }

    /// Gets a reference to the inner handler.
    pub fn handler(&mut self) -> &mut H {
        &mut self.handler
    }

    pub fn last_handled_sequences(&self) -> &HashMap<u16, u64> {
        &self.last_handled_sequences
    }

    pub fn should_flush(&self) -> Option<FlushReason> {
        // Flush if our models exceeds the models length flush interval models threshold
        if self.models.len() >= self.flush_interval_models {
            return Some(FlushReason::ModelsInterval);
        }

        let last_flushed = self.last_flushed.try_lock().ok()?;

        // Flush if we've exceeded the flush interval time
        if last_flushed.0.elapsed() >= self.flush_interval_time {
            return Some(FlushReason::TimeInterval);
        }

        // Flush if we've processed more events than the flush interval events threshold
        let events_since_flush: u64 = self
            .last_handled_sequences
            .iter()
            .map(|(partition_id, sequence)| {
                last_flushed
                    .1
                    .get(partition_id)
                    .map(|last_flushed_sequence| sequence - last_flushed_sequence)
                    .unwrap_or(0)
            })
            .sum();
        let exceeded_flush_interval_events = events_since_flush >= self.flush_interval_events;
        if exceeded_flush_interval_events {
            return Some(FlushReason::EventsInterval);
        }

        None
    }

    pub async fn flush(&mut self) -> Option<JoinHandle<Result<(), MongoEventProcessorError>>> {
        if let Some(reason) = self.should_flush() {
            return self.force_flush(reason).await;
        }

        None
    }

    pub async fn force_flush(
        &mut self,
        reason: FlushReason,
    ) -> Option<JoinHandle<Result<(), MongoEventProcessorError>>> {
        // If we haven't handled any sequences, we can skip flushing
        if self.last_handled_sequences.is_empty() {
            return None;
        }

        info!("flushing because {reason:?}");

        let mut last_flushed = self.last_flushed.clone().lock_owned().await;
        let client = self.client.clone();
        let checkpoints_collection = self.checkpoints_collection.clone();
        let projection_id = self.projection_id.clone();
        let models = mem::take(&mut self.models.models);
        let last_handled_sequences = self.last_handled_sequences.clone();

        let handle = tokio::spawn(async move {
            let mut session = client.start_session().await?;
            session.start_transaction().await?;

            let res = Self::force_flush_inner(
                client,
                checkpoints_collection,
                &mut session,
                &last_flushed.1,
                projection_id.clone(),
                &last_handled_sequences,
                models,
            )
            .await;
            match res {
                Ok(()) => {
                    session.commit_transaction().await.unwrap();
                    for (partition_id, sequence) in &last_handled_sequences {
                        info!(gauge.projection_sequence = sequence, %projection_id, partition_id, database = "mongodb");
                    }
                    last_flushed.1 = last_handled_sequences;
                }
                Err(err) => {
                    let _ = session.abort_transaction().await;
                    error!("{err}");
                    return Err(err);
                }
            }

            last_flushed.0 = Instant::now();

            Ok(())
        });

        Some(handle)
    }

    async fn force_flush_inner(
        client: Client,
        checkpoints_collection: Collection<Checkpoint>,
        session: &mut ClientSession,
        last_flushed_sequences: &HashMap<u16, u64>,
        projection_id: String,
        last_handled_sequences: &HashMap<u16, u64>,
        models: Vec<WriteModel>,
    ) -> Result<(), MongoEventProcessorError> {
        let last_flushed_sequences_bson: BTreeMap<_, _> = last_flushed_sequences
            .iter()
            .map(|(partition_id, sequence)| (partition_id.to_string(), *sequence))
            .collect();
        let last_handled_sequences_bson: BTreeMap<_, _> = last_handled_sequences
            .iter()
            .map(|(partition_id, sequence)| (partition_id.to_string(), *sequence))
            .collect();

        if !last_flushed_sequences.is_empty() {
            let res = checkpoints_collection
                .update_one(
                    doc! {
                        "_id": projection_id,
                        "sequences": to_bson(&last_flushed_sequences_bson)?,
                    },
                    doc! { "$set": { "sequences": to_bson(&last_handled_sequences_bson)? } },
                )
                .session(&mut *session)
                .await?;
            if res.matched_count == 0 {
                return Err(MongoEventProcessorError::UnexpectedLastSequences {
                    expected: last_flushed_sequences.clone(),
                });
            }
        } else {
            let res = checkpoints_collection
                .insert_one(Checkpoint {
                    id: projection_id,
                    sequences: last_handled_sequences_bson,
                })
                .session(&mut *session)
                .await;
            match res {
                Ok(_) => {}
                Err(err) => match err.kind.as_ref() {
                    ErrorKind::Write(WriteFailure::WriteError(WriteError { code, .. }))
                        if *code == 11000 =>
                    {
                        return Err(MongoEventProcessorError::UnexpectedLastSequences {
                            expected: HashMap::new(),
                        });
                    }
                    _ => return Err(err.into()),
                },
            }
        }

        if !models.is_empty() {
            client.bulk_write(models).session(&mut *session).await?;
        }

        Ok(())
    }
}

impl<E, H> EventProcessor<E, H> for MongoDBBulkEventProcessor<H>
where
    H: EventHandler<Models> + CompositeEventHandler<E, Models, MongoEventProcessorError>,
{
    type Context = Models;
    type Error = MongoEventProcessorError;

    async fn start_from(&self) -> Result<HashMap<u16, u64>, Self::Error> {
        for (partition_id, sequence) in &self.last_handled_sequences {
            info!(gauge.projection_sequence = sequence, projection_id = %self.projection_id, partition_id, database = "mongodb");
        }

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
        let partition_id = event.partition_id;
        let partition_sequence = event.partition_sequence;
        self.handler
            .composite_handle(&mut self.models, event)
            .await?;
        *self.last_handled_sequences.entry(partition_id).or_default() = partition_sequence;
        self.flush().await;

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Checkpoint {
    #[serde(rename = "_id")]
    pub id: String,
    pub sequences: BTreeMap<String, u64>,
}

#[derive(Clone, Copy, Debug)]
pub enum FlushReason {
    TimeInterval,
    EventsInterval,
    ModelsInterval,
    Manual,
}

pub trait FlushHandler {
    fn flush(&mut self);
}

#[derive(Debug, Error)]
pub enum MongoEventProcessorError {
    #[error("unexpected last event id, expected {expected:?}")]
    UnexpectedLastSequences { expected: HashMap<u16, u64> },
    #[error(transparent)]
    Mongodb(#[from] mongodb::error::Error),
    #[error(transparent)]
    SerializeBson(#[from] bson::ser::Error),
}
