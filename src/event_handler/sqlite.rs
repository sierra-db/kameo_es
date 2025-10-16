use std::{
    collections::{hash_map::Entry, HashMap},
    fmt,
    marker::PhantomData,
    ops::ControlFlow,
    time::Duration,
};

use kameo::prelude::*;
use kameo_es_core::Event;
use sqlx::{SqlitePool, SqliteTransaction};
use thiserror::Error;
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{debug, error, info};

use super::{CompositeEventHandler, EventHandler, EventHandlerError, EventProcessor};

pub struct SqliteProcessor<E, H>
where
    E: 'static,
    H: EventHandler<SqliteTransaction<'static>>
        + CompositeEventHandler<E, SqliteTransaction<'static>, SqliteProcessorError>
        + Send
        + 'static,
    <H as EventHandler<SqliteTransaction<'static>>>::Error: fmt::Debug + Sync,
{
    pool: SqlitePool,
    handler: H,
    worker_count: u16,
    workers: HashMap<u16, ActorRef<Worker<E, H>>>,
    last_flushed_sequences: HashMap<u16, u64>,
    flush_interval_time: Duration,
    flush_interval_events: u64,
}

impl<E, H> SqliteProcessor<E, H>
where
    E: 'static,
    H: EventHandler<SqliteTransaction<'static>>
        + CompositeEventHandler<E, SqliteTransaction<'static>, SqliteProcessorError>
        + Send
        + 'static,
    <H as EventHandler<SqliteTransaction<'static>>>::Error: fmt::Debug + Sync,
{
    pub async fn new(pool: SqlitePool, handler: H) -> anyhow::Result<Self> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS checkpoints (
                partition_id INTEGER NOT NULL PRIMARY KEY,
                sequence INTEGER NOT NULL
            ) 
            "#,
        )
        .execute(&pool)
        .await?;

        let partition_id_sequences: Vec<(i32, i64)> =
            sqlx::query_as("SELECT partition_id, sequence FROM checkpoints")
                .fetch_all(&pool)
                .await?;

        let last_flushed_sequences: HashMap<_, _> = partition_id_sequences
            .into_iter()
            .map(|(partition_id, sequence)| {
                (
                    partition_id.try_into().unwrap(),
                    u64::try_from(sequence).unwrap(),
                )
            })
            .collect();

        Ok(SqliteProcessor {
            pool,
            handler,
            worker_count: 16,
            workers: HashMap::new(),
            last_flushed_sequences,
            flush_interval_time: Duration::from_secs(2),
            flush_interval_events: 100,
        })
    }

    /// Number of parallelism.
    pub fn workers(mut self, count: u16) -> Self {
        self.worker_count = count;
        self
    }

    /// The number of seconds since last flush before attempting to flush again.
    pub fn flush_interval_time(mut self, period: Duration) -> Self {
        self.flush_interval_time = period;
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
}

impl<E, H> EventProcessor<E, H> for SqliteProcessor<E, H>
where
    E: 'static,
    H: EventHandler<SqliteTransaction<'static>>
        + CompositeEventHandler<E, SqliteTransaction<'static>, SqliteProcessorError>
        + Clone
        + Send
        + 'static,
    for<'a> <H as EventHandler<SqliteTransaction<'a>>>::Error: fmt::Debug + Unpin + Sync + 'static,
{
    type Context = SqliteTransaction<'static>;
    type Error = SqliteProcessorError;

    async fn start_from(&self) -> Result<HashMap<u16, u64>, Self::Error> {
        Ok(self
            .last_flushed_sequences
            .iter()
            .map(|(partition_id, sequence)| (*partition_id, sequence + 1))
            .collect())
    }

    async fn process_event(
        &mut self,
        event: Event,
    ) -> Result<(), EventHandlerError<Self::Error, <H as EventHandler<Self::Context>>::Error>> {
        let worker_id = event.partition_id % self.worker_count;
        let entry = self.workers.entry(worker_id);
        let worker_ref = match entry {
            Entry::Vacant(vacancy) => {
                let mut flush_interval = tokio::time::interval(self.flush_interval_time);
                flush_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

                let worker_ref = Worker::spawn_with_mailbox(
                    Worker {
                        pool: self.pool.clone(),
                        handler: self.handler.clone(),
                        transaction: None,
                        last_flushed_sequences: self.last_flushed_sequences.clone(),
                        last_handled_sequences: self.last_flushed_sequences.clone(),
                        flush_interval,
                        flush_interval_events: self.flush_interval_events,
                        phantom: PhantomData,
                    },
                    mailbox::bounded(1024 * 4),
                );

                vacancy.insert(worker_ref)
            }
            Entry::Occupied(occupied) => occupied.into_mut(),
        };

        match worker_ref.tell(HandleEvent(event)).await {
            Ok(()) => Ok(()),
            Err(err) => Err(EventHandlerError::Processor(SqliteProcessorError::Worker(
                err.map_msg(|_| ()),
            ))),
        }
    }
}

// impl<E, H> Actor for SqliteProcessor<E, H>
// where
//     E: 'static,
//     H: EventHandler<SqliteTransaction<'static>>
//         + CompositeEventHandler<E, SqliteTransaction<'static>, SqliteProcessorError>
//         + Send
//         + 'static,
//     <H as EventHandler<SqliteTransaction<'static>>>::Error: fmt::Debug + Sync,
// {
//     type Args = Self;
//     type Error = anyhow::Error;

//     async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
//         Ok(args)
//     }

//     async fn on_link_died(
//         &mut self,
//         _actor_ref: WeakActorRef<Self>,
//         id: ActorId,
//         reason: ActorStopReason,
//     ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
//         match &reason {
//             ActorStopReason::Normal => {
//                 info!("partition worker died normally");
//                 self.workers.retain(|_, worker| worker.id() != id);
//                 Ok(ControlFlow::Continue(()))
//             }
//             _ => {
//                 error!("partition worker died abnormally - stopping coordinator");
//                 Ok(ControlFlow::Break(ActorStopReason::LinkDied {
//                     id,
//                     reason: Box::new(reason),
//                 }))
//             }
//         }
//     }
// }

struct HandleEvent(Event);

struct Worker<E, H>
where
    E: 'static,
    H: EventHandler<SqliteTransaction<'static>>
        + CompositeEventHandler<E, SqliteTransaction<'static>, SqliteProcessorError>
        + Send
        + 'static,
    <H as EventHandler<SqliteTransaction<'static>>>::Error: fmt::Debug + Sync,
{
    pool: SqlitePool,
    handler: H,
    transaction: Option<SqliteTransaction<'static>>,
    last_flushed_sequences: HashMap<u16, u64>,
    last_handled_sequences: HashMap<u16, u64>,
    flush_interval: Interval,
    flush_interval_events: u64,
    phantom: PhantomData<fn() -> E>,
}

impl<E, H> Worker<E, H>
where
    E: 'static,
    H: EventHandler<SqliteTransaction<'static>>
        + CompositeEventHandler<E, SqliteTransaction<'static>, SqliteProcessorError>
        + Send
        + 'static,
    <H as EventHandler<SqliteTransaction<'static>>>::Error: fmt::Debug + Sync,
{
    async fn handle_event(
        &mut self,
        event: Event,
    ) -> Result<
        (),
        EventHandlerError<
            SqliteProcessorError,
            <H as EventHandler<SqliteTransaction<'static>>>::Error,
        >,
    > {
        if self
            .last_handled_sequences
            .get(&event.partition_id)
            .map(|last_sequence| last_sequence >= &event.partition_sequence)
            .unwrap_or(false)
        {
            debug!(
                "ignoring already handled event {}:{}",
                event.partition_id, event.partition_sequence
            );
            return Ok(());
        }

        let partition_id = event.partition_id;
        let sequence = event.partition_sequence;

        let tx = match self.transaction.as_mut() {
            Some(tx) => tx,
            None => {
                let tx = self.pool.begin().await?;
                self.transaction.insert(tx)
            }
        };

        let res = self.handler.composite_handle(tx, event).await;
        if let Err(err) = res {
            let _ = self
                .transaction
                .take()
                .expect("transaction should be active here")
                .rollback()
                .await;
            return Err(err);
        }

        self.last_handled_sequences.insert(partition_id, sequence);

        let events_since_flush: u64 = self
            .last_handled_sequences
            .iter()
            .map(|(partition_id, sequence)| {
                self.last_flushed_sequences
                    .get(partition_id)
                    .map(|last_flushed_sequence| sequence - last_flushed_sequence)
                    .unwrap_or(*sequence + 1)
            })
            .sum();

        let flush_reason = (events_since_flush >= self.flush_interval_events)
            .then_some(FlushReason::LiveEventsInterval);

        if let Some(reason) = flush_reason {
            self.flush_checkpoint(reason).await?;
        }

        Ok(())
    }

    async fn flush_checkpoint(
        &mut self,
        reason: FlushReason,
    ) -> Result<
        (),
        EventHandlerError<
            SqliteProcessorError,
            <H as EventHandler<SqliteTransaction<'static>>>::Error,
        >,
    > {
        let Some(tx) = self.transaction.as_mut() else {
            return Ok(());
        };

        for (partition_id, last_handled_sequence) in &self.last_handled_sequences {
            let last_flushed_sequence = self.last_flushed_sequences.get(partition_id);
            match (last_flushed_sequence, last_handled_sequence) {
                (None, last_handled_sequence) => {
                    info!("flushing due to {reason:?}");

                    self.handler
                        .flush(tx)
                        .await
                        .map_err(EventHandlerError::Handler)?;

                    let res = sqlx::query(
                        "INSERT INTO checkpoints (partition_id, sequence) VALUES ($1, $2)",
                    )
                    .bind(*partition_id as i32)
                    .bind(*last_handled_sequence as i64)
                    .execute(&mut **tx)
                    .await;

                    match res {
                        Ok(_) => {}
                        Err(sqlx::Error::Database(db_err))
                            if db_err.code().as_deref() == Some("1555")
                                || db_err.code().as_deref() == Some("2067") =>
                        {
                            // 1555 is SQLITE_CONSTRAINT_PRIMARYKEY
                            // 2067 is SQLITE_CONSTRAINT_UNIQUE
                            return Err(EventHandlerError::Processor(
                                SqliteProcessorError::UnexpectedLastEventId { expected: None },
                            ));
                        }
                        Err(err) => return Err(err.into()),
                    }
                }
                (Some(last_flushed_sequence), last_handled_sequence)
                    if last_flushed_sequence != last_handled_sequence =>
                {
                    info!("flushing due to {reason:?}");

                    self.handler
                        .flush(tx)
                        .await
                        .map_err(EventHandlerError::Handler)?;

                    let res =
                        sqlx::query("UPDATE checkpoints SET sequence = $1 WHERE partition_id = $2")
                            .bind(*last_handled_sequence as i64)
                            .bind(*partition_id as i32)
                            .execute(&mut **tx)
                            .await?;
                    if res.rows_affected() == 0 {
                        return Err(EventHandlerError::Processor(
                            SqliteProcessorError::UnexpectedLastEventId {
                                expected: Some(*last_handled_sequence),
                            },
                        ));
                    }
                }
                (Some(_), _) => {}
            }
        }

        self.handler
            .flush(tx)
            .await
            .map_err(EventHandlerError::Handler)?;

        self.transaction
            .take()
            .unwrap()
            .commit()
            .await
            .map_err(|err| EventHandlerError::Processor(err.into()))?;

        self.last_flushed_sequences = self.last_handled_sequences.clone();

        Ok(())
    }
}

impl<E, H> Actor for Worker<E, H>
where
    E: 'static,
    H: EventHandler<SqliteTransaction<'static>>
        + CompositeEventHandler<E, SqliteTransaction<'static>, SqliteProcessorError>
        + Send
        + 'static,
    <H as EventHandler<SqliteTransaction<'static>>>::Error: fmt::Debug + Sync,
{
    type Args = Self;
    type Error = anyhow::Error;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }

    async fn on_panic(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
        error!("PartitionWorker panicked: {err:?}");
        Ok(ControlFlow::Break(ActorStopReason::Panicked(err)))
    }

    async fn next(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        mailbox_rx: &mut MailboxReceiver<Self>,
    ) -> Option<mailbox::Signal<Self>> {
        loop {
            tokio::select! {
                msg = mailbox_rx.recv() => return msg,
                _ = self.flush_interval.tick() => {
                    self.flush_checkpoint(FlushReason::TimeInterval).await.unwrap();
                }
            }
        }
    }
}

impl<E, H> Message<HandleEvent> for Worker<E, H>
where
    E: 'static,
    H: EventHandler<SqliteTransaction<'static>>
        + CompositeEventHandler<E, SqliteTransaction<'static>, SqliteProcessorError>
        + Send
        + 'static,
    <H as EventHandler<SqliteTransaction<'static>>>::Error: fmt::Debug + Sync,
{
    type Reply = Result<
        (),
        EventHandlerError<
            SqliteProcessorError,
            <H as EventHandler<SqliteTransaction<'static>>>::Error,
        >,
    >;

    async fn handle(
        &mut self,
        HandleEvent(event): HandleEvent,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_event(event).await
    }
}

#[derive(Debug, Error)]
pub enum SqliteProcessorError {
    #[error(transparent)]
    Worker(#[from] SendError),
    #[error(transparent)]
    Postgres(#[from] sqlx::Error),
    #[error("unexpected last event id, expected {expected:?}")]
    UnexpectedLastEventId { expected: Option<u64> },
}

impl<H> From<sqlx::Error> for EventHandlerError<SqliteProcessorError, H> {
    fn from(err: sqlx::Error) -> Self {
        EventHandlerError::Processor(SqliteProcessorError::Postgres(err))
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FlushReason {
    TimeInterval,
    LiveEventsInterval,
    ReplayEventsInterval,
}
