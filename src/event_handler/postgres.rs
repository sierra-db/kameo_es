use std::{
    collections::{hash_map::Entry, HashMap},
    fmt,
    marker::PhantomData,
    ops::ControlFlow,
    sync::Arc,
    time::{Duration, Instant},
};

use backon::{ExponentialBuilder, RetryableWithContext};
use futures::future::OptionFuture;
use kameo::{mailbox::Signal, prelude::*};
use redis::aio::MultiplexedConnection;
use sierradb_client::AsyncTypedCommands;
use sqlx::{PgPool, Postgres};
use thiserror::Error;
use tracing::{debug, error, info};

use crate::Event;

use super::{CompositeEventHandler, EventHandler, EventHandlerError, EventProcessor};

pub struct PostgresProcessor<E, H>
where
    E: 'static,
    H: EventHandler<sqlx::Transaction<'static, Postgres>>
        + CompositeEventHandler<E, sqlx::Transaction<'static, Postgres>, PostgresEventProcessorError>
        + Send
        + 'static,
    <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error: fmt::Debug + Sync,
{
    pool: PgPool,
    conn: MultiplexedConnection,
    checkpoints_table: Arc<str>,
    projection_id: Arc<str>,
    handler: H,
    worker_count: u16,
    workers: HashMap<u16, ActorRef<Worker<E, H>>>,
    last_flushed_sequences: HashMap<u16, u64>,
    flush_live_interval_time: Duration,
    flush_live_interval_events: u64,
    flush_replay_interval_time: Duration,
    flush_replay_interval_events: u64,
}

impl<E, H> PostgresProcessor<E, H>
where
    E: 'static,
    H: EventHandler<sqlx::Transaction<'static, Postgres>>
        + CompositeEventHandler<E, sqlx::Transaction<'static, Postgres>, PostgresEventProcessorError>
        + Send
        + 'static,
    <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error: fmt::Debug + Sync,
{
    pub async fn new(
        pool: PgPool,
        conn: MultiplexedConnection,
        checkpoints_table: impl Into<Arc<str>>,
        projection_id: impl Into<Arc<str>>,
        handler: H,
    ) -> sqlx::Result<Self> {
        let checkpoints_table = checkpoints_table.into();
        let projection_id = projection_id.into();

        let partition_id_sequences: Vec<(i32, i64)> = sqlx::query_as(&format!(
            "SELECT partition_id, sequence FROM {checkpoints_table} WHERE projection_id = $1",
        ))
        .bind(projection_id.as_ref())
        .fetch_all(&pool)
        .await?;

        for (partition_id, sequence) in &partition_id_sequences {
            info!(gauge.projection_sequence = sequence, %projection_id, partition_id, database = "postgres");
        }

        let last_flushed_sequences = partition_id_sequences
            .into_iter()
            .map(|(partition_id, sequence)| {
                (
                    partition_id.try_into().unwrap(),
                    u64::try_from(sequence).unwrap(),
                )
            })
            .collect();

        Ok(PostgresProcessor {
            pool,
            conn,
            checkpoints_table: checkpoints_table.clone(),
            projection_id: projection_id.clone(),
            handler,
            worker_count: 16,
            workers: HashMap::new(),
            last_flushed_sequences,
            flush_live_interval_time: Duration::from_secs(2),
            flush_live_interval_events: 10,
            flush_replay_interval_time: Duration::from_secs(10),
            flush_replay_interval_events: 10_000,
        })
    }

    /// Number of parallelism.
    pub fn workers(mut self, count: u16) -> Self {
        self.worker_count = count;
        self
    }

    /// The number of seconds since last flush before attempting to flush again.
    pub fn flush_live_interval_time(mut self, period: Duration) -> Self {
        self.flush_live_interval_time = period;
        self
    }

    /// The number of events since the last flush before attempting to flush again when in live mode.
    pub fn flush_live_interval_events(mut self, events_count: u64) -> Self {
        self.flush_live_interval_events = events_count;
        self
    }

    /// The number of seconds since last flush before attempting to flush again.
    pub fn flush_replay_interval_time(mut self, period: Duration) -> Self {
        self.flush_replay_interval_time = period;
        self
    }

    /// The number of events since the last flush before attempting to flush again when in replay mode.
    pub fn flush_replay_interval_events(mut self, events_count: u64) -> Self {
        self.flush_replay_interval_events = events_count;
        self
    }

    /// Gets a reference to the inner handler.
    pub fn handler(&mut self) -> &mut H {
        &mut self.handler
    }
}

impl<E, H> EventProcessor<E, H> for ActorRef<PostgresProcessor<E, H>>
where
    E: 'static,
    H: EventHandler<sqlx::Transaction<'static, Postgres>>
        + CompositeEventHandler<E, sqlx::Transaction<'static, Postgres>, PostgresEventProcessorError>
        + Clone
        + Send
        + 'static,
    for<'a> <H as EventHandler<sqlx::Transaction<'a, Postgres>>>::Error:
        fmt::Debug + Unpin + Sync + 'static,
{
    type Context = sqlx::Transaction<'static, Postgres>;
    type Error = PostgresEventProcessorError;

    async fn start_from(&self) -> Result<HashMap<u16, u64>, Self::Error> {
        let from_map = self
            .ask(GetStartFrom)
            .send()
            .await
            .map_err(|err| err.map_msg(|_| ()))?;
        Ok(from_map)
    }

    async fn process_event(
        &mut self,
        event: Event,
    ) -> Result<(), EventHandlerError<Self::Error, <H as EventHandler<Self::Context>>::Error>> {
        self.tell(HandleEvent(event)).send().await.unwrap();
        Ok(())
    }
}

impl<E, H> Actor for PostgresProcessor<E, H>
where
    E: 'static,
    H: EventHandler<sqlx::Transaction<'static, Postgres>>
        + CompositeEventHandler<E, sqlx::Transaction<'static, Postgres>, PostgresEventProcessorError>
        + Send
        + 'static,
    <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error: fmt::Debug + Sync,
{
    type Args = Self;
    type Error = anyhow::Error;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(args)
    }

    async fn on_link_died(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        id: ActorId,
        reason: ActorStopReason,
    ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
        match &reason {
            ActorStopReason::Normal => {
                info!("partition worker died normally");
                self.workers.retain(|_, worker| worker.id() != id);
                Ok(ControlFlow::Continue(()))
            }
            _ => {
                error!("partition worker died abnormally - stopping coordinator");
                Ok(ControlFlow::Break(ActorStopReason::LinkDied {
                    id,
                    reason: Box::new(reason),
                }))
            }
        }
    }
}

struct GetStartFrom;

impl<E, H> Message<GetStartFrom> for PostgresProcessor<E, H>
where
    E: 'static,
    H: EventHandler<sqlx::Transaction<'static, Postgres>>
        + CompositeEventHandler<E, sqlx::Transaction<'static, Postgres>, PostgresEventProcessorError>
        + Send
        + 'static,
    <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error: fmt::Debug + Sync,
{
    type Reply = Result<HashMap<u16, u64>, sqlx::Error>;

    async fn handle(
        &mut self,
        _msg: GetStartFrom,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self
            .last_flushed_sequences
            .iter()
            .map(|(partition_id, sequence)| (*partition_id, sequence + 1))
            .collect())
    }
}

struct HandleEvent(Event);

impl<E, H> Message<HandleEvent> for PostgresProcessor<E, H>
where
    E: 'static,
    H: EventHandler<sqlx::Transaction<'static, Postgres>>
        + CompositeEventHandler<E, sqlx::Transaction<'static, Postgres>, PostgresEventProcessorError>
        + Clone
        + Send
        + 'static,
    for<'a> <H as EventHandler<sqlx::Transaction<'a, Postgres>>>::Error:
        fmt::Debug + Unpin + Sync + 'static,
{
    type Reply = ForwardedReply<
        HandleEvent,
        Result<
            (),
            EventHandlerError<
                PostgresEventProcessorError,
                <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error,
            >,
        >,
    >;

    async fn handle(
        &mut self,
        HandleEvent(event): HandleEvent,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let worker_id = event.partition_id % self.worker_count;
        let entry = self.workers.entry(worker_id);
        let worker_ref = match entry {
            Entry::Vacant(vacancy) => {
                let worker_ref = Worker::spawn_link_with_mailbox(
                    ctx.actor_ref(),
                    Worker {
                        pool: self.pool.clone(),
                        conn: self.conn.clone(),
                        checkpoints_table: self.checkpoints_table.clone(),
                        projection_id: self.projection_id.clone(),
                        handler: self.handler.clone(),
                        transaction: None,
                        events_since_flush: 0,
                        last_flushed_sequences: self.last_flushed_sequences.clone(),
                        last_handled_sequences: self.last_flushed_sequences.clone(),
                        partition_latest_sequences: HashMap::new(),
                        last_flushed: Instant::now(),
                        flush_live_interval_time: self.flush_live_interval_time,
                        flush_live_interval_events: self.flush_live_interval_events,
                        flush_replay_interval_time: self.flush_replay_interval_time,
                        flush_replay_interval_events: self.flush_replay_interval_events,
                        is_live: false,
                        phantom: PhantomData,
                    },
                    mailbox::bounded(1024 * 4),
                )
                .await;

                vacancy.insert(worker_ref)
            }
            Entry::Occupied(occupied) => occupied.into_mut(),
        };

        ctx.forward(worker_ref, HandleEvent(event)).await
    }
}

struct Worker<E, H>
where
    E: 'static,
    H: EventHandler<sqlx::Transaction<'static, Postgres>>
        + CompositeEventHandler<E, sqlx::Transaction<'static, Postgres>, PostgresEventProcessorError>
        + Send
        + 'static,
    <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error: fmt::Debug + Sync,
{
    pool: PgPool,
    conn: MultiplexedConnection,
    checkpoints_table: Arc<str>,
    projection_id: Arc<str>,
    handler: H,
    transaction: Option<sqlx::Transaction<'static, Postgres>>,
    events_since_flush: u64,
    last_flushed: Instant,
    last_flushed_sequences: HashMap<u16, u64>,
    last_handled_sequences: HashMap<u16, u64>,
    partition_latest_sequences: HashMap<u16, Option<u64>>,
    flush_live_interval_time: Duration,
    flush_live_interval_events: u64,
    flush_replay_interval_time: Duration,
    flush_replay_interval_events: u64,
    is_live: bool,
    phantom: PhantomData<fn() -> E>,
}

impl<E, H> Worker<E, H>
where
    E: 'static,
    H: EventHandler<sqlx::Transaction<'static, Postgres>>
        + CompositeEventHandler<E, sqlx::Transaction<'static, Postgres>, PostgresEventProcessorError>
        + Send
        + 'static,
    <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error: fmt::Debug + Sync,
{
    async fn handle_event(
        &mut self,
        event: Event,
    ) -> Result<
        (),
        EventHandlerError<
            PostgresEventProcessorError,
            <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error,
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

        match self.partition_latest_sequences.entry(event.partition_id) {
            Entry::Occupied(_) => {}
            Entry::Vacant(entry) => {
                // We came across a partition we haven't handled, switch back to replaying
                self.is_live = false;

                let epseq = self.conn.epseq_by_id(event.partition_id).await?;
                entry.insert(epseq);
            }
        }

        let partition_id = event.partition_id;
        let sequence = event.partition_sequence;

        handle_event
            .retry(ExponentialBuilder::new().with_jitter().with_max_times(5))
            .context((&self.pool, &mut self.transaction, &mut self.handler, &event))
            .notify(|err, _dur| {
                error!("failed to process event: {err:?}");
            })
            .await
            .1?;

        self.last_handled_sequences.insert(partition_id, sequence);
        self.events_since_flush += 1;

        let replaying = !self.is_live
            && self
                .partition_latest_sequences
                .iter()
                .any(|(partition_id, latest_sequence)| {
                    let gap = latest_sequence
                        .map(|latest_sequence| {
                            match self.last_handled_sequences.get(partition_id) {
                                Some(last_handled_sequence) => {
                                    latest_sequence.saturating_sub(*last_handled_sequence)
                                }
                                None => latest_sequence + 1,
                            }
                        })
                        .unwrap_or(0);
                    gap > 0
                });

        let flush_reason = if replaying {
            if sequence.is_multiple_of(100) {
                info!(
                    "{} replay progress: {:.1}% ({}/{})",
                    self.projection_id,
                    self.replay_progress_percent(),
                    self.last_handled_sequences.len(),
                    self.partition_latest_sequences.len()
                );
            }

            // Replay
            (self.events_since_flush >= self.flush_replay_interval_events)
                .then_some(FlushReason::ReplayEventsInterval)
        } else {
            if !self.is_live {
                self.is_live = true;

                info!(
                    "projection {} is now live at {partition_id}:{sequence} for worker",
                    self.projection_id
                );
            }

            // Live
            (self.events_since_flush >= self.flush_live_interval_events)
                .then_some(FlushReason::LiveEventsInterval)
        };

        if let Some(flush_reason) = flush_reason {
            flush_retry
                .retry(ExponentialBuilder::new())
                .context((self, flush_reason))
                .notify(|err, _dur| {
                    error!("failed to flush events: {err:?}");
                })
                .await
                .1?;
        }

        Ok(())
    }

    async fn flush_checkpoint(
        &mut self,
        reason: FlushReason,
    ) -> Result<
        (),
        EventHandlerError<
            PostgresEventProcessorError,
            <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error,
        >,
    > {
        let Some(tx) = self.transaction.as_mut() else {
            self.last_flushed = Instant::now();
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

                    let res = sqlx::query(&format!(
                        "INSERT INTO {} (projection_id, partition_id, sequence) VALUES ($1, $2, $3)",
                        self.checkpoints_table
                    ))
                    .bind(self.projection_id.as_ref())
                    .bind(*partition_id as i32)
                    .bind(*last_handled_sequence as i64)
                    .execute(&mut **tx)
                    .await;

                    match res {
                        Ok(_) => {
                            info!(gauge.projection_sequence = last_handled_sequence, projection_id = %self.projection_id, partition_id, database = "postgres");
                        }
                        Err(sqlx::Error::Database(db_err))
                            if db_err.code().as_deref() == Some("23505") =>
                        {
                            // 23505 is the error code for unique violations (e.g., primary key conflicts)
                            return Err(EventHandlerError::Processor(
                                PostgresEventProcessorError::UnexpectedLastEventId {
                                    expected: None,
                                },
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

                    let res = sqlx::query(&format!(
                        "
                        UPDATE {} SET sequence = $1
                        WHERE projection_id = $2 AND partition_id = $3
                    ",
                        self.checkpoints_table
                    ))
                    .bind(*last_handled_sequence as i64)
                    .bind(self.projection_id.as_ref())
                    .bind(*partition_id as i32)
                    .execute(&mut **tx)
                    .await?;
                    if res.rows_affected() == 0 {
                        return Err(EventHandlerError::Processor(
                            PostgresEventProcessorError::UnexpectedLastEventId {
                                expected: Some(*last_handled_sequence),
                            },
                        ));
                    }

                    info!(gauge.projection_sequence = last_handled_sequence, projection_id = %self.projection_id, partition_id, database = "postgres");
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
        self.events_since_flush = 0;
        self.last_flushed = Instant::now();

        Ok(())
    }

    fn replay_progress_percent(&self) -> f64 {
        let mut total_events = 0u64;
        let mut processed_events = 0u64;

        for (partition_id, latest_sequence) in &self.partition_latest_sequences {
            // Latest sequence is 0-indexed, so +1 for total count
            if let Some(latest) = latest_sequence {
                total_events += latest + 1;

                // Get how many we've processed in this partition
                if let Some(last_handled) = self.last_handled_sequences.get(partition_id) {
                    processed_events += last_handled + 1;
                }
                // If not in last_handled yet, we've processed 0 events for this partition
            }
        }

        if total_events == 0 {
            return 100.0; // No events to process
        }

        (processed_events as f64 / total_events as f64) * 100.0
    }
}

async fn handle_event<'a, E, H>(
    (pool, transaction, handler, event): (
        &'a PgPool,
        &'a mut Option<sqlx::Transaction<'static, Postgres>>,
        &'a mut H,
        &'a Event,
    ),
) -> (
    (
        &'a PgPool,
        &'a mut Option<sqlx::Transaction<'static, Postgres>>,
        &'a mut H,
        &'a Event,
    ),
    Result<
        (),
        EventHandlerError<
            PostgresEventProcessorError,
            <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error,
        >,
    >,
)
where
    E: 'static,
    H: EventHandler<sqlx::Transaction<'static, Postgres>>
        + CompositeEventHandler<E, sqlx::Transaction<'static, Postgres>, PostgresEventProcessorError>
        + Send
        + 'static,
{
    let mut tx = match transaction.take() {
        Some(tx) => tx,
        None => {
            let tx_res = pool.begin().await;
            match tx_res {
                Ok(tx) => tx,
                Err(err) => return ((pool, transaction, handler, event), Err(err.into())),
            }
        }
    };

    let res = handler.composite_handle(&mut tx, event.clone()).await;
    if res.is_err() {
        let _ = tx.rollback().await;
        *transaction = None;
        return ((pool, transaction, handler, event), res);
    }

    *transaction = Some(tx);
    ((pool, transaction, handler, event), Ok(()))
}

async fn flush_retry<'w, E, H>(
    (worker, reason): (&'w mut Worker<E, H>, FlushReason),
) -> (
    (&'w mut Worker<E, H>, FlushReason),
    Result<
        (),
        EventHandlerError<
            PostgresEventProcessorError,
            <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error,
        >,
    >,
)
where
    E: 'static,
    H: EventHandler<sqlx::Transaction<'static, Postgres>>
        + CompositeEventHandler<E, sqlx::Transaction<'static, Postgres>, PostgresEventProcessorError>
        + Send
        + 'static,
    <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error: fmt::Debug + Sync,
{
    let res = worker.flush_checkpoint(reason).await;
    ((worker, reason), res)
}

impl<E, H> Actor for Worker<E, H>
where
    E: 'static,
    H: EventHandler<sqlx::Transaction<'static, Postgres>>
        + CompositeEventHandler<E, sqlx::Transaction<'static, Postgres>, PostgresEventProcessorError>
        + Send
        + 'static,
    <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error: fmt::Debug + Sync,
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
    ) -> Option<Signal<Self>> {
        let last_flush_duration = self.last_flushed.elapsed();
        let flush_sleep_duration = if self.is_live {
            self.flush_live_interval_time
                .checked_sub(last_flush_duration)
        } else {
            self.flush_replay_interval_time
                .checked_sub(last_flush_duration)
        };
        let flush_sleep = OptionFuture::from(flush_sleep_duration.map(tokio::time::sleep));

        tokio::select! {
            msg = mailbox_rx.recv() => return msg,
            _ = flush_sleep => {
                flush_retry
                    .retry(ExponentialBuilder::new())
                    .context((self, FlushReason::TimeInterval))
                    .notify(|err, _dur| {
                        error!("failed to flush events: {err:?}");
                    })
                    .await
                    .1.unwrap();
            }
        }

        mailbox_rx.recv().await
    }
}

impl<E, H> Message<HandleEvent> for Worker<E, H>
where
    E: 'static,
    H: EventHandler<sqlx::Transaction<'static, Postgres>>
        + CompositeEventHandler<E, sqlx::Transaction<'static, Postgres>, PostgresEventProcessorError>
        + Send
        + 'static,
    <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error: fmt::Debug + Sync,
{
    type Reply = Result<
        (),
        EventHandlerError<
            PostgresEventProcessorError,
            <H as EventHandler<sqlx::Transaction<'static, Postgres>>>::Error,
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
pub enum PostgresEventProcessorError {
    #[error(transparent)]
    GetStartFrom(#[from] SendError<(), sqlx::Error>),
    #[error(transparent)]
    Postgres(#[from] sqlx::Error),
    #[error("unexpected last event id, expected {expected:?}")]
    UnexpectedLastEventId { expected: Option<u64> },
}

impl<H> From<sqlx::Error> for EventHandlerError<PostgresEventProcessorError, H> {
    fn from(err: sqlx::Error) -> Self {
        EventHandlerError::Processor(PostgresEventProcessorError::Postgres(err))
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FlushReason {
    TimeInterval,
    LiveEventsInterval,
    ReplayEventsInterval,
}
