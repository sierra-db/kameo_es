use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use futures::FutureExt;
use kameo::{
    actor::ActorRef,
    error::{Infallible, SendError},
    message::Message,
    Actor,
};

use crate::{command_service::CommandService, event_store::AppendEvents, GenericValue, StreamID};

pub enum TransactionOutcome<C = (), A = ()> {
    Commit(C),
    Abort(A),
}

pub struct Transaction<'a> {
    id: usize,
    pub(crate) cmd_service: &'a CommandService,
    entities: &'a mut HashMap<StreamID, Box<dyn EntityTransaction>>,
    appends: &'a mut Vec<AppendEvents<(&'static str, GenericValue), GenericValue>>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(
        id: usize,
        cmd_service: &'a CommandService,
        entities: &'a mut HashMap<StreamID, Box<dyn EntityTransaction>>,
        appends: &'a mut Vec<AppendEvents<(&'static str, GenericValue), GenericValue>>,
    ) -> Self {
        Transaction {
            id,
            cmd_service,
            entities,
            appends,
        }
    }

    pub(crate) fn id(&self) -> usize {
        self.id
    }

    pub(crate) fn is_registered(&mut self, stream_id: &StreamID) -> bool {
        self.entities.contains_key(&stream_id)
    }

    pub(crate) fn register_entity(
        &mut self,
        stream_id: StreamID,
        entity_actor_ref: Box<dyn EntityTransaction>,
    ) {
        self.entities.entry(stream_id).or_insert(entity_actor_ref);
    }

    pub(crate) fn append(
        &mut self,
        append: AppendEvents<(&'static str, GenericValue), GenericValue>,
    ) {
        self.appends.push(append);
    }

    pub(crate) fn committed(self) {
        for (_, entity) in self.entities {
            let _ = entity.commit_transaction(self.id);
        }
    }

    pub(crate) fn reset(self) {
        for (_, entity) in &*self.entities {
            let _ = entity.reset_transaction(self.id);
        }
        // self.entities.clear();
        self.appends.clear();
    }

    pub(crate) fn abort(self) {
        for (_, entity) in self.entities {
            let _ = entity.abort_transaction(self.id);
        }
    }

    pub(crate) fn get_id() -> usize {
        static COUNTER: AtomicUsize = AtomicUsize::new(1);
        COUNTER.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub(crate) struct BeginTransaction {
    pub(crate) tx_id: usize,
    pub(crate) is_buffered: bool,
}

#[derive(Debug)]
pub(crate) struct CommitTransaction {
    pub(crate) tx_id: usize,
}

#[derive(Debug)]
pub(crate) struct ResetTransaction {
    pub(crate) tx_id: usize,
}

#[derive(Debug)]
pub(crate) struct AbortTransaction {
    pub(crate) tx_id: usize,
}

pub(crate) trait EntityTransaction: Send + 'static {
    fn commit_transaction(
        &self,
        tx_id: usize,
    ) -> Result<(), SendError<CommitTransaction, Infallible>>;
    fn reset_transaction(
        &self,
        tx_id: usize,
    ) -> Result<(), SendError<ResetTransaction, Infallible>>;
    fn abort_transaction(
        &self,
        tx_id: usize,
    ) -> Result<(), SendError<AbortTransaction, Infallible>>;
}

impl<A> EntityTransaction for ActorRef<A>
where
    A: Actor
        + Message<CommitTransaction, Reply = ()>
        + Message<ResetTransaction, Reply = anyhow::Result<()>>
        + Message<AbortTransaction, Reply = ()>,
{
    fn commit_transaction(
        &self,
        tx_id: usize,
    ) -> Result<(), SendError<CommitTransaction, Infallible>> {
        self.tell(CommitTransaction { tx_id })
            .send()
            .now_or_never()
            .unwrap()
    }

    fn reset_transaction(
        &self,
        tx_id: usize,
    ) -> Result<(), SendError<ResetTransaction, Infallible>> {
        self.tell(ResetTransaction { tx_id })
            .send()
            .now_or_never()
            .unwrap()
    }

    fn abort_transaction(
        &self,
        tx_id: usize,
    ) -> Result<(), SendError<AbortTransaction, Infallible>> {
        self.tell(AbortTransaction { tx_id })
            .send()
            .now_or_never()
            .unwrap()
    }
}
