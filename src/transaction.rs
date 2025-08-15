use std::{any, collections::HashMap};

use futures::FutureExt;
use kameo::{
    actor::ActorRef,
    error::{Infallible, SendError},
    message::Message,
    Actor,
};
use kameo_es_core::{Apply, Entity};
use sierradb_client::EMAppendEvent;
use uuid::Uuid;

use crate::{command_service::CommandService, entity_actor::EntityTransactionActor, StreamId};

pub enum TransactionOutcome<C = (), A = ()> {
    Commit(C),
    Abort(A),
}

pub struct Transaction<'a> {
    pub(crate) cmd_service: &'a CommandService,
    pub(crate) partition_key: Uuid,
    entities: &'a mut HashMap<StreamId, Box<dyn EntityTransaction>>,
    appends: &'a mut Vec<EMAppendEvent<'static>>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(
        cmd_service: &'a CommandService,
        partition_key: Uuid,
        entities: &'a mut HashMap<StreamId, Box<dyn EntityTransaction>>,
        appends: &'a mut Vec<EMAppendEvent<'static>>,
    ) -> Self {
        Transaction {
            cmd_service,
            partition_key,
            entities,
            appends,
        }
    }

    pub(crate) fn is_registered(&mut self, stream_id: &StreamId) -> bool {
        self.entities.contains_key(stream_id)
    }

    pub(crate) fn register_entity(
        &mut self,
        stream_id: StreamId,
        entity_actor_ref: Box<dyn EntityTransaction>,
    ) {
        self.entities.entry(stream_id).or_insert(entity_actor_ref);
    }

    pub(crate) fn lookup_entity<E: Entity + Apply>(
        &self,
        stream_id: &str,
    ) -> Option<ActorRef<EntityTransactionActor<E>>> {
        self.entities
            .get(stream_id)
            .and_then(|ent| ent.as_any().downcast().map(|a| *a).ok())
    }

    pub(crate) fn append(&mut self, append: EMAppendEvent<'static>) {
        self.appends.push(append);
    }

    pub(crate) fn committed(self) {
        for (_, entity) in self.entities {
            let _ = entity.commit_transaction();
        }
    }

    pub(crate) fn reset(self) {
        for (_, entity) in &*self.entities {
            let _ = entity.reset_transaction();
        }
        // self.entities.clear();
        self.appends.clear();
    }

    pub(crate) fn abort(self) {
        for (_, entity) in self.entities {
            let _ = entity.abort_transaction();
        }
    }
}

#[derive(Debug)]
pub(crate) struct BeginTransaction;

#[derive(Debug)]
pub(crate) struct CommitTransaction;

#[derive(Debug)]
pub(crate) struct ResetTransaction;

#[derive(Debug)]
pub(crate) struct AbortTransaction;

pub(crate) trait EntityTransaction: Send + 'static {
    fn as_any(&self) -> Box<dyn any::Any>;
    fn commit_transaction(&self) -> Result<(), SendError<CommitTransaction, Infallible>>;
    fn reset_transaction(&self) -> Result<(), SendError<ResetTransaction, Infallible>>;
    fn abort_transaction(&self) -> Result<(), SendError<AbortTransaction, Infallible>>;
}

impl<A> EntityTransaction for ActorRef<A>
where
    A: Actor
        + Message<CommitTransaction, Reply = ()>
        + Message<ResetTransaction, Reply = anyhow::Result<()>>
        + Message<AbortTransaction, Reply = ()>,
{
    fn as_any(&self) -> Box<dyn any::Any> {
        Box::new(self.clone())
    }

    fn commit_transaction(&self) -> Result<(), SendError<CommitTransaction, Infallible>> {
        self.tell(CommitTransaction).send().now_or_never().unwrap()
    }

    fn reset_transaction(&self) -> Result<(), SendError<ResetTransaction, Infallible>> {
        self.tell(ResetTransaction).send().now_or_never().unwrap()
    }

    fn abort_transaction(&self) -> Result<(), SendError<AbortTransaction, Infallible>> {
        self.tell(AbortTransaction).send().now_or_never().unwrap()
    }
}

// #[derive(Debug)]
// pub struct AppendEvents<E, M> {
//     pub stream_id: StreamId,
//     pub events: Vec<E>,
//     pub expected_version: ExpectedVersion,
//     pub metadata: M,
//     pub timestamp: DateTime<Utc>,
// }
