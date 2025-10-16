use std::{collections::HashMap, fmt, time::Instant};

use chrono::Utc;

use crate::{Apply, Command, CommandName, Entity, Metadata};

pub trait GivenEntity: Entity + Apply {
    fn given(events: impl Into<Vec<Self::Event>>) -> Given<Self>;
}

impl<E> GivenEntity for E
where
    E: Entity + Apply,
{
    fn given(events: impl Into<Vec<Self::Event>>) -> Given<Self> {
        Given {
            entity: Self::default(),
            events: events.into(),
        }
    }
}

pub struct Given<E>
where
    E: Entity,
{
    entity: E,
    events: Vec<E::Event>,
}

impl<E> Given<E>
where
    E: Entity + Apply,
{
    pub fn when<C>(mut self, cmd: C) -> When<E, C>
    where
        E: Entity + Command<C> + Apply,
        C: CommandName,
    {
        for event in self.events {
            self.entity.apply(event, Metadata::default());
        }

        When {
            entity: self.entity,
            cmd,
        }
    }
}

pub struct When<E, C>
where
    E: Entity + Command<C>,
    C: CommandName,
{
    entity: E,
    cmd: C,
}

impl<E, C> When<E, C>
where
    E: Entity + Command<C>,
    C: CommandName,
{
    pub fn then(self, events: impl Into<Vec<E::Event>>) -> Given<E>
    where
        E::Event: fmt::Debug + PartialEq<E::Event>,
    {
        let metadata = Metadata::default();
        let ctx = crate::Context {
            metadata: &metadata,
            causation_tracking: &HashMap::new(),
            time: Utc::now(),
            executed_at: Instant::now(),
        };
        let is_idempotent = self.entity.is_idempotent(&self.cmd, ctx);
        if is_idempotent {
            return Given {
                entity: self.entity,
                events: vec![],
            };
        }
        let true_events = self
            .entity
            .handle(self.cmd, ctx)
            .expect("expected command to succeed");
        let expected_events: Vec<_> = events.into();
        assert_eq!(&true_events, &expected_events, "wrong events returned");

        Given {
            entity: self.entity,
            events: true_events,
        }
    }

    pub fn then_error(self, err: <E as Command<C>>::Error) -> Given<E>
    where
        E::Event: fmt::Debug,
        E::Error: fmt::Debug + PartialEq<E::Error>,
    {
        let metadata = Metadata::default();
        let ctx = crate::Context {
            metadata: &metadata,
            causation_tracking: &HashMap::new(),
            time: Utc::now(),
            executed_at: Instant::now(),
        };
        let true_err = self
            .entity
            .handle(self.cmd, ctx)
            .expect_err("expected command to return an error");
        assert_eq!(true_err, err);

        Given {
            entity: self.entity,
            events: Vec::new(),
        }
    }

    pub fn then_idempotent(self) -> Given<E> {
        let metadata = Metadata::default();
        let ctx = crate::Context {
            metadata: &metadata,
            causation_tracking: &HashMap::new(),
            time: Utc::now(),
            executed_at: Instant::now(),
        };
        let is_idempotent = self.entity.is_idempotent(&self.cmd, ctx);
        assert!(is_idempotent, "expected command to be idempotent");

        Given {
            entity: self.entity,
            events: Vec::new(),
        }
    }

    pub fn and_then<F>(self, f: F) -> Given<E>
    where
        F: FnOnce(Vec<E::Event>),
    {
        let metadata = Metadata::default();
        let ctx = crate::Context {
            metadata: &metadata,
            causation_tracking: &HashMap::new(),
            time: Utc::now(),
            executed_at: Instant::now(),
        };
        let events = self
            .entity
            .handle(self.cmd, ctx)
            .expect("expected command to succeed");
        f(events.clone());

        Given {
            entity: self.entity,
            events,
        }
    }
}
