pub mod command_service;
pub mod entity_actor;
pub mod error;
pub mod event_handler;
pub mod transaction;

use std::{convert::Infallible, io};

use kameo::error::SendError;
use sierradb_client::SierraError;
use thiserror::Error;

pub use kameo_es_core::*;

#[derive(Debug, Error)]
pub enum Error<M = (), E = Infallible> {
    #[error(transparent)]
    Database(#[from] SierraError),
    #[error(transparent)]
    SendError(#[from] SendError<M, E>),
}

#[derive(Debug, Error)]
pub enum TryFromSierraEventError {
    #[error("failed to deserialize event data: {0}")]
    DeserializeEventData(ciborium::de::Error<io::Error>),
    #[error("failed to deserialize event metadata: {0}")]
    DeserializeEventMetadata(ciborium::de::Error<io::Error>),
}

fn event_from_sierra(ev: sierradb_client::Event) -> Result<Event, TryFromSierraEventError> {
    let data = if !ev.payload.is_empty() {
        ciborium::from_reader(ev.payload.as_slice())
            .map_err(TryFromSierraEventError::DeserializeEventData)?
    } else {
        GenericValue(ciborium::Value::Null)
    };
    let metadata = if !ev.metadata.is_empty() {
        ciborium::from_reader(ev.metadata.as_slice())
            .map_err(TryFromSierraEventError::DeserializeEventMetadata)?
    } else {
        Metadata::default()
    };

    Ok(Event {
        id: ev.event_id,
        partition_key: ev.partition_key,
        partition_id: ev.partition_id,
        transaction_id: ev.transaction_id,
        partition_sequence: ev.partition_sequence,
        stream_version: ev.stream_version,
        stream_id: StreamId::new(ev.stream_id),
        name: ev.event_name,
        data,
        metadata,
        timestamp: ev.timestamp.into(),
    })
}

/// Matches on an event for multiple branches, with each branch being an entity type.
///
/// The passed in event will be converted to Event<E> where E is the Entity for the given branch.
///
/// # Example
///
/// ```
/// match_event! {
///     event,
///     BankAccount => {
///         let id: BankAccount::ID = event.entity_id()?;
///         let data = BankAccount::Event = event.data()?;
///         // ...
///     }
///     else => println!("unknown entity"),
/// }
/// ```
#[macro_export]
macro_rules! match_event {
    // All input is normalized, now transform.
    (@ {
        event=$event:ident;

        $( $ent:ident => $handle:expr, )+

        // Fallback expression used when all select branches have been disabled.
        ; $else:expr

    }) => {{
        let category = $event.stream_id.category();
        $(
            if category == $ent::category() {
                let $event = $event.as_entity::<$ent>();
                {
                    let _res = $handle;
                }
            } else
        )*
        if true {
            $else
        }
    }};

    // ==== Normalize =====

    // These rules match a single `select!` branch and normalize it for
    // processing by the first rule.

    (@ { event=$event:ident; $($t:tt)* } ) => {
        // No `else` branch
        $crate::match_event!(@{ event=$event; $($t)*; {} })
    };
    (@ { event=$event:ident; $($t:tt)* } else => $else:expr $(,)?) => {
        $crate::match_event!(@{ event=$event; $($t)*; $else })
    };
    (@ { event=$event:ident; $($t:tt)* } $ent:ident => $h:block, $($r:tt)* ) => {
        $crate::match_event!(@{ event=$event; $($t)* $ent => $h, } $($r)*)
    };
    (@ { event=$event:ident; $($t:tt)* } $ent:ident => $h:block $($r:tt)* ) => {
        $crate::match_event!(@{ event=$event; $($t)* $ent => $h, } $($r)*)
    };
    (@ { event=$event:ident; $($t:tt)* } $ent:ident => $h:expr ) => {
        $crate::match_event!(@{ event=$event; $($t)* $ent => $h, })
    };
    (@ { event=$event:ident; $($t:tt)* } $ent:ident => $h:expr, $($r:tt)* ) => {
        $crate::match_event!(@{ event=$event; $($t)* $ent => $h, } $($r)*)
    };

    // ===== Entry point =====

    ( $event:ident, else => $else:expr $(,)? ) => {{
        $else
    }};

    ( $event:ident, $ent:ident => $($t:tt)* ) => {
        // Randomly generate a starting point. This makes `select!` a bit more
        // fair and avoids always polling the first future.
        $crate::match_event!(@{ event=$event; } $ent => $($t)*)
    };

    () => {
        compile_error!("select! requires at least one branch.")
    };
}
