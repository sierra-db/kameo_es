use std::{
    collections::HashMap,
    io::{self, SeekFrom},
    path::Path,
};

use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use tracing::info;

use crate::{
    event_handler::{CompositeEventHandler, EventHandler, EventHandlerError, EventProcessor},
    Event,
};

pub struct FileEventProcessor<H> {
    file: File,
    handler: H,
    flush_interval: u64,
    last_event_sequence: HashMap<u16, u64>,
    last_flush: Option<u64>,
}

impl<H> FileEventProcessor<H> {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self, FileEventProcessorError>
    where
        H: Default + DeserializeOwned,
    {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .await?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;
        let (handler, last_event_sequence) = if !buf.is_empty() {
            let last_event_id = u64::from_le_bytes(
                buf[..8]
                    .try_into()
                    .map_err(|_| FileEventProcessorError::DeserializeLastEventId)?,
            );
            let bytes = buf.split_off(8);
            let handler = ciborium::from_reader(bytes.as_slice())?;
            (handler, Some(last_event_id))
        } else {
            (H::default(), None)
        };

        Ok(FileEventProcessor {
            file,
            handler,
            flush_interval: 20,
            last_event_sequence,
            last_flush: None,
        })
    }

    pub fn handler(&mut self) -> &mut H {
        &mut self.handler
    }

    pub fn into_handler(self) -> H {
        self.handler
    }

    pub fn flush_interval(mut self, events: u64) -> Self {
        self.flush_interval = events;
        self
    }

    pub async fn flush(&mut self) -> Result<(), FileEventProcessorError>
    where
        H: Serialize,
    {
        let Some(last_event_id) = self.last_event_sequence else {
            return Ok(());
        };
        if Some(last_event_id) <= self.last_flush {
            return Ok(());
        }
        let events_since_last_flush = self
            .last_flush
            .map(|last_flush| last_event_id - last_flush)
            .unwrap_or(u64::MAX);
        if events_since_last_flush < self.flush_interval {
            return Ok(());
        }

        info!("flushing at {last_event_id}");

        let mut state_bytes = Vec::new();
        ciborium::into_writer(&self.handler, &mut state_bytes)?;

        let mut bytes = Vec::with_capacity(8 + state_bytes.len());
        bytes.extend_from_slice(&last_event_id.to_le_bytes());
        bytes.extend_from_slice(&state_bytes);

        self.file.set_len(0).await?;
        self.file.seek(SeekFrom::Start(0)).await?;
        self.file.write_all(&bytes).await?;
        self.file.flush().await?;
        self.file.sync_all().await?;

        self.last_flush = Some(last_event_id);

        Ok(())
    }
}

impl<E, H> EventProcessor<E, H> for FileEventProcessor<H>
where
    H: EventHandler<()> + CompositeEventHandler<E, (), FileEventProcessorError> + Serialize,
{
    type Context = ();
    type Error = FileEventProcessorError;

    async fn start_from(&self) -> Result<HashMap<u16, u64>, Self::Error> {
        Ok(self.last_event_sequence.map(|n| n + 1).unwrap_or(0))
    }

    async fn process_event(
        &mut self,
        event: Event,
    ) -> Result<(), EventHandlerError<Self::Error, <H as EventHandler<()>>::Error>> {
        let sequence = event.partition_sequence;

        self.handler.composite_handle(&mut (), event).await?;

        self.last_event_sequence = Some(sequence);
        self.flush().await.map_err(EventHandlerError::Processor)?;

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum FileEventProcessorError {
    #[error("failed to deserialize last event id")]
    DeserializeLastEventId,
    #[error(transparent)]
    DeserializeState(#[from] ciborium::de::Error<io::Error>),
    #[error(transparent)]
    SerializeState(#[from] ciborium::ser::Error<io::Error>),
    #[error(transparent)]
    Io(#[from] io::Error),
}
