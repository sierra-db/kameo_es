use std::{
    collections::HashMap,
    io::{self, SeekFrom},
    marker::PhantomData,
    path::Path,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
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

pub struct CborSerializer;
pub struct JsonSerializer;

pub trait FileSerializer: Send {
    type DeError: Send;
    type SerError: Send;

    fn from_slice<T>(slice: &[u8]) -> Result<T, Self::DeError>
    where
        T: DeserializeOwned;

    fn to_vec<T>(value: &T) -> Result<Vec<u8>, Self::SerError>
    where
        T: Serialize;
}

impl FileSerializer for CborSerializer {
    type DeError = ciborium::de::Error<std::io::Error>;
    type SerError = ciborium::ser::Error<std::io::Error>;

    fn from_slice<T>(slice: &[u8]) -> Result<T, Self::DeError>
    where
        T: DeserializeOwned,
    {
        ciborium::from_reader(slice)
    }

    fn to_vec<T>(value: &T) -> Result<Vec<u8>, Self::SerError>
    where
        T: Serialize,
    {
        let mut buf = Vec::new();
        ciborium::into_writer(value, &mut buf)?;
        Ok(buf)
    }
}

impl FileSerializer for JsonSerializer {
    type DeError = serde_json::Error;
    type SerError = serde_json::Error;

    fn from_slice<T>(slice: &[u8]) -> Result<T, Self::DeError>
    where
        T: DeserializeOwned,
    {
        serde_json::from_slice(slice)
    }

    fn to_vec<T>(value: &T) -> Result<Vec<u8>, Self::SerError>
    where
        T: Serialize,
    {
        serde_json::to_vec_pretty(value)
    }
}

pub struct FileEventProcessor<H: Hydrate, S: FileSerializer> {
    file: File,
    handler: H,
    flush_interval: u64,
    last_partition_sequences: HashMap<u16, u64>,
    events_since_flush: u64,
    phantom: PhantomData<S>,
}

pub trait Hydrate {
    type OwnedState: DeserializeOwned + Send + Sync;
    type BorrowedState<'s>: Serialize + Send + Sync
    where
        Self: 's;

    fn hydrate(&mut self, state: Self::OwnedState);
    fn state<'s>(&'s self) -> Self::BorrowedState<'s>;
}

#[derive(Default, Serialize, Deserialize)]
struct SavedData<S, T> {
    last_partition_sequences: S,
    data: T,
}

impl<T> SavedData<HashMap<u16, u64>, T> {
    async fn load<S>(
        file: &mut File,
    ) -> Result<Option<Self>, FileEventProcessorError<S::DeError, S::SerError>>
    where
        T: DeserializeOwned,
        S: FileSerializer,
    {
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;
        if buf.is_empty() {
            return Ok(None);
        }

        S::from_slice(buf.as_slice()).map_err(FileEventProcessorError::DeserializeState)
    }
}

impl<'a, T> SavedData<&'a HashMap<u16, u64>, T> {
    async fn save<S>(
        &'a self,
        file: &mut File,
    ) -> Result<(), FileEventProcessorError<S::DeError, S::SerError>>
    where
        T: Serialize + 'a,
        S: FileSerializer,
    {
        let bytes = S::to_vec(self).map_err(FileEventProcessorError::SerializeState)?;

        file.set_len(0).await?;
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        file.sync_all().await?;

        Ok(())
    }
}

impl<H: Hydrate, S: FileSerializer> FileEventProcessor<H, S> {
    pub async fn new(
        path: impl AsRef<Path>,
        mut handler: H,
    ) -> Result<Self, FileEventProcessorError<S::DeError, S::SerError>>
    where
        H: Default + DeserializeOwned,
    {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await?;

        let last_partition_sequences = match SavedData::load::<S>(&mut file).await? {
            Some(SavedData {
                last_partition_sequences,
                data,
            }) => {
                handler.hydrate(data);
                last_partition_sequences
            }
            None => HashMap::new(),
        };

        Ok(FileEventProcessor {
            file,
            handler,
            flush_interval: 20,
            last_partition_sequences,
            events_since_flush: 0,
            phantom: PhantomData,
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

    pub async fn flush(&mut self) -> Result<(), FileEventProcessorError<S::DeError, S::SerError>> {
        info!("flushing");

        SavedData {
            last_partition_sequences: &self.last_partition_sequences,
            data: self.handler.state(),
        }
        .save::<S>(&mut self.file)
        .await?;

        self.events_since_flush = 0;

        Ok(())
    }

    pub async fn flush_if_needed(
        &mut self,
    ) -> Result<(), FileEventProcessorError<S::DeError, S::SerError>> {
        if self.last_partition_sequences.is_empty() {
            return Ok(());
        }

        if self.events_since_flush < self.flush_interval {
            return Ok(());
        }

        self.flush().await
    }
}

impl<E, H, S> EventProcessor<E, H> for FileEventProcessor<H, S>
where
    H: EventHandler<()>
        + CompositeEventHandler<E, (), FileEventProcessorError<S::DeError, S::SerError>>
        + Hydrate
        + 'static,
    S: FileSerializer,
{
    type Context = ();
    type Error = FileEventProcessorError<S::DeError, S::SerError>;

    async fn start_from(&self) -> Result<HashMap<u16, u64>, Self::Error> {
        Ok(self
            .last_partition_sequences
            .iter()
            .map(|(partition_id, sequence)| (*partition_id, sequence + 1))
            .collect())
    }

    async fn process_event(
        &mut self,
        event: Event,
    ) -> Result<(), EventHandlerError<Self::Error, <H as EventHandler<()>>::Error>> {
        let partition_id = event.partition_id;
        let partition_sequence = event.partition_sequence;

        self.handler.composite_handle(&mut (), event).await?;

        self.last_partition_sequences
            .insert(partition_id, partition_sequence);
        self.events_since_flush += 1;
        self.flush_if_needed()
            .await
            .map_err(EventHandlerError::Processor)?;

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum FileEventProcessorError<D, S> {
    #[error("failed to deserialize last event id")]
    DeserializeLastEventId,
    #[error(transparent)]
    DeserializeState(D),
    #[error(transparent)]
    SerializeState(S),
    #[error(transparent)]
    Io(#[from] io::Error),
}
