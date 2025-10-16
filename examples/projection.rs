use std::collections::HashMap;

use kameo_es::{
    command_service::{CommandService, ExecuteExt},
    event_handler::{
        in_memory::InMemoryEventProcessor, EntityEventHandler, EventHandler,
        EventHandlerStreamBuilder,
    },
    Apply, Command, CommandName, Entity, Event, EventType, Metadata,
};
use redis::Value;
use serde::{Deserialize, Serialize};
use sierradb_client::SierraAsyncClientExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = redis::Client::open("redis://127.0.0.1:9090?protocol=resp3")?;
    let mut conn = client.get_multiplexed_tokio_connection().await?;
    let cmd_service = CommandService::new(conn.clone());
    let mut subscriptions = client.subscription_manager().await?;

    MyEntity::execute(&cmd_service, "abc".to_string(), MyEntityEvent::Foo {}).await?;

    let mut processor = InMemoryEventProcessor::new(EventKindCounter::default());

    let mut stream =
        <(MyEntity,)>::event_handler_stream(&mut subscriptions, &mut processor).await?;
    stream.run(&mut processor).await?;

    Ok(())
}

// === Entity & Events ===

#[derive(Clone, Debug, Default)]
pub struct MyEntity;

impl Entity for MyEntity {
    type ID = String;
    type Event = MyEntityEvent;
    type Metadata = ();

    fn category() -> &'static str {
        "my_entity"
    }
}

#[derive(Clone, Debug, CommandName, Serialize, Deserialize)]
pub enum MyEntityEvent {
    Foo {},
    Bar {},
    Baz {},
}

impl EventType for MyEntityEvent {
    fn event_type(&self) -> &'static str {
        match self {
            MyEntityEvent::Foo {} => "Foo",
            MyEntityEvent::Bar {} => "Bar",
            MyEntityEvent::Baz {} => "Baz",
        }
    }
}

impl Apply for MyEntity {
    fn apply(&mut self, _event: Self::Event, _metadata: Metadata<Self::Metadata>) {}
}

impl Command<MyEntityEvent> for MyEntity {
    type Error = String;

    fn handle(
        &self,
        cmd: MyEntityEvent,
        _ctx: kameo_es::Context<'_, Self>,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        println!("runnning");
        Ok(vec![cmd])
    }
}

// === Event Handler ===

#[derive(Default)]
pub struct EventKindCounter {
    events: HashMap<String, u32>,
}

impl EventHandler<()> for EventKindCounter {
    type Error = anyhow::Error;
}

impl EntityEventHandler<MyEntity, ()> for EventKindCounter {
    async fn handle(
        &mut self,
        _ctx: &mut (),
        _id: String,
        event: Event<MyEntityEvent, ()>,
    ) -> Result<(), Self::Error> {
        // Increment the counter for this event name
        *self.events.entry(event.name).or_default() += 1;

        // We'll print the event counts for debugging purposes
        let output = self
            .events
            .iter()
            .map(|(event_name, count)| format!("{event_name} = {count}"))
            .collect::<Vec<_>>()
            .join(", ");
        println!("{output}");

        Ok(())
    }
}
