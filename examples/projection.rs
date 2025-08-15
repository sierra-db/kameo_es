use std::collections::HashMap;

use kameo_es::{
    command_service::{CommandService, ExecuteExt},
    event_handler::{
        in_memory::InMemoryEventProcessor, EntityEventHandler, EventHandler,
        EventHandlerStreamBuilder,
    },
    Apply, Command, Entity, Event, EventType, Metadata,
};
use redis::Value;
use serde::{Deserialize, Serialize};
use sierradb_client::{AsyncTypedCommands, SierraAsyncClientExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = redis::Client::open("redis://127.0.0.1:9090?protocol=resp3")?;

    let mut conn = client.get_multiplexed_tokio_connection().await?;
    let mut cmd_service = CommandService::new(conn.clone());
    MyEntity::execute(&cmd_service, "abc".to_string(), MyEntityEvent::Foo {}).await?;

    let resp: Value = redis::cmd("HELLO").arg("3").query_async(&mut conn).await?;
    dbg!(resp);

    let mut manager = client.subscription_manager().await?;

    let mut processor = InMemoryEventProcessor::new(EventKindCounter::default());

    let mut stream = <(MyEntity,)>::event_handler_stream(&mut manager, &mut processor).await?;
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

    fn name() -> &'static str {
        "my_entity"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
    fn apply(&mut self, event: Self::Event, metadata: Metadata<Self::Metadata>) {}
}

impl Command<MyEntityEvent> for MyEntity {
    type Error = String;

    fn handle(
        &self,
        cmd: MyEntityEvent,
        ctx: kameo_es::Context<'_, Self>,
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
