# kameo_es

A Rust event sourcing library built on top of [Kameo](https://github.com/tqwewe/kameo) actor framework and [SierraDB](https://github.com/sierra-db/sierradb) for event storage. This library provides a simple and type-safe way to build event sourcing aggregates and entities with support for projections and read models.

## Features

- **Event Sourcing**: Build aggregates using the Command-Query Responsibility Segregation (CQRS) pattern
- **Actor-based**: Built on Kameo for concurrent and fault-tolerant entity management
- **SierraDB Integration**: Uses SierraDB as the underlying event store
- **Multiple Projection Backends**: Support for in-memory, file, SQLite, PostgreSQL, and MongoDB backends for storing checkpoints and read models
- **Type Safety**: Derive macros for events and commands with compile-time guarantees
- **Causation Tracking**: Built-in idempotency and causation tracking
- **Rate Limiting**: Optional rate limiting for commands
- **Transactions**: Support for transactional command execution

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
kameo_es = "0.1.0"

# Optional features for projection backends
kameo_es = { version = "0.1.0", features = ["postgres"] }
kameo_es = { version = "0.1.0", features = ["mongodb"] }
kameo_es = { version = "0.1.0", features = ["sqlite"] }
```

Available feature flags:
- `postgres` - PostgreSQL projection backend
- `mongodb` - MongoDB projection backend  
- `sqlite` - SQLite projection backend

## Quick Start

```rust
use kameo_es::{
    command_service::CommandService, Apply, Command, Entity, EventType, 
    Metadata, Context
};
use serde::{Deserialize, Serialize};

// Define your aggregate
#[derive(Clone, Debug, Default)]
pub struct BankAccount {
    balance: i64,
}

impl Entity for BankAccount {
    type ID = String;
    type Event = BankAccountEvent;
    type Metadata = ();

    fn category() -> &'static str {
        "bank_account"
    }
}

// Define events with the EventType derive macro
#[derive(Clone, Debug, EventType, Serialize, Deserialize)]
pub enum BankAccountEvent {
    MoneyDeposited { amount: u32 },
    MoneyWithdrawn { amount: u32 },
}

// Apply events to update state
impl Apply for BankAccount {
    fn apply(&mut self, event: Self::Event, _metadata: Metadata<()>) {
        match event {
            BankAccountEvent::MoneyDeposited { amount } => {
                self.balance += amount as i64;
            }
            BankAccountEvent::MoneyWithdrawn { amount } => {
                self.balance -= amount as i64;
            }
        }
    }
}

// Define commands
#[derive(Clone, Debug)]
pub struct Deposit {
    pub amount: u32,
}

impl Command<Deposit> for BankAccount {
    type Error = String;

    fn handle(
        &self,
        cmd: Deposit,
        _ctx: Context<'_, Self>,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        Ok(vec![BankAccountEvent::MoneyDeposited {
            amount: cmd.amount,
        }])
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to SierraDB (Redis protocol)
    let client = redis::Client::open("redis://127.0.0.1:9090/")?;
    let conn = client.get_multiplexed_tokio_connection().await?;
    let mut cmd_service = CommandService::new(conn);

    // Execute commands
    BankAccount::execute(&mut cmd_service, "alice".to_string(), Deposit { amount: 1000 }).await?;
    
    Ok(())
}
```

## Core Concepts

### Entities
Entities are the main aggregates in your domain. They implement the `Entity` trait and define:
- `ID`: The identifier type for the entity
- `Event`: The event enum for this entity
- `Metadata`: Custom metadata type (often `()`)
- `category()`: A unique category name for the entity

### Commands  
Commands represent operations that can be performed on entities. They implement the `Command<C>` trait and contain the business logic for generating events.

### Events
Events represent state changes that have occurred. Use the `EventType` derive macro to automatically implement the required trait.

### Projections
Projections create read models from the event stream. Multiple backends are supported for storing checkpoints and projection state:

- **In-Memory**: For testing and ephemeral projections
- **File**: JSON/CBOR file-based storage  
- **SQLite**: Lightweight database storage
- **PostgreSQL**: Full-featured database with transactions
- **MongoDB**: Document database storage

## Examples

See the `examples/` directory for complete working examples:

- `bank_account.rs` - Basic entity with commands and events
- `projection.rs` - Event handlers and projections

## Projection Backends

### In-Memory
```rust
use kameo_es::event_handler::in_memory::InMemoryEventProcessor;

let processor = InMemoryEventProcessor::new(my_handler);
```

### PostgreSQL
```rust
use kameo_es::event_handler::postgres::PostgresProcessor;

let processor = PostgresProcessor::new(pool, my_handler).await?;
```

### File-based
```rust
use kameo_es::event_handler::file::{FileProcessor, JsonSerializer};

let processor = FileProcessor::<JsonSerializer>::new("checkpoints.json", my_handler).await?;
```

## Requirements

- Rust 1.70+
- SierraDB server (Redis-compatible protocol)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
