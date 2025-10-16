use std::time::Duration;

use anyhow::bail;
use futures::FutureExt;
use kameo_es::{
    command_service::{CommandService, ExecuteExt},
    transaction::{Transaction, TransactionOutcome},
    Apply, Command, Context, Entity, EventType, Metadata, StreamId,
};

use serde::{Deserialize, Serialize};
use sierradb_client::stream_partition_key;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fmt::init();

    let client = redis::Client::open("redis://127.0.0.1:9090/")?;
    let conn = conn.get_multiplexed_tokio_connection().await?;
    let mut cmd_service = CommandService::new(conn);

    BankAccount::execute(&mut tx, "bob".to_string(), Deposit { amount: 1_000 }).await?;
    BankAccount::execute(&mut tx, "alice".to_string(), Deposit { amount: 2_000 }).await?;

    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct BankAccount {
    balance: i64,
}

impl Entity for BankAccount {
    type ID = String;
    type Event = BankAccountEvent;
    type Metadata = ();

    fn category() -> &'static str {
        "BankAccount"
    }
}

#[derive(Clone, Debug, EventType, Serialize, Deserialize)]
pub enum BankAccountEvent {
    MoneyWithdrawn { amount: u32 },
    MoneyDeposited { amount: u32 },
}

impl Apply for BankAccount {
    fn apply(&mut self, event: Self::Event, _metadata: Metadata<()>) {
        match event {
            BankAccountEvent::MoneyWithdrawn { amount } => self.balance -= amount as i64,
            BankAccountEvent::MoneyDeposited { amount } => self.balance += amount as i64,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Withdraw {
    pub amount: u32,
}

impl Command<Withdraw> for BankAccount {
    type Error = anyhow::Error;

    fn handle(
        &self,
        cmd: Withdraw,
        _ctx: Context<'_, Self>,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        if self.balance < cmd.amount as i64 {
            bail!("insufficient balance");
        }

        Ok(vec![BankAccountEvent::MoneyWithdrawn {
            amount: cmd.amount,
        }])
    }
}

#[derive(Clone, Debug)]
pub struct Deposit {
    pub amount: u32,
}

impl Command<Deposit> for BankAccount {
    type Error = anyhow::Error;

    fn handle(
        &self,
        cmd: Deposit,
        _ctx: Context<'_, Self>,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        dbg!(self.balance);
        Ok(vec![BankAccountEvent::MoneyDeposited {
            amount: cmd.amount,
        }])
    }
}
