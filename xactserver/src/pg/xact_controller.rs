use anyhow::{anyhow, bail};
use bytes::Bytes;
use log::{error, info};
use std::net::SocketAddr;
use tokio::sync::oneshot;

use crate::XactId;

pub enum PgXactController {
    Local(LocalXact),
    Surrogate(SurrogateXact),
}

impl PgXactController {
    pub fn new_local(commit_tx: oneshot::Sender<bool>) -> Self {
        Self::Local(LocalXact {
            commit_tx: Some(commit_tx),
        })
    }

    pub fn new_surrogate(xact_id: XactId, connect_pg: SocketAddr, data: Bytes) -> Self {
        Self::Surrogate(SurrogateXact::new(xact_id, connect_pg, data))
    }

    pub async fn execute(&mut self) -> anyhow::Result<bool> {
        match self {
            Self::Local(_) => {
                bail!("Local transaction is already executed")
            }
            Self::Surrogate(xact) => xact.execute().await,
        }
    }

    pub async fn commit(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Local(xact) => xact.commit(),
            Self::Surrogate(xact) => xact.commit().await,
        }
    }

    pub async fn rollback(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Local(xact) => xact.rollback(),
            Self::Surrogate(xact) => xact.rollback().await,
        }
    }
}

pub struct LocalXact {
    commit_tx: Option<oneshot::Sender<bool>>,
}

impl LocalXact {
    fn commit(&mut self) -> anyhow::Result<()> {
        self.commit_tx
            .take()
            .ok_or(anyhow!("Transaction has already committed or rollbacked"))
            .and_then(|tx| {
                tx.send(true)
                    .or(Err(anyhow!("Failed to commit transaction")))
            })
    }

    fn rollback(&mut self) -> anyhow::Result<()> {
        self.commit_tx
            .take()
            .ok_or(anyhow!("Transaction has already committed or rollbacked"))
            .and_then(|tx| {
                tx.send(true)
                    .or(Err(anyhow!("Failed to rollback transaction")))
            })
    }
}

pub struct SurrogateXact {
    xact_id: XactId,
    connect_pg: SocketAddr,
    data: Bytes,
    client: Option<tokio_postgres::Client>,
}

impl SurrogateXact {
    fn new(xact_id: XactId, connect_pg: SocketAddr, data: Bytes) -> Self {
        Self {
            xact_id,
            connect_pg,
            data,
            client: None,
        }
    }

    async fn execute(&mut self) -> anyhow::Result<bool> {
        // TODO: Use a connection pool
        let conn_str = format!(
            "host={} port={} user=cloud_admin dbname=postgres application_name=xactserver",
            self.connect_pg.ip(),
            self.connect_pg.port(),
        );
        info!("Connecting to local pg, conn_str: {}", conn_str);
        let (client, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                error!("Connection error: {}", e);
            }
        });

        let pg_xact = self.client.get_or_insert(client).transaction().await?;
        // TODO: Not doing much for now
        pg_xact
            .execute("SELECT print_bytes($1::bytea);", &[&self.data.as_ref()])
            .await?;
        pg_xact
            .execute("PREPARE TRANSACTION $1", &[&self.xact_id.to_string()])
            .await?;
        Ok(true)
    }

    async fn commit(&self) -> anyhow::Result<()> {
        match self.client {
            Some(ref client) => {
                client
                    .execute("COMMIT PREPARED $1", &[&self.xact_id.to_string()])
                    .await?;
            }
            None => {
                bail!("Connection does not exist");
            }
        }
        Ok(())
    }

    async fn rollback(&self) -> anyhow::Result<()> {
        match self.client {
            Some(ref client) => {
                client
                    .execute("ROLLBACK PREPARED $1", &[&self.xact_id.to_string()])
                    .await?;
            }
            None => {
                bail!("Connection does not exist");
            }
        }
        Ok(())
    }
}
