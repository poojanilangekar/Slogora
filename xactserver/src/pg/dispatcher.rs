use bytes::Buf;
use log::error;
use tokio::sync::mpsc;
use tokio_postgres::{connect, NoTls};

use crate::XsMessage;

pub struct PgDispatcher {
    addr: String,
}

impl PgDispatcher {
    pub fn new(addr: &str) -> PgDispatcher {
        PgDispatcher {
            addr: addr.to_owned(),
        }
    }

    pub fn thread_main(&self, xactserver_rx: mpsc::Receiver<XsMessage>) -> anyhow::Result<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        rt.block_on(async move {
            let mut xactserver_rx = xactserver_rx;
            while let Some(msg) = xactserver_rx.recv().await {
                if let XsMessage::SurrogateXact { mut data, vote_tx } = msg {
                    let ip_port: Vec<&str> = self.addr.split(":").collect();
                    let conn_str = format!(
                        "host={} port={} user=postgres application_name=xactserver",
                        ip_port[0], ip_port[1]
                    );

                    println!("connecting to local pg, conn_str: {}", conn_str);

                    // TODO(mliu) should only connect once and retry if the connection is broken
                    let (client, conn) = connect(&conn_str, NoTls).await.unwrap();

                    // The connection object performs the actual communication with the database,
                    // so spawn it off to run on its own.
                    tokio::spawn(async move {
                        if let Err(e) = conn.await {
                            error!("connection error: {}", e);
                        }
                    });

                    println!("connected to pg, sending transaction data");

                    // Copy buf to a new vec<u8> because tokio_postgres does not
                    // know how to convert bytes::Bytes to postgres type
                    let mut txn_data: Vec<u8> = Vec::new();
                    txn_data.resize(data.len(), 0);
                    data.copy_to_slice(&mut txn_data);

                    // TODO(mliu) should retry, just printing the error out for now
                    if let Err(e) = client
                        .query("SELECT print_bytes($1::bytea);", &[&txn_data])
                        .await
                    {
                        error!("calling postgres UDF failed with error: {}", e);
                    }
                    vote_tx.send(true).unwrap();
                }
            }
        });
        Ok(())
    }
}
