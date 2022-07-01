use log::info;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::proto::xact_coordination_server::{XactCoordination, XactCoordinationServer};
use crate::proto::{PrepareRequest, PrepareResponse, VoteRequest, VoteResponse};
use crate::XsMessage;

pub struct Node {
    addr: SocketAddr,
    xactserver_tx: mpsc::Sender<XsMessage>,
}

impl Node {
    pub fn new(addr: &SocketAddr, xactserver_tx: mpsc::Sender<XsMessage>) -> Node {
        Self {
            addr: addr.to_owned(),
            xactserver_tx,
        }
    }

    pub fn thread_main(self) -> anyhow::Result<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;

        info!("node listening on {}", self.addr);

        let addr = self.addr.clone();
        let svc = XactCoordinationServer::new(self);
        rt.block_on(Server::builder().add_service(svc).serve(addr))?;

        Ok(())
    }
}

#[tonic::async_trait]
impl XactCoordination for Node {
    async fn prepare(
        &self,
        request: Request<PrepareRequest>,
    ) -> Result<Response<PrepareResponse>, Status> {
        self.xactserver_tx
            .send(XsMessage::Prepare(request.into_inner()))
            .await
            .unwrap();
        Ok(Response::new(PrepareResponse {}))
    }

    async fn vote(&self, _request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        Ok(Response::new(VoteResponse {}))
    }
}
