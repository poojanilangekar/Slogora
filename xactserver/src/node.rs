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
    xact_manager_tx: mpsc::Sender<XsMessage>,
}

impl Node {
    pub fn new(addr: &SocketAddr, xact_manager_tx: mpsc::Sender<XsMessage>) -> Node {
        Self {
            addr: addr.to_owned(),
            xact_manager_tx,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        info!("Node listening on {}", self.addr);

        let addr = self.addr.clone();
        let svc = XactCoordinationServer::new(self);
        Server::builder().add_service(svc).serve(addr).await?;

        Ok(())
    }
}

#[tonic::async_trait]
impl XactCoordination for Node {
    async fn prepare(
        &self,
        request: Request<PrepareRequest>,
    ) -> Result<Response<PrepareResponse>, Status> {
        self.xact_manager_tx
            .send(XsMessage::Prepare(request.into_inner()))
            .await
            .unwrap();
        Ok(Response::new(PrepareResponse {}))
    }

    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        self.xact_manager_tx
            .send(XsMessage::Vote(request.into_inner()))
            .await
            .unwrap();
        Ok(Response::new(VoteResponse {}))
    }
}
