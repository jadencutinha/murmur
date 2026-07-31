//! The gRPC front door for a single murmur node.
//!
//! This checkpoint serves the KV API straight from the local Sable engine —
//! there is no replication yet, so a node is authoritative for its own data.
//! Later checkpoints slot the Raft layer between this service and the store, but
//! the wire contract defined here does not change.

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};

use crate::proto::kv_server::{Kv, KvServer};
use crate::proto::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse,
};
use crate::store::KvStore;

/// Implements the generated `Kv` gRPC service on top of a local [`KvStore`].
pub struct KvService {
    store: KvStore,
}

impl KvService {
    pub fn new(store: KvStore) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl Kv for KvService {
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let key = request.into_inner().key;
        let found = self
            .store
            .get(&key)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(match found {
            Some(value) => GetResponse { found: true, value },
            None => GetResponse {
                found: false,
                value: Vec::new(),
            },
        }))
    }

    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> Result<Response<PutResponse>, Status> {
        let PutRequest { key, value } = request.into_inner();
        self.store
            .put(&key, &value)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PutResponse {}))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let key = request.into_inner().key;
        self.store
            .delete(&key)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(DeleteResponse {}))
    }
}

/// Serve the KV API on an already-bound listener until the process exits.
///
/// Taking a bound [`TcpListener`] (rather than a `SocketAddr`) lets callers bind
/// port 0 and learn the actual address — which the integration tests rely on to
/// run many isolated nodes without port collisions.
pub async fn serve(store: KvStore, listener: TcpListener) -> anyhow::Result<()> {
    let svc = KvService::new(store);
    Server::builder()
        .add_service(KvServer::new(svc))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;
    Ok(())
}
