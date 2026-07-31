//! The gRPC front door for a *single, unreplicated* murmur node — the original
//! checkpoint-2 path, kept for the direct KV smoke test.
//!
//! The replicated production path is [`crate::app`], which routes every request
//! through Raft. This service talks straight to the local Sable engine, so it
//! implements the same `Kv` wire contract but ignores the client-dedup fields
//! (`client_id`/`seq`): with a single authoritative node there is nothing to
//! deduplicate across.

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};

use crate::proto::kv_server::{Kv, KvServer};
use crate::proto::{
    AppendRequest, AppendResponse, DeleteRequest, DeleteResponse, GetRequest, GetResponse,
    PutRequest, PutResponse, RegisterRequest, RegisterResponse,
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
    async fn register(
        &self,
        _request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        // Handed out for wire compatibility; the direct path does no dedup.
        Ok(Response::new(RegisterResponse { client_id: fastrand::u64(1..=u64::MAX) }))
    }

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
        // client_id/seq ignored: nothing to deduplicate against on a single node.
        let PutRequest { key, value, .. } = request.into_inner();
        self.store
            .put(&key, &value)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PutResponse {}))
    }

    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let AppendRequest { key, value, .. } = request.into_inner();
        let mut current = self
            .store
            .get(&key)
            .map_err(|e| Status::internal(e.to_string()))?
            .unwrap_or_default();
        current.extend_from_slice(&value);
        self.store
            .put(&key, &current)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(AppendResponse { value: current }))
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
