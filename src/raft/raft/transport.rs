use madsim::net::{rpc::Request, Endpoint};
use std::{future::Future, io, net::SocketAddr, time::Duration};

pub trait Transport {
    /// Call function on a remote host.
    fn call<R: Request>(
        &self,
        dst: SocketAddr,
        request: R,
    ) -> impl std::future::Future<Output = io::Result<R::Response>> + Send;

    fn call_timeout<R: Request>(
        &self,
        dst: SocketAddr,
        request: R,
        timeout: Duration,
    ) -> impl std::future::Future<Output = io::Result<R::Response>> + Send;

    /// Add a RPC handler.
    fn add_rpc_handler<R: Request, AsyncFn, Fut>(&self, f: AsyncFn)
    where
        AsyncFn: FnMut(R) -> Fut + Send + 'static,
        Fut: Future<Output = R::Response> + Send + 'static;
}

pub struct MadsimTransport {
    endpoint: Endpoint,
}

impl MadsimTransport {
    pub fn new(endpoint: Endpoint) -> Self {
        Self { endpoint }
    }
}

impl Transport for MadsimTransport {
    async fn call<R: Request>(&self, dst: SocketAddr, request: R) -> io::Result<R::Response> {
        self.endpoint.call(dst, request).await
    }

    async fn call_timeout<R: Request>(
        &self,
        dst: SocketAddr,
        request: R,
        timeout: Duration,
    ) -> io::Result<R::Response> {
        self.endpoint.call_timeout(dst, request, timeout).await
    }

    fn add_rpc_handler<R: Request, AsyncFn, Fut>(&self, f: AsyncFn)
    where
        AsyncFn: FnMut(R) -> Fut + Send + 'static,
        Fut: Future<Output = R::Response> + Send + 'static,
    {
        self.endpoint.add_rpc_handler(f);
    }
}
