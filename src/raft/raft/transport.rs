use super::Result;
use madsim::net::{rpc::Request, Endpoint};
use std::{future::Future, net::SocketAddr, time::Duration};

pub trait Transport {
    /// Call function on a remote host.
    fn call<R: Request>(
        &mut self,
        dst: SocketAddr,
        request: R,
    ) -> impl std::future::Future<Output = std::io::Result<R::Response>> + Send;

    fn call_timeout<R: Request>(
        &mut self,
        dst: SocketAddr,
        request: R,
        timeout: Duration,
    ) -> impl std::future::Future<Output = std::io::Result<R::Response>> + Send;

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
    async fn call<R: Request>(
        &mut self,
        dst: SocketAddr,
        request: R,
    ) -> std::io::Result<R::Response> {
        self.endpoint.call(dst, request).await
    }

    async fn call_timeout<R: Request>(
        &mut self,
        dst: SocketAddr,
        request: R,
        timeout: Duration,
    ) -> std::io::Result<R::Response> {
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

#[cfg(test)]
pub mod testing {
    use std::any::Any;
    use std::{future::Future, net::SocketAddr, time::Duration};

    use futures::{channel::mpsc, StreamExt};
    use madsim::net::rpc::Request;
    use serde::de::DeserializeOwned;
    use serde::Serialize;

    use super::Result;
    use super::Transport;

    pub struct MockTransport {
        tx: mpsc::UnboundedSender<Box<dyn Any + Send>>,
        rx: mpsc::UnboundedReceiver<Box<dyn Any + Send>>,
    }

    impl MockTransport {
        pub fn new() -> Self {
            let (tx, rx) = mpsc::unbounded();
            Self { tx, rx }
        }

        pub async fn respond<R: Request>(&self, response: R::Response) {
            self.tx
                .unbounded_send(Box::new(response))
                .expect("Failed to send response");
        }
    }

    impl Transport for MockTransport {
        async fn call<R: Request>(
            &mut self,
            _dst: SocketAddr,
            _request: R,
        ) -> std::io::Result<R::Response> {
            let response = self.rx.next().await;
            match response {
                Some(rsp) => Ok(*rsp
                    .downcast::<R::Response>()
                    .expect("Response type mismatch")),
                None => panic!("No response received in MockTransport"),
            }
        }

        async fn call_timeout<R: Request>(
            &mut self,
            dst: SocketAddr,
            request: R,
            _timeout: Duration,
        ) -> std::io::Result<R::Response> {
            self.call(dst, request).await
        }

        fn add_rpc_handler<R: Request, AsyncFn, Fut>(&self, _f: AsyncFn)
        where
            AsyncFn: FnMut(R) -> Fut + Send + 'static,
            Fut: Future<Output = R::Response> + Send + 'static,
        {
            unimplemented!()
        }
    }
}
