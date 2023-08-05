use axum::{routing::get, Router};
use http::Uri;
use std::net::SocketAddr;
use std::net::{IpAddr, Ipv4Addr};

pub struct KcTestServer {
    addr: SocketAddr,
    _shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl KcTestServer {
    pub fn new() -> Self {
        // build our application with a single route

        let (shutdown, rx) = tokio::sync::oneshot::channel::<()>();

        // run it with hyper on localhost:3000

        // let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let app = Router::new().route("/", get(root));
        let server = axum::Server::bind(&"127.0.0.1:3000".parse().unwrap())
            .serve(app.into_make_service())
            .with_graceful_shutdown(async {
                rx.await.ok();
            });

        tokio::spawn(server);

        KcTestServer {
            addr: "127.0.0.1:3000".parse().unwrap(),
            _shutdown: Some(shutdown),
        }
    }

    pub fn endpoint(&self) -> reqwest::Url {
        reqwest::Url::parse("http://0.0.0.0:3000").unwrap()
    }
}

impl Drop for KcTestServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self._shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

async fn root() -> &'static str {
    "Hello, World!"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        let server = KcTestServer::new();
        println!("{}", server.addr);
        dbg!(server.addr);
        dbg!(server.endpoint());
        let body = reqwest::get(server.endpoint())
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(body, "Hello, World!");
    }
}
