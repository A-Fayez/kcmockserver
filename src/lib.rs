use axum::{routing::get, Router};
use std::net::SocketAddr;
use std::str::FromStr;

pub struct KcTestServer {
    addr: SocketAddr,
    _shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl KcTestServer {
    pub fn new() -> Self {
        // build our application with a single route

        let (shutdown, rx) = tokio::sync::oneshot::channel::<()>();

        let addr = SocketAddr::from_str("0.0.0.0:3000").unwrap();
        let app = Router::new().route("/", get(root));
        let server = axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(async {
                rx.await.ok();
            });

        tokio::spawn(server);

        KcTestServer {
            addr: addr,
            _shutdown: Some(shutdown),
        }
    }

    pub fn endpoint(&self) -> reqwest::Url {
        reqwest::Url::parse(&format!("http://{}", self.addr)).unwrap()
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
        let body = reqwest::get(server.endpoint())
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(body, "Hello, World!");
    }
}
