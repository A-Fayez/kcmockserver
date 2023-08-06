use axum::{routing::get, Router};
use std::net::{SocketAddr, TcpListener};

pub struct KcTestServer {
    addr: SocketAddr,
    _shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl KcTestServer {
    pub async fn new() -> Self {
        let (shutdown, rx) = tokio::sync::oneshot::channel::<()>();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/", get(root))
            .route("/slash", get(slash));
        let server = axum::Server::from_tcp(listener)
            .unwrap()
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

    pub fn uri(&self, endpoint: Option<&str>) -> reqwest::Url {
        reqwest::Url::parse(&format!("http://{}{}", self.addr, endpoint.unwrap_or("/"))).unwrap()
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

async fn slash() -> &'static str {
    "Hello Slash"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        let server = KcTestServer::new().await;
        println!("{}", server.addr);
        dbg!(server.addr);
        let body = reqwest::get(server.uri(Some("/")))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(body, "Hello, World!");
    }

    #[tokio::test]
    async fn it_works_slash() {
        let server = KcTestServer::new().await;
        let endpoint = server.uri(Some("/slash"));
        dbg!(server.addr);
        let body = reqwest::get(endpoint).await.unwrap().text().await.unwrap();
        assert_eq!(body, "Hello Slash");
    }
}
