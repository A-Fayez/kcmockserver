use axum::{routing::get, Router};
use route_handlers::*;
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

mod route_handlers {
    pub async fn root() -> &'static str {
        "Hello, World!"
    }

    pub async fn slash() -> &'static str {
        "Hello Slash"
    }
}

mod connectors {
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    type ConnectorConfig = HashMap<String, String>;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct ConnectorName(pub String);

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct Connector {
        pub name: ConnectorName,
        pub config: ConnectorConfig,
        pub tasks: Vec<Task>,
        #[serde(rename = "type")]
        pub connector_type: ConnectorType,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct CreateConnector {
        pub name: ConnectorName,
        pub config: ConnectorConfig,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct Task {
        pub connector: ConnectorName,
        #[serde(rename = "task")]
        pub id: usize,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "lowercase")]
    pub enum ConnectorType {
        SINK,
        SOURCE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::*;

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

    #[test]
    fn test_create_connector_deserialization_and_serialization() {
        // test deserialization
        let c_connector = r#"
        {
            "name": "test",
            "config": {
                "tasks.max": "10",
                "connector.class": "com.example.kafka",
                "name": "test"
            }
        }"#;

        let c: CreateConnector = serde_json::from_str(c_connector).unwrap();
        assert_eq!(c.name.0, "test");
        assert_eq!(c.config.get("name").unwrap(), "test");
        assert_eq!(
            c.config.get("connector.class").unwrap(),
            "com.example.kafka"
        );

        let c = r#"
        {
            "name": "test",
            "config": {
                "tasks.max": "10",
                "connector.class": "com.example.kafka"
            },
            "tasks": [
                {
                    "connector": "test",
                    "task": 0
                }
            ],
            "type": "sink"
        }"#;

        let c: Connector = serde_json::from_str(c).unwrap();
        assert_eq!(c.connector_type, ConnectorType::SINK);
        dbg!(c);
    }
}
