use axum::{
    routing::{get, post},
    Router,
};
use connectors::Connector;
use route_handlers::*;
use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener},
    sync::{Arc, Mutex},
};

pub struct KcTestServer {
    addr: SocketAddr,
    _shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

type Connectors = Arc<Mutex<HashMap<String, Connector>>>;

impl KcTestServer {
    pub async fn new() -> Self {
        let (shutdown, rx) = tokio::sync::oneshot::channel::<()>();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/", get(root))
            .route("/connectors", post(create_connector))
            .with_state(Connectors::new(Mutex::new(
                HashMap::<String, Connector>::new(),
            )));

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

    use crate::connectors::*;
    use axum::extract::Json;

    pub async fn root() -> &'static str {
        "Hello, World!"
    }

    pub async fn create_connector(Json(payload): Json<CreateConnector>) -> Json<Connector> {
        Json(Connector::from(&payload))
    }
}

mod connectors {
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    type ConnectorConfig = HashMap<String, String>;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
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

    impl From<&CreateConnector> for Connector {
        fn from(connector: &CreateConnector) -> Self {
            let mut tasks = Vec::<Task>::new();
            let c_type = if connector.name.0.to_lowercase().contains("sink") {
                ConnectorType::SINK
            } else {
                ConnectorType::SOURCE
            };

            tasks.push(Task {
                connector: (connector.name.clone()),
                id: (0),
            });
            Connector {
                name: (connector.name.clone()),
                config: (connector.config.clone()),
                tasks: (tasks),
                connector_type: (c_type),
            }
        }
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

    #[tokio::test]
    async fn test_creating_a_connector() {
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
        let returned_connector = Connector::from(&c);

        let server = KcTestServer::new().await;
        let endpoint = server.uri(Some("/connectors"));
        let client = reqwest::Client::new();
        let body = client.post(endpoint).json(&c).send().await;
        let returned_response = body.unwrap().json::<Connector>().await;
        assert_eq!(returned_connector, returned_response.unwrap());
    }
}
