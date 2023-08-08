use axum::{
    routing::{get, post},
    Router,
};
use connectors::Connector;
use http::Uri;
use route_handlers::*;
use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener},
    str::FromStr,
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
            .route("/connectors", get(list_connectors))
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
            addr,
            _shutdown: Some(shutdown),
        }
    }

    pub fn base_url(&self) -> Uri {
        // reqwest::Url::parse(&format!("http://{}{}", self.addr, endpoint.unwrap_or("/"))).unwrap()
        Uri::from_str(&format!("http://{}", &self.addr.to_string())).unwrap()
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
    use crate::Connectors;
    use axum::extract::{Json, State};

    pub async fn create_connector(
        State(state): State<Connectors>,
        Json(payload): Json<CreateConnector>,
    ) -> Json<Connector> {
        let connector = Connector::from(&payload);
        state
            .lock()
            .unwrap()
            .insert(connector.name.clone().0, connector);

        Json(Connector::from(&payload))
    }

    pub async fn list_connectors(State(state): State<Connectors>) -> Json<Vec<ConnectorName>> {
        let connectors = state
            .lock()
            .unwrap()
            .keys()
            .map(|c| ConnectorName(c.to_string()))
            .collect::<Vec<ConnectorName>>();
        Json(connectors)
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
        Sink,
        Source,
    }

    impl From<&CreateConnector> for Connector {
        fn from(connector: &CreateConnector) -> Self {
            let mut tasks = Vec::<Task>::new();
            let c_type = if connector.name.0.to_lowercase().contains("sink") {
                ConnectorType::Sink
            } else {
                ConnectorType::Source
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
        assert_eq!(c.connector_type, ConnectorType::Sink);
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
        let endpoint = format!("{}{}", server.base_url().to_string(), "connectors");
        let reqwest_uri = reqwest::Url::from_str(&endpoint).unwrap();
        dbg!(&reqwest_uri);
        dbg!(server.base_url());
        dbg!(&endpoint);
        let client = reqwest::Client::new();
        let body = client.post(reqwest_uri).json(&c).send().await;
        let returned_response = body.unwrap().json::<Connector>().await;
        assert_eq!(returned_connector, returned_response.unwrap());
    }

    #[tokio::test]
    async fn test_listing_connectors() {
        let c_connector = r#"
        {
            "name": "sink-connector",
            "config": {
                "tasks.max": "10",
                "connector.class": "com.example.kafka",
                "name": "sink-connector"
            }
        }"#;
        let c: CreateConnector = serde_json::from_str(c_connector).unwrap();

        let server = KcTestServer::new().await;
        let endpoint = format!("{}{}", server.base_url().to_string(), "connectors");
        let reqwest_uri = reqwest::Url::from_str(&endpoint).unwrap();
        let client = reqwest::Client::new();
        client
            .post(reqwest_uri.clone())
            .json(&c)
            .send()
            .await
            .unwrap();

        let response = reqwest::get(endpoint).await.unwrap().text().await.unwrap();
        assert_eq!(response, "[\"sink-connector\"]");
    }

    #[tokio::test]
    async fn test_listing_empty_connectors() {
        let server = KcTestServer::new().await;
        let endpoint = format!("{}{}", server.base_url().to_string(), "connectors");
        let reqwest_uri = reqwest::Url::from_str(&endpoint).unwrap();
        let response = reqwest::get(reqwest_uri)
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(response, "[]");
    }
}
