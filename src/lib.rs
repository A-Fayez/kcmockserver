use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener},
    str::FromStr,
    sync::{Arc, Mutex},
};

use axum::{
    routing::{get, post},
    Router,
};
use http::Uri;

use connectors::Connector;
use route_handlers::*;

pub struct KcTestServer {
    addr: SocketAddr,
    _shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

type Connectors = Arc<Mutex<HashMap<String, Connector>>>;

impl KcTestServer {
    pub fn new() -> Self {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (shutdown, rx) = tokio::sync::oneshot::channel::<()>();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/", get(server_status))
            .route("/connectors", get(list_connectors))
            .route("/connectors", post(create_connector))
            .with_state(Connectors::new(Mutex::new(
                HashMap::<String, Connector>::new(),
            )));

        std::thread::spawn(move || {
            runtime.block_on(async {
                axum::Server::from_tcp(listener)
                    .unwrap()
                    .serve(app.into_make_service())
                    .with_graceful_shutdown(async {
                        rx.await.ok();
                    })
                    .await
                    .unwrap();
            })
        });

        KcTestServer {
            addr,
            _shutdown: Some(shutdown),
        }
    }

    pub fn base_url(&self) -> Uri {
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

    use axum::extract::{Json, Query, State};
    use axum::response::{IntoResponse, Response};
    use serde::Deserialize;
    use serde_json::json;

    use crate::connectors::*;
    use crate::Connectors;

    pub async fn server_status() -> Json<serde_json::Value> {
        Json(json!({
            "version": "3.0.0",
            "commit": uuid::Uuid::new_v4().to_string(),
            "kafka_cluster_id": uuid::Uuid::new_v4().to_string(),
        }))
    }

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

    pub async fn list_connectors(
        State(state): State<Connectors>,
        Query(params): Query<Params>,
    ) -> Response {
        let mut verbose_connectors = VerboseConnectors::new();
        match params.expand {
            Some(ExpandValue::Status) => state.lock().unwrap().iter().for_each(|c| {
                verbose_connectors.insert(
                    c.0.to_string(),
                    Status {
                        status: ConnectorStatus::from(c.1),
                    },
                );
            }),
            Some(ExpandValue::Info) => unimplemented!(),
            None => (),
        }

        if verbose_connectors.len() != 0 {
            return Json(verbose_connectors).into_response();
        }

        let connectors = state
            .lock()
            .unwrap()
            .keys()
            .map(|c| ConnectorName(c.to_string()))
            .collect::<Vec<ConnectorName>>();
        Json(connectors).into_response()
    }

    #[derive(Debug, Deserialize)]
    pub struct Params {
        pub expand: Option<ExpandValue>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub enum ExpandValue {
        Status,
        Info,
    }
}

mod connectors {
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    type ConnectorConfig = HashMap<String, String>;

    pub type VerboseConnectors = HashMap<String, Status>;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq, Hash)]
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

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct Status {
        pub status: ConnectorStatus,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct ConnectorStatus {
        pub name: ConnectorName,
        pub connector: ConnectorState,
        pub tasks: Vec<VerboseTask>,
        #[serde(rename = "type")]
        pub connector_type: ConnectorType,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct ConnectorState {
        pub state: State,
        pub worker_id: String,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct VerboseTask {
        pub id: usize,
        pub state: State,
        pub worker_id: String,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "UPPERCASE")]
    pub enum State {
        Running,
        Failed,
        Unassigned,
        Paused,
    }
    impl From<&CreateConnector> for Connector {
        fn from(connector: &CreateConnector) -> Self {
            let mut tasks = Vec::<Task>::new();
            // fix this should check connector.class and not connector name
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

    impl From<&Connector> for ConnectorStatus {
        fn from(connector: &Connector) -> Self {
            let connector_name = connector.name.clone();
            let tasks = vec![VerboseTask {
                id: 0,
                state: State::Running,
                worker_id: String::from("127.0.1.1:8083"),
            }];
            Self {
                name: connector_name,
                connector: ConnectorState {
                    state: (State::Running),
                    worker_id: (String::from("127.0.1.1:8083")),
                },
                tasks,
                connector_type: ConnectorType::Source,
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

        let server = KcTestServer::new();
        let endpoint = format!("{}{}", server.base_url().to_string(), "connectors");
        let reqwest_uri = reqwest::Url::from_str(&endpoint).unwrap();
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

        let server = KcTestServer::new();
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

    // #[tokio::test]
    #[test]
    fn test_listing_empty_connectors() {
        let server = KcTestServer::new();
        dbg!(server.addr);
        // std::thread::sleep(Duration::from_secs(10));
        let endpoint = format!("{}{}", server.base_url().to_string(), "connectors");
        let reqwest_uri = reqwest::Url::from_str(&endpoint).unwrap();
        let response = reqwest::blocking::get(reqwest_uri).unwrap().text().unwrap();
        assert_eq!(response, "[]");
    }

    #[tokio::test]
    async fn test_listing_connectors_with_expand_status_query_parameter() {
        let server = KcTestServer::new();

        // first create some test connectors
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
        let endpoint = format!("{}{}", server.base_url().to_string(), "connectors");

        let client = reqwest::Client::new();
        client.post(endpoint.clone()).json(&c).send().await.unwrap();

        let c_connector = r#"
        {
            "name": "dev",
            "config": {
                "tasks.max": "3",
                "connector.class": "com.example.mongo",
                "name": "dev"
            }
        }"#;
        let c: CreateConnector = serde_json::from_str(c_connector).unwrap();
        let endpoint = format!("{}{}", server.base_url().to_string(), "connectors");

        let client = reqwest::Client::new();
        client.post(endpoint.clone()).json(&c).send().await.unwrap();

        // query with params
        let params = [("expand", "status")];
        let endpoint_with_params = reqwest::Url::parse_with_params(&endpoint, &params).unwrap();
        let response: serde_json::Map<String, serde_json::Value> =
            reqwest::get(endpoint_with_params)
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
        // let response = serde_json::to_string_pretty(&response).unwrap();
        // println!("{}", response);
        assert!(response.contains_key("dev"));
        assert!(response.contains_key("test"));
        let status = response.get("dev").unwrap().get("status").unwrap();
        assert_eq!(status.get("name").unwrap(), &serde_json::Value::from("dev"));
        assert_eq!(
            status.get("connector").unwrap().get("state").unwrap(),
            "RUNNING"
        );

        let status = response.get("test").unwrap().get("status").unwrap();
        assert_eq!(
            status.get("name").unwrap(),
            &serde_json::Value::from("test")
        );
        assert_eq!(
            status.get("connector").unwrap().get("state").unwrap(),
            "RUNNING"
        );
    }
}
