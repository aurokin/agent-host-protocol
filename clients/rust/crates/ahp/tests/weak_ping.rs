#![allow(clippy::panic, clippy::unwrap_used)]

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ahp::hosts::{HostConfig, HostEvent, HostId, MultiHostClient, ReconnectPolicy};
use ahp::{
    BoxedTransport, Client, ClientConfig, ClientError, DynTransport, Transport, TransportError,
    TransportMessage, WeakPingHandle,
};
use ahp_types::messages::{
    JsonRpcError, JsonRpcErrorResponse, JsonRpcMessage, JsonRpcRequest, JsonRpcSuccessResponse,
    JsonRpcVersion,
};
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};

struct BoundTransport {
    tx: mpsc::Sender<TransportMessage>,
    rx: mpsc::Receiver<TransportMessage>,
    bindings: mpsc::UnboundedSender<WeakPingHandle>,
    bind_count: Arc<AtomicUsize>,
}

struct Peer {
    tx: mpsc::Sender<TransportMessage>,
    rx: mpsc::Receiver<TransportMessage>,
    bindings: mpsc::UnboundedReceiver<WeakPingHandle>,
    bind_count: Arc<AtomicUsize>,
}

fn pair() -> (BoundTransport, Peer) {
    let (to_peer, rx) = mpsc::channel(16);
    let (tx, from_peer) = mpsc::channel(16);
    let (bindings, bound) = mpsc::unbounded_channel();
    let bind_count = Arc::new(AtomicUsize::new(0));
    (
        BoundTransport {
            tx: to_peer,
            rx: from_peer,
            bindings,
            bind_count: bind_count.clone(),
        },
        Peer {
            tx,
            rx,
            bindings: bound,
            bind_count,
        },
    )
}

impl Transport for BoundTransport {
    fn bind_client(&mut self, ping: WeakPingHandle) {
        self.bind_count.fetch_add(1, Ordering::SeqCst);
        self.bindings.send(ping).unwrap();
    }

    async fn send(&mut self, message: TransportMessage) -> Result<(), TransportError> {
        assert_eq!(self.bind_count.load(Ordering::SeqCst), 1);
        self.tx
            .send(message)
            .await
            .map_err(|_| TransportError::Closed)
    }

    async fn recv(&mut self) -> Result<Option<TransportMessage>, TransportError> {
        Ok(self.rx.recv().await)
    }
}

struct LegacyDynTransport {
    tx: mpsc::Sender<TransportMessage>,
    rx: mpsc::Receiver<TransportMessage>,
}

impl DynTransport for LegacyDynTransport {
    fn send<'a>(
        &'a mut self,
        message: TransportMessage,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>> {
        Box::pin(async move {
            self.tx
                .send(message)
                .await
                .map_err(|_| TransportError::Closed)
        })
    }

    fn recv<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<TransportMessage>, TransportError>> + Send + 'a>>
    {
        Box::pin(async move { Ok(self.rx.recv().await) })
    }

    fn close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn legacy_object_safe_transport_needs_no_binding_implementation() {
    let (transport, mut peer) = pair();
    let legacy = LegacyDynTransport {
        tx: transport.tx,
        rx: transport.rx,
    };
    let client = Client::connect(BoxedTransport::from_dyn(Box::new(legacy)), no_timeout())
        .await
        .unwrap();
    let mut request = Box::pin(client.ping());
    let sent = tokio::select! {
        result = &mut request => panic!("premature result: {result:?}"),
        sent = peer.request() => sent,
    };
    peer.reply(sent.id, Value::Null).await;
    bounded(request).await.unwrap();
    drop(client);
    assert!(bounded(peer.rx.recv()).await.is_none());
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .expect("fixture timed out")
}

impl Peer {
    async fn request(&mut self) -> JsonRpcRequest {
        let wire = bounded(self.rx.recv()).await.expect("driver closed");
        let JsonRpcMessage::Request(request) = wire.into_parsed().unwrap() else {
            panic!("expected request")
        };
        request
    }

    async fn reply(&self, id: u64, result: Value) {
        bounded(
            self.tx
                .send(TransportMessage::Parsed(JsonRpcMessage::SuccessResponse(
                    JsonRpcSuccessResponse {
                        jsonrpc: JsonRpcVersion::V2,
                        id,
                        result,
                    },
                ))),
        )
        .await
        .unwrap();
    }

    async fn bound(&mut self) -> WeakPingHandle {
        bounded(self.bindings.recv()).await.unwrap()
    }
}

fn no_timeout() -> ClientConfig {
    ClientConfig {
        default_request_timeout: None,
        ..ClientConfig::default()
    }
}

#[tokio::test]
async fn boxed_binding_precedes_handshakes_and_is_not_repeated() {
    for from_dyn in [false, true] {
        let (transport, mut peer) = pair();
        let transport = if from_dyn {
            let dynamic: Box<dyn DynTransport> = Box::new(transport);
            BoxedTransport::from_dyn(dynamic)
        } else {
            BoxedTransport::new(transport)
        };
        let client = Client::connect(transport, no_timeout()).await.unwrap();
        let ping = peer
            .bindings
            .try_recv()
            .expect("bind before connect returns");
        assert!(peer.rx.try_recv().is_err(), "binding must not send a ping");

        for method in ["initialize", "reconnect", "initialize"] {
            let request = client.request::<_, Value>(method, json!({}));
            tokio::pin!(request);
            let sent = tokio::select! {
                request = &mut request => panic!("premature result: {request:?}"),
                sent = peer.request() => sent,
            };
            assert_eq!(sent.method, method);
            peer.reply(sent.id, json!({"handshake": method})).await;
            assert_eq!(bounded(request).await.unwrap()["handshake"], method);
        }
        assert_eq!(peer.bind_count.load(Ordering::SeqCst), 1);
        assert!(peer.bindings.try_recv().is_err());
        drop(client);
        assert!(bounded(peer.rx.recv()).await.is_none());
        assert!(matches!(ping.ping().await, Err(ClientError::Shutdown)));
    }
}

#[tokio::test]
async fn weak_and_normal_pings_share_ids_and_out_of_order_correlation() {
    let (transport, mut peer) = pair();
    let client = Client::connect(BoxedTransport::new(transport), no_timeout())
        .await
        .unwrap();
    let ping = peer.bound().await;
    let ordinary = client.request::<_, Value>("listSessions", json!({"channel": "ahp-root://"}));
    let heartbeat = ping.ping();
    let normal_ping = client.ping();
    tokio::pin!(ordinary, heartbeat, normal_ping);

    let requests = async {
        let mut requests = Vec::new();
        for _ in 0..3 {
            requests.push(peer.request().await);
        }
        requests
    };
    let requests = tokio::select! {
        result = &mut ordinary => panic!("premature result: {result:?}"),
        result = &mut heartbeat => panic!("premature result: {result:?}"),
        result = &mut normal_ping => panic!("premature result: {result:?}"),
        requests = requests => requests,
    };
    let mut ids: Vec<_> = requests.iter().map(|request| request.id).collect();
    ids.sort_unstable();
    assert_eq!(ids, [1, 2, 3]);
    let discovery = requests
        .iter()
        .find(|r| r.method == "listSessions")
        .unwrap();
    for request in requests.iter().rev().filter(|r| r.method == "ping") {
        assert_eq!(request.params.as_ref().unwrap()["channel"], "ahp-root://");
        peer.reply(request.id, Value::Null).await;
    }
    bounded(heartbeat).await.unwrap();
    bounded(normal_ping).await.unwrap();
    // Discovery is still unanswered while both heartbeat responses resolve.
    assert!(tokio::time::timeout(Duration::ZERO, &mut ordinary)
        .await
        .is_err());
    peer.reply(discovery.id, json!({"items": []})).await;
    assert_eq!(bounded(ordinary).await.unwrap(), json!({"items": []}));
    client.shutdown().await;
    assert!(bounded(peer.rx.recv()).await.is_none());
}

#[tokio::test]
async fn weak_ping_works_while_managed_host_discovery_blocks_client_access() {
    let (transport, mut peer) = pair();
    let transport = Arc::new(Mutex::new(Some(transport)));
    let config = HostConfig::new("discovering", "Discovering host", move |_| {
        let transport = transport.clone();
        async move { Ok(BoxedTransport::new(transport.lock().await.take().unwrap())) }
    })
    .with_client_config(no_timeout())
    .with_reconnect_policy(ReconnectPolicy::disabled());
    let multi = MultiHostClient::new();
    multi.add_host(config).await.unwrap();
    let ping = peer.bound().await;
    let initialize = peer.request().await;
    assert_eq!(initialize.method, "initialize");
    peer.reply(
        initialize.id,
        json!({"protocolVersion": ahp_types::PROTOCOL_VERSION, "serverSeq": 0, "snapshots": []}),
    )
    .await;
    let discovery = peer.request().await;
    assert_eq!(discovery.method, "listSessions");
    assert!(multi.client(&HostId::from("discovering")).await.is_none());

    let heartbeat = ping.ping();
    tokio::pin!(heartbeat);
    let sent = tokio::select! {
        result = &mut heartbeat => panic!("premature ping: {result:?}"),
        sent = peer.request() => sent,
    };
    assert_eq!(sent.method, "ping");
    assert_ne!(sent.id, initialize.id);
    assert_ne!(sent.id, discovery.id);
    peer.reply(sent.id, Value::Null).await;
    bounded(heartbeat).await.unwrap();
    assert!(multi.client(&HostId::from("discovering")).await.is_none());
    bounded(multi.remove_host(&HostId::from("discovering")))
        .await
        .unwrap();
    assert!(bounded(peer.rx.recv()).await.is_none());
    assert!(matches!(ping.ping().await, Err(ClientError::Shutdown)));
}

#[tokio::test]
async fn unanswered_weak_ping_does_not_retain_last_client_or_driver() {
    let (transport, mut peer) = pair();
    let client = Client::connect(BoxedTransport::new(transport), no_timeout())
        .await
        .unwrap();
    let clone = client.clone();
    let ping = peer.bound().await;
    let heartbeat = ping.ping();
    tokio::pin!(heartbeat);
    let sent = tokio::select! {
        result = &mut heartbeat => panic!("premature ping: {result:?}"),
        sent = peer.request() => sent,
    };
    assert_eq!(sent.method, "ping");
    drop(client);
    assert!(peer.rx.try_recv().is_err());
    drop(clone);
    assert!(matches!(
        bounded(heartbeat).await,
        Err(ClientError::Shutdown)
    ));
    assert!(bounded(peer.rx.recv()).await.is_none(), "driver leaked");
    assert!(matches!(ping.ping().await, Err(ClientError::Shutdown)));
}

#[tokio::test]
async fn shutdown_and_transport_close_preserve_normal_errors_but_cancel_weak_ping() {
    for explicit in [false, true] {
        let (transport, mut peer) = pair();
        let client = Client::connect(transport, no_timeout()).await.unwrap();
        let ping = peer.bound().await;
        let normal = client.ping();
        let weak = ping.ping();
        tokio::pin!(normal, weak);
        let requests = async {
            peer.request().await;
            peer.request().await;
        };
        tokio::select! {
            result = &mut normal => panic!("premature ping: {result:?}"),
            result = &mut weak => panic!("premature ping: {result:?}"),
            _ = requests => {},
        }
        if explicit {
            client.shutdown().await;
        } else {
            drop(peer.tx);
        }
        let Err(ClientError::Rpc(error)) = bounded(normal).await else {
            panic!("normal ping must retain its existing RPC teardown error");
        };
        assert_eq!(error.code, -32000);
        assert_eq!(
            error.message,
            if explicit {
                "client shut down"
            } else {
                "transport closed"
            }
        );
        assert!(matches!(bounded(weak).await, Err(ClientError::Shutdown)));
        assert!(matches!(ping.ping().await, Err(ClientError::Shutdown)));
        assert!(bounded(peer.rx.recv()).await.is_none());
    }
}

#[tokio::test]
async fn weak_ping_preserves_server_error_and_configured_timeout() {
    let (transport, mut peer) = pair();
    let client = Client::connect(transport, no_timeout()).await.unwrap();
    let ping = peer.bound().await;
    let heartbeat = ping.ping();
    tokio::pin!(heartbeat);
    let request = tokio::select! {
        result = &mut heartbeat => panic!("premature ping: {result:?}"),
        request = peer.request() => request,
    };
    peer.tx
        .send(TransportMessage::Parsed(JsonRpcMessage::ErrorResponse(
            JsonRpcErrorResponse {
                jsonrpc: JsonRpcVersion::V2,
                id: request.id,
                error: JsonRpcError {
                    code: -32601,
                    message: "unsupported ping".into(),
                    data: None,
                },
            },
        )))
        .await
        .unwrap();
    let Err(ClientError::Rpc(error)) = bounded(heartbeat).await else {
        panic!("expected server RPC error");
    };
    assert_eq!(error.code, -32601);
    drop(client);
    assert!(bounded(peer.rx.recv()).await.is_none());

    let (transport, mut peer) = pair();
    let client = Client::connect(
        transport,
        ClientConfig {
            default_request_timeout: Some(Duration::ZERO),
            ..ClientConfig::default()
        },
    )
    .await
    .unwrap();
    let ping = peer.bound().await;
    assert!(matches!(
        bounded(ping.ping()).await,
        Err(ClientError::Cancelled)
    ));
    assert_eq!(peer.request().await.method, "ping");
    drop(client);
    assert!(bounded(peer.rx.recv()).await.is_none());
}

#[tokio::test]
async fn managed_host_ids_survive_factory_failure_handshake_failure_and_reconnect() {
    let (first, mut first_peer) = pair();
    let (second, mut second_peer) = pair();
    let (third, mut third_peer) = pair();
    let transports = Arc::new(Mutex::new(VecDeque::from([
        Err(TransportError::Closed),
        Ok(first),
        Ok(second),
        Ok(third),
    ])));
    let attempts = Arc::new(AtomicUsize::new(0));
    let factory_attempts = attempts.clone();
    let config = HostConfig::new("retained", "Retained delivery host", move |_| {
        let transports = transports.clone();
        factory_attempts.fetch_add(1, Ordering::SeqCst);
        async move {
            transports
                .lock()
                .await
                .pop_front()
                .unwrap()
                .map(BoxedTransport::new)
        }
    })
    .with_client_config(no_timeout())
    .with_reconnect_policy(ReconnectPolicy::immediate_forever());
    let multi = MultiHostClient::new();
    let mut events = multi.host_events();
    multi.add_host(config).await.unwrap();

    let first_ping = first_peer.bound().await;
    let abandoned = first_peer.request().await;
    assert_eq!(abandoned.method, "initialize");
    assert_eq!(abandoned.id, 1);
    drop(first_peer.tx);
    assert!(bounded(first_peer.rx.recv()).await.is_none());
    assert!(matches!(
        first_ping.ping().await,
        Err(ClientError::Shutdown)
    ));

    let second_ping = second_peer.bound().await;
    let retry = second_peer.request().await;
    assert_eq!(retry.method, "initialize");
    assert_eq!(retry.id, 2);
    assert_eq!(
        retry.params.as_ref().unwrap()["clientId"],
        abandoned.params.as_ref().unwrap()["clientId"]
    );
    let init =
        json!({"protocolVersion": ahp_types::PROTOCOL_VERSION, "serverSeq": 10, "snapshots": []});
    second_peer.reply(abandoned.id, init.clone()).await;
    let heartbeat = second_ping.ping();
    tokio::pin!(heartbeat);
    let barrier = tokio::select! {
        result = &mut heartbeat => panic!("premature result: {result:?}"),
        sent = second_peer.request() => sent,
    };
    assert_eq!(
        barrier.method, "ping",
        "stale handshake must not start discovery"
    );
    assert_eq!(barrier.id, 3);
    second_peer.reply(barrier.id, Value::Null).await;
    bounded(heartbeat).await.unwrap();
    assert!(second_peer.rx.try_recv().is_err());
    assert!(multi.client(&HostId::from("retained")).await.is_none());

    second_peer.reply(retry.id, init).await;
    let discovery = second_peer.request().await;
    assert_eq!(discovery.method, "listSessions");
    assert_eq!(discovery.id, 4);
    second_peer.reply(discovery.id, json!({"items": []})).await;
    loop {
        if matches!(
            bounded(events.recv()).await,
            Some(HostEvent::Connected { .. })
        ) {
            break;
        }
    }
    let unanswered = second_ping.ping();
    tokio::pin!(unanswered);
    let old_ping = tokio::select! {
        result = &mut unanswered => panic!("premature result: {result:?}"),
        sent = second_peer.request() => sent,
    };
    assert_eq!(old_ping.id, 5);
    drop(second_peer.tx);
    assert!(matches!(
        bounded(unanswered).await,
        Err(ClientError::Shutdown)
    ));
    assert!(bounded(second_peer.rx.recv()).await.is_none());

    let third_ping = third_peer.bound().await;
    let reconnect = third_peer.request().await;
    assert_eq!(reconnect.method, "reconnect");
    assert_eq!(reconnect.id, 6);
    assert_eq!(
        reconnect.params.as_ref().unwrap()["clientId"],
        retry.params.as_ref().unwrap()["clientId"]
    );
    third_peer.reply(old_ping.id, Value::Null).await;
    let heartbeat = third_ping.ping();
    tokio::pin!(heartbeat);
    let barrier = tokio::select! {
        result = &mut heartbeat => panic!("premature result: {result:?}"),
        sent = third_peer.request() => sent,
    };
    assert_eq!(
        barrier.method, "ping",
        "stale ping must not satisfy reconnect"
    );
    assert_eq!(barrier.id, 7);
    third_peer.reply(barrier.id, Value::Null).await;
    bounded(heartbeat).await.unwrap();
    assert!(third_peer.rx.try_recv().is_err());
    third_peer
        .reply(
            reconnect.id,
            json!({"type": "replay", "actions": [], "missing": []}),
        )
        .await;
    let discovery = third_peer.request().await;
    assert_eq!(discovery.method, "listSessions");
    assert_eq!(discovery.id, 8);
    third_peer.reply(discovery.id, json!({"items": []})).await;
    loop {
        if matches!(
            bounded(events.recv()).await,
            Some(HostEvent::Connected { .. })
        ) {
            break;
        }
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 4);
    for count in [
        &first_peer.bind_count,
        &second_peer.bind_count,
        &third_peer.bind_count,
    ] {
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
    bounded(multi.remove_host(&HostId::from("retained")))
        .await
        .unwrap();
    assert!(bounded(third_peer.rx.recv()).await.is_none());
    assert!(matches!(
        third_ping.ping().await,
        Err(ClientError::Shutdown)
    ));
}

#[tokio::test]
async fn independent_managed_hosts_start_independent_id_sequences() {
    let multi = MultiHostClient::new();
    for id in ["first", "second"] {
        let (transport, mut peer) = pair();
        let transport = Arc::new(Mutex::new(Some(transport)));
        let config = HostConfig::new(id, id, move |_| {
            let transport = transport.clone();
            async move { Ok(BoxedTransport::new(transport.lock().await.take().unwrap())) }
        })
        .with_client_config(no_timeout())
        .with_reconnect_policy(ReconnectPolicy::disabled());
        multi.add_host(config).await.unwrap();
        assert_eq!(peer.request().await.id, 1);
        bounded(multi.remove_host(&HostId::from(id))).await.unwrap();
        assert!(bounded(peer.rx.recv()).await.is_none());
    }
}
