#![allow(clippy::panic, clippy::unwrap_used)]

use super::*;
use crate::BoxedTransport;

struct TestTransport {
    sent: mpsc::Sender<TransportMessage>,
    received: mpsc::Receiver<TransportMessage>,
    bound: Option<WeakPingHandle>,
}

impl Transport for TestTransport {
    fn bind_client(&mut self, ping: WeakPingHandle) {
        self.bound = Some(ping);
    }

    async fn send(&mut self, message: TransportMessage) -> Result<(), TransportError> {
        self.sent
            .send(message)
            .await
            .map_err(|_| TransportError::Closed)
    }

    async fn recv(&mut self) -> Result<Option<TransportMessage>, TransportError> {
        Ok(self.received.recv().await)
    }
}

async fn client(
    config: ClientConfig,
) -> (
    Client,
    mpsc::Receiver<TransportMessage>,
    mpsc::Sender<TransportMessage>,
) {
    let (sent, rx) = mpsc::channel(1);
    let (tx, received) = mpsc::channel(1);
    let transport = TestTransport {
        sent,
        received,
        bound: None,
    };
    (
        Client::connect(BoxedTransport::new(transport), config)
            .await
            .unwrap(),
        rx,
        tx,
    )
}

#[tokio::test]
async fn cancelled_and_timed_out_requests_remove_pending_entries() {
    let (client, mut sent, _received) = client(ClientConfig {
        default_request_timeout: None,
        ..ClientConfig::default()
    })
    .await;
    let weak = WeakPingHandle {
        shared: Arc::downgrade(&client.shared),
    };
    for is_weak in [false, true] {
        let mut request = Box::pin(async {
            if is_weak {
                weak.ping().await
            } else {
                client.ping().await
            }
        });
        tokio::select! {
            result = &mut request => panic!("premature result: {result:?}"),
            _ = sent.recv() => {},
        }
        assert_eq!(client.shared.pending.lock().unwrap().len(), 1);
        drop(request);
        assert!(client.shared.pending.lock().unwrap().is_empty());
    }
    drop(client);
    assert!(tokio::time::timeout(Duration::from_secs(2), sent.recv())
        .await
        .unwrap()
        .is_none());
    assert!(
        weak.shared.upgrade().is_none(),
        "Shared leaked after driver drop"
    );

    let (client, mut sent, _received) = self::client(ClientConfig {
        default_request_timeout: Some(Duration::ZERO),
        ..ClientConfig::default()
    })
    .await;
    let weak = WeakPingHandle {
        shared: Arc::downgrade(&client.shared),
    };
    assert!(matches!(weak.ping().await, Err(ClientError::Cancelled)));
    assert!(client.shared.pending.lock().unwrap().is_empty());
    assert!(sent.recv().await.is_some());
    assert!(matches!(client.ping().await, Err(ClientError::Cancelled)));
    assert!(client.shared.pending.lock().unwrap().is_empty());
    assert!(sent.recv().await.is_some());
    drop(client);
    assert!(tokio::time::timeout(Duration::from_secs(2), sent.recv())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn owner_drop_clears_unanswered_weak_request_and_releases_shared_allocator() {
    let (client, mut sent, _received) = client(ClientConfig {
        default_request_timeout: None,
        ..ClientConfig::default()
    })
    .await;
    let weak = WeakPingHandle {
        shared: Arc::downgrade(&client.shared),
    };
    let ids = Arc::downgrade(&client.shared.request_ids);
    let mut request = Box::pin(weak.ping());
    tokio::select! {
        result = &mut request => panic!("premature result: {result:?}"),
        _ = sent.recv() => {},
    }
    drop(client);
    assert!(matches!(request.await, Err(ClientError::Shutdown)));
    assert!(tokio::time::timeout(Duration::from_secs(2), sent.recv())
        .await
        .unwrap()
        .is_none());
    assert!(weak.shared.upgrade().is_none());
    assert!(ids.upgrade().is_none());
}

#[tokio::test]
async fn request_id_exhaustion_never_wraps_or_enqueues_another_request() {
    let (client, mut sent, _received) = client(ClientConfig {
        default_request_timeout: Some(Duration::ZERO),
        ..ClientConfig::default()
    })
    .await;
    *client.shared.request_ids.next.lock().unwrap() = Some(u64::MAX);
    let weak = WeakPingHandle {
        shared: Arc::downgrade(&client.shared),
    };
    assert!(matches!(weak.ping().await, Err(ClientError::Cancelled)));
    let JsonRpcMessage::Request(request) = sent.recv().await.unwrap().into_parsed().unwrap() else {
        panic!("expected request");
    };
    assert_eq!(request.id, u64::MAX);
    for is_weak in [false, true] {
        let result = if is_weak {
            weak.ping().await
        } else {
            client.ping().await
        };
        assert!(
            matches!(result, Err(ClientError::Transport(TransportError::Protocol(message))) if message == "request ID space exhausted")
        );
        assert!(client.shared.pending.lock().unwrap().is_empty());
        assert!(sent.try_recv().is_err());
    }
    assert!(client.shared.request_ids.next.lock().unwrap().is_none());
    client.shutdown().await;
    assert!(matches!(weak.ping().await, Err(ClientError::Shutdown)));
    drop(client);
    assert!(tokio::time::timeout(Duration::from_secs(2), sent.recv())
        .await
        .unwrap()
        .is_none());
}

struct BlockedTransport {
    entered: Option<oneshot::Sender<()>>,
    dropped: Option<oneshot::Sender<()>>,
}

impl Transport for BlockedTransport {
    async fn send(&mut self, _: TransportMessage) -> Result<(), TransportError> {
        self.entered.take().unwrap().send(()).unwrap();
        std::future::pending().await
    }

    async fn recv(&mut self) -> Result<Option<TransportMessage>, TransportError> {
        std::future::pending().await
    }
}

impl Drop for BlockedTransport {
    fn drop(&mut self) {
        self.dropped.take().unwrap().send(()).unwrap();
    }
}

#[tokio::test]
async fn owner_drop_cancels_weak_ping_blocked_on_full_outbound_queue() {
    let (entered, send_started) = oneshot::channel();
    let (dropped, transport_dropped) = oneshot::channel();
    let client = Client::connect(
        BoxedTransport::new(BlockedTransport {
            entered: Some(entered),
            dropped: Some(dropped),
        }),
        ClientConfig {
            default_request_timeout: None,
            ..ClientConfig::default()
        },
    )
    .await
    .unwrap();
    let weak = WeakPingHandle {
        shared: Arc::downgrade(&client.shared),
    };
    client.notify("block", ()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), send_started)
        .await
        .unwrap()
        .unwrap();
    for _ in 0..64 {
        client.notify("queued", ()).await.unwrap();
    }
    assert_eq!(client.shared.outbound.capacity(), 0);
    let mut request = Box::pin(weak.ping());
    std::future::poll_fn(|cx| {
        assert!(request.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert_eq!(client.shared.pending.lock().unwrap().len(), 1);
    drop(client);
    assert!(weak
        .shared
        .upgrade()
        .unwrap()
        .pending
        .lock()
        .unwrap()
        .is_empty());
    assert!(matches!(request.await, Err(ClientError::Shutdown)));
    tokio::time::timeout(Duration::from_secs(2), transport_dropped)
        .await
        .unwrap()
        .unwrap();
    assert!(weak.shared.upgrade().is_none());
}
