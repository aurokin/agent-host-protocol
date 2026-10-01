# ahp

Async Rust client for the [Agent Host Protocol (AHP)](https://github.com/microsoft/agent-host-protocol).

[![crates.io](https://img.shields.io/crates/v/ahp.svg)](https://crates.io/crates/ahp)
[![docs.rs](https://img.shields.io/docsrs/ahp)](https://docs.rs/ahp)

Transport-agnostic SDK that builds on [`ahp-types`](https://crates.io/crates/ahp-types). Bring your own transport — WebSocket, stdio, TCP, or an in-memory channel pair for tests.

## Features

- **[`Client`](https://docs.rs/ahp/latest/ahp/client/struct.Client.html)** — async JSON-RPC client with action subscription, write-ahead dispatch, and background I/O task
- **[`reducers`](https://docs.rs/ahp/latest/ahp/reducers/)** — pure state reducers; apply `StateAction`s to `RootState` / `SessionState` / terminal state
- **[`Transport`](https://docs.rs/ahp/latest/ahp/transport/trait.Transport.html)** — pluggable trait for any framed message stream

## Usage

```toml
[dependencies]
ahp = "0.1"
ahp-ws = "0.1"   # or bring your own transport
tokio = { version = "1", features = ["full"] }
```

```rust
use ahp::{Client, ClientConfig, SubscriptionEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let transport = ahp_ws::WebSocketTransport::connect("ws://localhost:12345").await?;
    let client = Client::connect(transport, ClientConfig::default()).await?;

    client.initialize("my-client".into(), vec![ahp_types::PROTOCOL_VERSION.to_string()], vec![ahp_types::ROOT_RESOURCE_URI.to_string()]).await?;

    let mut sub = client.attach_subscription(ahp_types::ROOT_RESOURCE_URI).await;
    while let Some(SubscriptionEvent::Action(a)) = sub.recv().await {
        println!("seq={} action={:?}", a.server_seq, a.action);
    }

    client.shutdown().await;
    Ok(())
}
```

## Custom transport

Implement `ahp::Transport` for any framed byte stream:

```rust
use ahp::{Transport, TransportError, TransportMessage};
use std::future::Future;

struct MyTransport { /* ... */ }

impl Transport for MyTransport {
    fn send(&mut self, msg: TransportMessage)
        -> impl Future<Output = Result<(), TransportError>> + Send
    { async { todo!() } }

    fn recv(&mut self)
        -> impl Future<Output = Result<Option<TransportMessage>, TransportError>> + Send
    { async { todo!() } }
}
```

See `tests/client_roundtrip.rs` for a complete in-memory example.

### Transport-owned keepalive

Override the optional synchronous
`Transport::bind_client(&mut self, ping: ahp::WeakPingHandle)` callback to pass
the handle to a transport-owned keepalive task. `Client::connect` binds it once,
before transport I/O starts or any `initialize` / `reconnect` request can be
sent. `BoxedTransport` forwards the callback, including for managed hosts.
Subsequent handshakes on the same client do not bind it again.

`WeakPingHandle::ping().await` uses the same root-channel request, ID allocator,
response map, and configured timeout as `Client::ping`. It can therefore keep an
initialized connection alive while session discovery is still pending. The
transport owns the keepalive timing and any negotiation or activity gating;
binding does not start keepalive.

Neither a retained handle nor an in-flight weak ping owns the client driver.
Explicit shutdown, transport closure, or dropping the last `Client` resolves
weak pings with `ClientError::Shutdown`. Request timeouts remain
`ClientError::Cancelled`, and server errors remain `ClientError::Rpc`. Cancelling
a request future removes its pending response entry without retracting an
already-sent request. Existing transports need no changes: binding defaults to
a no-op on both `Transport` and the object-safe `DynTransport` adapter.

Managed hosts retain their request ID allocator across connection attempts for
the lifetime of one host supervisor, including failed handshakes. Late replies
from an earlier transport cannot match a newly allocated request on that
logical host. Independent hosts and standalone `Client::connect` calls still
start independent ID sequences. Exhausting the `u64` request ID space fails
explicitly with `ClientError::Transport(TransportError::Protocol(_))` rather
than reusing an ID.

## See also

- [`ahp-types`](https://crates.io/crates/ahp-types) — wire types only (no I/O)
- [`ahp-ws`](https://crates.io/crates/ahp-ws) — WebSocket transport
- [Connecting to multiple hosts](https://github.com/microsoft/agent-host-protocol/blob/main/clients/rust/MULTI_HOST.md) — the [`hosts`](https://docs.rs/ahp/latest/ahp/hosts/) module wraps multi-host registry, reconnect, fan-in, and aggregated views; single-host consumers use `MultiHostClient::single`
- [Protocol documentation](https://microsoft.github.io/agent-host-protocol/)
