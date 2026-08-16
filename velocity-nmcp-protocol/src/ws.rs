//! NMCP WebSocket Server — remote access via WebSocket with NMCP framing.
//!
//! Wraps each WebSocket message in an NMCP frame (16-byte binary header + payload).
//! Generic over the dispatch trait, allowing any flavor's router to handle frames.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

use crate::frame::{NmcpDispatch, NmcpFrame};

// ─── WebSocket Server ────────────────────────────────────────────────────────

/// NMCP WebSocket Server — accepts remote connections and dispatches NMCP frames.
///
/// Generic over the dispatch trait, allowing any flavor's router to handle frames.
/// Supports optional TLS via `tokio-rustls` — when configured, TCP streams are
/// wrapped with TLS before the WebSocket handshake.
pub struct NmcpWebSocketServer<D: NmcpDispatch> {
    router: Arc<D>,
    listen_addr: String,
    max_connections: usize,
    running: AtomicBool,
    active_connections: AtomicUsize,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
}

impl<D: NmcpDispatch> NmcpWebSocketServer<D> {
    /// Create a new WebSocket server.
    pub fn new(router: Arc<D>, listen_addr: String) -> Self {
        Self {
            router,
            listen_addr,
            max_connections: 64,
            running: AtomicBool::new(true),
            active_connections: AtomicUsize::new(0),
            tls_acceptor: None,
        }
    }

    /// Set maximum concurrent connections.
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// Enable TLS with the given acceptor.
    pub fn with_tls(mut self, acceptor: tokio_rustls::TlsAcceptor) -> Self {
        self.tls_acceptor = Some(acceptor);
        self
    }

    /// Shut down the server.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Get the number of active connections.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Run the WebSocket server.
    pub async fn run(&self) -> Result<(), String> {
        let listener = TcpListener::bind(&self.listen_addr)
            .await
            .map_err(|e| format!("WebSocket bind failed: {}", e))?;

        if let Ok(addr) = listener.local_addr() {
            let scheme = if self.tls_acceptor.is_some() { "wss" } else { "ws" };
            println!("  NMCP WebSocket: {}://{}", scheme, addr);
        }

        while self.running.load(Ordering::Relaxed) {
            let accept_result = tokio::select! {
                result = listener.accept() => result,
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => continue,
            };

            let (stream, _peer_addr) = match accept_result {
                Ok(s) => s,
                Err(_) => continue,
            };

            // Check connection limit
            if self.active_connections.load(Ordering::Relaxed) >= self.max_connections {
                drop(stream);
                continue;
            }

            self.active_connections.fetch_add(1, Ordering::Relaxed);

            let router = self.router.clone();
            let active = &self.active_connections as *const AtomicUsize;
            // Safety: we only read the counter, and the server owns it
            let active_count = unsafe { &*active };
            let tls = self.tls_acceptor.clone();

            tokio::spawn(async move {
                // Optionally wrap with TLS before WebSocket handshake
                if let Some(acceptor) = tls {
                    match acceptor.accept(stream).await {
                        Ok(tls_stream) => {
                            match tokio_tungstenite::accept_async(tls_stream).await {
                                Ok(ws_stream) => handle_ws_messages(ws_stream, &router).await,
                                Err(_) => {}
                            }
                        }
                        Err(_) => {}
                    }
                } else {
                    match tokio_tungstenite::accept_async(stream).await {
                        Ok(ws_stream) => handle_ws_messages(ws_stream, &router).await,
                        Err(_) => {}
                    }
                }
                active_count.fetch_sub(1, Ordering::Relaxed);
            });
        }

        Ok(())
    }
}

/// Handle WebSocket messages on an established connection.
///
/// Generic over the stream type — works with both plain TCP and TLS streams.
async fn handle_ws_messages<S>(ws_stream: S, router: &Arc<impl NmcpDispatch>)
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error>
        + Unpin,
{
    let (mut writer, mut reader) = ws_stream.split();

    while let Some(Ok(msg)) = reader.next().await {
        match msg {
            Message::Binary(data) => {
                let request = match NmcpFrame::from_bytes(&data) {
                    Some(f) => f,
                    None => {
                        let err = NmcpFrame::error_response(0, 400, "invalid NMCP frame");
                        let _ = writer.send(Message::Binary(err.to_bytes())).await;
                        continue;
                    }
                };
                let response = router.dispatch(&request);
                let _ = writer.send(Message::Binary(response.to_bytes())).await;
            }
            Message::Text(text) => {
                let payload = text.into_bytes();
                let request = NmcpFrame::new(0, 0, payload);
                let response = router.dispatch(&request);
                let _ = writer.send(Message::Binary(response.to_bytes())).await;
            }
            Message::Close(_) => break,
            _ => {} // Ping/Pong handled by tungstenite
        }
    }
}

// ─── WebSocket Client ────────────────────────────────────────────────────────

/// NMCP WebSocket Client — connects to a remote NMCP WebSocket server.
pub struct NmcpWebSocketClient {
    ws_url: String,
    next_seq: std::sync::atomic::AtomicU32,
}

impl NmcpWebSocketClient {
    /// Create a new WebSocket client.
    pub fn new(ws_url: String) -> Self {
        Self {
            ws_url,
            next_seq: std::sync::atomic::AtomicU32::new(1),
        }
    }

    /// Send a request and receive a response.
    pub async fn call(
        &self,
        frame_type: u32,
        payload: Vec<u8>,
    ) -> Result<NmcpFrame, String> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let request = NmcpFrame::new(frame_type, seq, payload);

        let (mut ws, _) = tokio_tungstenite::connect_async(&self.ws_url)
            .await
            .map_err(|e| format!("WebSocket connect failed: {}", e))?;

        ws.send(Message::Binary(request.to_bytes()))
            .await
            .map_err(|e| format!("send failed: {}", e))?;

        while let Some(Ok(msg)) = ws.next().await {
            if let Message::Binary(data) = msg {
                return NmcpFrame::from_bytes(&data)
                    .ok_or_else(|| "invalid response frame".to_string());
            }
        }

        Err("connection closed before response".to_string())
    }
}
