use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::CONTENT_TYPE;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::sleep;

use keep_sse::config::ResolvedConfig;
use keep_sse::create_client;

/// 启动 mock 上游（Full body 响应），返回 (addr, shutdown)。
async fn start_mock<F>(handler: F) -> (SocketAddr, oneshot::Sender<()>)
where
    F: Fn(Request<Incoming>) -> Response<Full<Bytes>> + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, mut rx) = oneshot::channel::<()>();
    let handler = std::sync::Arc::new(handler);
    tokio::task::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut rx => break,
                accept = listener.accept() => {
                    let (stream, _) = match accept { Ok(v) => v, Err(_) => continue };
                    let io = TokioIo::new(stream);
                    let h = handler.clone();
                    tokio::task::spawn(async move {
                        let _ = http1::Builder::new()
                            .serve_connection(io, service_fn(move |req| {
                                let h = h.clone();
                                async move { Ok::<_, Infallible>(h(req)) }
                            }))
                            .await;
                    });
                }
            }
        }
    });
    (addr, tx)
}

/// 启动可流式发送数据的 mock 上游（原始 TCP）。
/// handler 返回 (status, headers, header_delay_ms, Vec<(data, delay_ms)>) —
/// header_delay_ms 为发送响应头前的延迟；每块发送后 sleep delay_ms。
async fn start_mock_raw<F>(handler: F) -> (SocketAddr, oneshot::Sender<()>)
where
    F: Fn(&[u8], &str) -> (u16, Vec<(&'static str, String)>, u64, Vec<(Vec<u8>, u64)>)
        + Send
        + Sync
        + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, mut rx) = oneshot::channel::<()>();
    let handler = std::sync::Arc::new(handler);
    tokio::task::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut rx => break,
                accept = listener.accept() => {
                    let (mut stream, _) = match accept { Ok(v) => v, Err(_) => continue };
                    let h = handler.clone();
                    tokio::task::spawn(async move {
                        let mut buf = vec![0u8; 8192];
                        let mut req_data = Vec::new();
                        loop {
                            let n = stream.read(&mut buf).await.unwrap();
                            if n == 0 { return; }
                            req_data.extend_from_slice(&buf[..n]);
                            if req_data.windows(4).any(|w| w == b"\r\n\r\n") { break; }
                        }
                        let header_end = req_data.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                        let req_str = String::from_utf8_lossy(&req_data[..header_end]);
                        let path = req_str.lines().next().unwrap().split_whitespace().nth(1).unwrap_or("/");
                        let cl = req_str.lines()
                            .find(|l| l.to_lowercase().starts_with("content-length:"))
                            .and_then(|l| l.split(':').nth(1))
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        let mut body = req_data[header_end..].to_vec();
                        while body.len() < cl {
                            let n = stream.read(&mut buf).await.unwrap();
                            if n == 0 { break; }
                            body.extend_from_slice(&buf[..n]);
                        }
                        let (status, headers, header_delay_ms, chunks) = h(&body, path);
                        if header_delay_ms > 0 {
                            sleep(Duration::from_millis(header_delay_ms)).await;
                        }
                        let mut resp = format!("HTTP/1.1 {}\r\n", status);
                        for (k, v) in &headers {
                            resp.push_str(&format!("{}: {}\r\n", k, v));
                        }
                        resp.push_str("transfer-encoding: chunked\r\n\r\n");
                        stream.write_all(resp.as_bytes()).await.unwrap();
                        for (chunk, delay_ms) in &chunks {
                            if *delay_ms > 0 {
                                sleep(Duration::from_millis(*delay_ms)).await;
                            }
                            stream.write_all(format!("{:x}\r\n", chunk.len()).as_bytes()).await.unwrap();
                            stream.write_all(chunk).await.unwrap();
                            stream.write_all(b"\r\n").await.unwrap();
                            stream.flush().await.unwrap();
                        }
                        stream.write_all(b"0\r\n\r\n").await.unwrap();
                        stream.flush().await.unwrap();
                    });
                }
            }
        }
    });
    (addr, tx)
}

/// 启动网关（默认配置）。
async fn start_gateway(upstream: SocketAddr, heartbeat: u64) -> (SocketAddr, oneshot::Sender<()>) {
    start_gateway_with(test_cfg(upstream, heartbeat, 32 * 1024 * 1024)).await
}

/// 构造测试用 ResolvedConfig。
fn test_cfg(upstream: SocketAddr, heartbeat: u64, max_probe_body: usize) -> ResolvedConfig {
    let upstream_str = format!("http://{}", upstream);
    ResolvedConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        upstream: upstream_str.parse().unwrap(),
        upstream_authority: upstream.to_string(),
        heartbeat_interval: Duration::from_secs(heartbeat),
        connect_timeout: Duration::from_secs(5),
        max_probe_body,
        shutdown_timeout: Duration::from_secs(30),
        anthropic_late_error_type: None,
        hold_window: Duration::ZERO,
        retry_deadline: Duration::from_secs(180),
        max_stream_retries: 3,
        hold_max_bytes: 8 * 1024 * 1024,
        hold_max_body: 16 * 1024 * 1024,
    }
}

/// 启动网关（自定义配置），使用 server::run 驱动，返回 shutdown 触发端。
async fn start_gateway_with(cfg: ResolvedConfig) -> (SocketAddr, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = ResolvedConfig {
        listen: addr,
        ..cfg
    };
    let client = create_client(&cfg);
    let (tx, rx) = oneshot::channel::<()>();
    tokio::task::spawn(async move {
        keep_sse::server::run(cfg, client, listener, async move {
            let _ = rx.await;
        })
        .await;
    });
    sleep(Duration::from_millis(50)).await;
    (addr, tx)
}

/// 发送 HTTP 请求并读取完整响应（原始字节）。
async fn send_raw(addr: SocketAddr, req: &str) -> Vec<u8> {
    send_raw_bytes(addr, req.as_bytes()).await
}

async fn send_raw_bytes(addr: SocketAddr, req: &[u8]) -> Vec<u8> {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    // Insert Connection: close if not present (so read_to_end terminates).
    if !req.windows(10).any(|w| w == b"Connection") {
        let pos = req
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .unwrap_or(req.len());
        let mut buf = Vec::with_capacity(req.len() + 20);
        buf.extend_from_slice(&req[..pos]);
        buf.extend_from_slice(b"\r\nConnection: close");
        buf.extend_from_slice(&req[pos..]);
        stream.write_all(&buf).await.unwrap();
    } else {
        stream.write_all(req).await.unwrap();
    }
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.unwrap();
    resp
}

/// 解析 HTTP 响应：返回 (status, headers, body)。
fn parse_response(raw: &[u8]) -> (u16, Vec<(String, String)>, Vec<u8>) {
    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap_or(raw.len());
    let header_str = String::from_utf8_lossy(&raw[..header_end]);
    let mut lines = header_str.lines();
    let status: u16 = lines
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut headers = Vec::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(": ") {
            headers.push((k.to_lowercase(), v.to_string()));
        }
    }
    let body = if header_end + 4 <= raw.len() {
        raw[header_end + 4..].to_vec()
    } else {
        Vec::new()
    };
    (status, headers, body)
}

// === Tests ===

#[tokio::test]
async fn test_transparent_get() {
    let (upstream, tx) = start_mock(|req| {
        let body = format!("GET {}", req.uri().path());
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/plain")
            .body(Full::new(Bytes::from(body)))
            .unwrap()
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let resp = send_raw(gw, "GET /foo HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
    let (status, _headers, body) = parse_response(&resp);
    assert_eq!(status, 200);
    assert_eq!(&body[..], b"GET /foo");

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_transparent_post_with_gzip_body() {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    let payload = br#"{"hello":"world"}"#;
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    gz.write_all(payload).unwrap();
    let compressed = gz.finish().unwrap();

    let (upstream, tx) = start_mock(move |req| {
        let body = Full::new(Bytes::from(format!("echo:{}", req.uri().path())));
        Response::builder()
            .status(StatusCode::OK)
            .body(body)
            .unwrap()
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let req = format!(
        "POST /api/data HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Encoding: gzip\r\n\r\n",
        compressed.len()
    );
    let mut req = req.into_bytes();
    req.extend_from_slice(&compressed);
    let resp = send_raw_bytes(gw, &req).await;
    let (status, _, _) = parse_response(&resp);
    assert_eq!(status, 200);

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_stream_false_goes_transparent() {
    let (upstream, tx) = start_mock(|_req| {
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from_static(b"{\"id\":\"chatcmpl-1\"}")))
            .unwrap()
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let body = r#"{"model":"x","stream":false,"messages":[]}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    // Transparent path: Content-Type is application/json (not text/event-stream)
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str()),
        Some("application/json")
    );
    assert_eq!(&resp_body, b"{\"id\":\"chatcmpl-1\"}");

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_stream_true_chat_completions() {
    let (upstream, tx) = start_mock_raw(|_body, _path| {
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            0,
            vec![
                (
                    b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n".to_vec(),
                    0,
                ),
                (b"data: [DONE]\n\n".to_vec(), 50),
            ],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let body = r#"{"model":"x","stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    // Fast SSE passthrough: content-type is upstream's original value
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str()),
        Some("text/event-stream")
    );
    let body_str = String::from_utf8_lossy(&resp_body);
    assert!(body_str.contains("data: {\"choices\""));
    assert!(body_str.contains("[DONE]"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_accept_event_stream_header_goes_sse() {
    let (upstream, tx) = start_mock_raw(|_body, _path| {
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            0,
            vec![
                (
                    b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n".to_vec(),
                    0,
                ),
                (b"data: [DONE]\n\n".to_vec(), 50),
            ],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    // body 显式 stream:false，但带 Accept: text/event-stream 头 → 走 SSE 通路
    let body = r#"{"model":"x","stream":false}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    // SSE 通路：上游 content-type 原样透传
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str()),
        Some("text/event-stream")
    );
    let body_str = String::from_utf8_lossy(&resp_body);
    assert!(body_str.contains("data: {\"choices\""));
    assert!(body_str.contains("[DONE]"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_stream_true_all_four_endpoints() {
    for (path, body, _expect_event) in &[
        (
            "/v1/chat/completions",
            r#"{"stream":true}"#,
            "data: {\"choices\"",
        ),
        ("/v1/responses", r#"{"stream":true}"#, "data: {\"type\""),
        ("/v1/messages", r#"{"stream":true}"#, "data: {\"type\""),
        (
            "/v1beta/models/gemini-pro:streamGenerateContent?alt=sse",
            r#"{}"#,
            "data: {\"candidates\"",
        ),
    ] {
        let (upstream, tx) = start_mock_raw(|_b, _p| {
            (
                200,
                vec![("content-type", "text/event-stream".to_string())],
                0,
                vec![(
                    b"data: {\"type\":\"message\",\"choices\":[],\"candidates\":[]}\n\n".to_vec(),
                    0,
                )],
            )
        })
        .await;
        let (gw, gw_tx) = start_gateway(upstream, 60).await;

        let req = format!(
            "POST {} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
            path, body.len(), body
        );
        let resp = send_raw(gw, &req).await;
        let (status, headers, resp_body) = parse_response(&resp);
        assert_eq!(status, 200, "path {} should return 200", path);
        let ct = headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str());
        assert_eq!(ct, Some("text/event-stream"), "path {} content-type", path);
        let body_str = String::from_utf8_lossy(&resp_body);
        assert!(
            body_str.contains("data: "),
            "path {} should have SSE data event",
            path
        );

        tx.send(()).unwrap();
        gw_tx.send(()).unwrap();
    }
}

#[tokio::test]
async fn test_heartbeat_during_delay() {
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            0,
            vec![
                (b"data: hello\n\n".to_vec(), 2500), // 2.5s delay before first data
            ],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 1).await; // 1s heartbeat

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, _, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    let heartbeats = resp_body.windows(3).filter(|w| w == b":\n\n").count();
    // Should have at least 2 heartbeats before the data arrives
    assert!(
        heartbeats >= 2,
        "expected >= 2 heartbeats, got {} in {:?}",
        heartbeats,
        String::from_utf8_lossy(&resp_body)
    );
    assert!(
        resp_body.windows(5).any(|w| w == b"hello"),
        "should contain hello in {:?}",
        String::from_utf8_lossy(&resp_body)
    );

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_fast_429_passthrough() {
    let err = r#"{"error":{"message":"rate limited","type":"rate_limit_error","param":null,"code":null}}"#;
    let (upstream, tx) = start_mock(move |_req| {
        Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from_static(err.as_bytes())))
            .unwrap()
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    // Fast 429 → complete passthrough (status, headers, body)
    assert_eq!(status, 429);
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str()),
        Some("application/json")
    );
    assert_eq!(&resp_body[..], err.as_bytes());
    let body_str = String::from_utf8_lossy(&resp_body);
    assert!(!body_str.contains("[DONE]"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_upstream_connection_failure_sse() {
    // Bind and immediately close to get a port that's not listening.
    let addr = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();

    let (gw, gw_tx) = start_gateway(addr, 60).await;

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, _, resp_body) = parse_response(&resp);
    assert_eq!(status, 502); // TCP error → 502 Bad Gateway
    let body_str = String::from_utf8_lossy(&resp_body);
    assert!(body_str.contains("upstream request failed"));
    assert!(!body_str.contains("[DONE]"));

    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_upstream_connection_failure_transparent() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let (gw, gw_tx) = start_gateway(addr, 60).await;

    let resp = send_raw(gw, "GET /test HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
    let (status, _, resp_body) = parse_response(&resp);
    assert_eq!(status, 502);
    let body_str = String::from_utf8_lossy(&resp_body);
    assert!(body_str.contains("upstream request failed"));

    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_gzip_stream_request_body() {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    let payload = r#"{"model":"x","stream":true}"#;
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    gz.write_all(payload.as_bytes()).unwrap();
    let compressed = gz.finish().unwrap();

    let received_body = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
    let rb = received_body.clone();
    let (upstream, tx) = start_mock_raw(move |body, _path| {
        *rb.lock() = body.to_vec();
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            0,
            vec![(b"data: hi\n\n".to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Encoding: gzip\r\nContent-Type: application/json\r\n\r\n",
        compressed.len()
    );
    let mut req = req.into_bytes();
    req.extend_from_slice(&compressed);
    let resp = send_raw_bytes(gw, &req).await;
    let (status, _, _) = parse_response(&resp);
    assert_eq!(status, 200);

    // Verify upstream received the original compressed bytes
    let rb = received_body.lock();
    assert_eq!(
        &rb[..],
        &compressed[..],
        "upstream should receive original compressed body"
    );

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_accept_encoding_passthrough() {
    let (upstream, tx) =
        start_mock_raw(|_body, _path| (200, vec![], 0, vec![(b"ok".to_vec(), 0)])).await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    // Transparent path with Accept-Encoding
    let resp = send_raw(
        gw,
        "GET /test HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: gzip, br\r\n\r\n",
    )
    .await;
    let (status, _, _) = parse_response(&resp);
    assert_eq!(status, 200);

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_gemini_without_alt_sse_goes_transparent() {
    let (upstream, tx) = start_mock(|_req| {
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from_static(b"[{\"candidates\":[]}]")))
            .unwrap()
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let body = r#"{}"#;
    let req = format!(
        "POST /v1beta/models/gemini-pro:streamGenerateContent HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, _resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    // No alt=sse → transparent, content-type from upstream
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str()),
        Some("application/json")
    );

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_slow_upstream_gateway_200() {
    // Upstream header delay > heartbeat window → gateway sends its own 200 SSE.
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            1500, // 1.5s header delay > 1s heartbeat window
            vec![(b"data: hello\n\n".to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 1).await; // 1s heartbeat

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    // Gateway's own content-type (with charset)
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str()),
        Some("text/event-stream; charset=utf-8")
    );
    let body_str = String::from_utf8_lossy(&resp_body);
    assert!(body_str.contains("hello"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_slow_upstream_429_error_event() {
    // Upstream header delay > window and non-2xx → SSE error event via gateway 200.
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            429,
            vec![("content-type", "application/json".to_string())],
            1500, // 1.5s header delay > 1s window
            vec![(b"{\"error\":\"rate limited\"}".to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 1).await; // 1s heartbeat

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, _, resp_body) = parse_response(&resp);
    assert_eq!(status, 200); // Gateway 200 (window timeout)
    let body_str = String::from_utf8_lossy(&resp_body);
    assert!(body_str.contains("rate limited"));
    assert!(body_str.contains("[DONE]"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_fast_sse_passthrough_headers_and_heartbeat() {
    // Fast 2xx SSE with x-request-id header, data delayed 2.5s → header passthrough + heartbeats.
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![
                ("content-type", "text/event-stream".to_string()),
                ("x-request-id", "test-123".to_string()),
            ],
            0,
            vec![(b"data: hello\n\n".to_vec(), 2500)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 1).await; // 1s heartbeat

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    // x-request-id should be passed through
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "x-request-id")
            .map(|(_, v)| v.as_str()),
        Some("test-123")
    );
    let heartbeats = resp_body.windows(3).filter(|w| w == b":\n\n").count();
    assert!(
        heartbeats >= 2,
        "expected >= 2 heartbeats, got {} in {:?}",
        heartbeats,
        String::from_utf8_lossy(&resp_body)
    );
    assert!(
        resp_body.windows(5).any(|w| w == b"hello"),
        "should contain hello in {:?}",
        String::from_utf8_lossy(&resp_body)
    );

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_fast_json_response_pure_passthrough() {
    // Fast 2xx application/json for stream:true request → pure passthrough (no SSE wrapping).
    let json_body = br#"{"id":"chatcmpl-1","object":"chat.completion","choices":[]}"#;
    let (upstream, tx) = start_mock(move |_req| {
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from_static(json_body)))
            .unwrap()
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 1).await;

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str()),
        Some("application/json")
    );
    // Body is byte-for-byte identical to upstream
    assert_eq!(&resp_body[..], json_body);
    // No heartbeat bytes
    assert!(!resp_body.windows(3).any(|w| w == b":\n\n"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

/// 解析 chunked transfer-encoding body，返回解码后的字节。
fn dechunk(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < input.len() {
        let line_end = input[pos..]
            .iter()
            .position(|&b| b == b'\n')
            .map(|i| pos + i)
            .unwrap_or(input.len());
        let size_str = std::str::from_utf8(&input[pos..line_end]).unwrap().trim();
        let size = usize::from_str_radix(size_str, 16).unwrap();
        pos = line_end + 1;
        if size == 0 {
            break;
        }
        out.extend_from_slice(&input[pos..pos + size]);
        pos += size + 2; // skip \r\n
    }
    out
}

#[tokio::test]
async fn test_sse_downstream_identity() {
    // 慢数据上游：延迟 2.5s（触发心跳），再发数据
    // 客户端发 Accept-Encoding: gzip，但 SSE 下行一律 identity（不压缩、不写头）
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            2500,
            vec![
                (b"data: hello\n\n".to_vec(), 0),
                (b"data: world\n\n".to_vec(), 0),
            ],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 1).await;

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: gzip\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    // 下行不压缩：无 Content-Encoding 头
    assert_eq!(
        headers.iter().find(|(k, _)| k == "content-encoding"),
        None,
        "expected no content-encoding header, got {:?}",
        headers
    );

    let body = dechunk(&resp_body);
    let body_str = String::from_utf8_lossy(&body);
    // 心跳与数据事件都在，且为明文
    assert!(body_str.contains("hello"));
    assert!(body_str.contains("world"));
    let heartbeats = body_str.matches(":\n\n").count();
    assert!(
        heartbeats >= 1,
        "expected >= 1 heartbeat, got {} in {:?}",
        heartbeats,
        body_str
    );

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_undecodable_encoding_passthrough() {
    // 窗口内 2xx SSE + Content-Encoding: snappy → 完整透传
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![
                ("content-type", "text/event-stream".to_string()),
                ("content-encoding", "snappy".to_string()),
            ],
            0,
            vec![(b"data: hi\n\n".to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    // snappy 不可解码 → 透传上游 content-encoding
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-encoding")
            .map(|(_, v)| v.as_str()),
        Some("snappy")
    );
    let body = dechunk(&resp_body);
    assert_eq!(&body[..], b"data: hi\n\n");

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_probe_decompress_bomb_goes_transparent() {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    // 压缩后 < 1 KiB、解压后 > 1 KiB 的 gzip 体
    let payload = "x".repeat(2048);
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    gz.write_all(payload.as_bytes()).unwrap();
    let compressed = gz.finish().unwrap();
    assert!(compressed.len() < 1024);

    let received_body = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
    let rb = received_body.clone();
    let (upstream, tx) = start_mock_raw(move |body, _path| {
        *rb.lock() = body.to_vec();
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            0,
            vec![(b"data: ok\n\n".to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway_with(test_cfg(upstream, 60, 1024)).await;

    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Encoding: gzip\r\nContent-Type: application/json\r\n\r\n",
        compressed.len()
    );
    let mut req = req.into_bytes();
    req.extend_from_slice(&compressed);
    let resp = send_raw_bytes(gw, &req).await;
    let (status, _, _) = parse_response(&resp);
    assert_eq!(status, 200);

    // 上游应收到原始压缩字节（透明代理）
    let rb = received_body.lock();
    assert_eq!(
        &rb[..],
        &compressed[..],
        "upstream should receive original compressed bytes"
    );

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_error_event_reserialized_single_line() {
    // 慢上游 429 + pretty-printed 错误 JSON → SSE 错误事件 data: 行内无裸换行
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        let body = br#"{
  "error": { "message": "rate limited", "type": "rate_limit_error" }
}"#;
        (
            429,
            vec![("content-type", "application/json".to_string())],
            1500,
            vec![(body.to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 1).await;

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, _, resp_body) = parse_response(&resp);
    assert_eq!(status, 200); // 窗口超时，网关发 200 SSE
    let s = String::from_utf8_lossy(&resp_body);
    assert!(s.contains("data: {\"error\":"));
    assert!(s.contains("\"message\":\"rate limited\""));
    // data: 行内无裸换行（紧凑重序列化）
    for line in s.lines() {
        if line.starts_with("data: ") {
            assert!(
                !line.contains('\n'),
                "data: line should not contain newlines: {line}"
            );
        }
    }
    assert!(s.contains("[DONE]"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_heartbeat_not_injected_mid_event() {
    // 心跳间隔 1s，上游把一个事件拆两 chunk、中间停 2.5s
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            0,
            vec![
                (
                    b"data: {\"choices\":[{\"delta\":{\"content\":\"hel".to_vec(),
                    0,
                ),
                (b"lo\"}}]}\n\n".to_vec(), 2500),
            ],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 1).await;

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    let resp = send_raw(gw, &req).await;
    let (status, _, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    let body = dechunk(&resp_body);
    let s = String::from_utf8_lossy(&body);
    // 事件字节连续完整：hel 后面紧接 lo\"}}]}\n\n，中间无心跳
    let event_start = s.find("data: {\"choices\":").unwrap();
    let event_fragment = &s[event_start..];
    // 心跳 ":\n\n 不应出现在事件中间
    let mid_event_section =
        &event_fragment[..event_fragment.find("\n\n").unwrap_or(event_fragment.len())];
    assert!(
        !mid_event_section.contains(":\n\n"),
        "heartbeat should not appear mid-event: {mid_event_section:?}"
    );
    // 事件完整
    assert!(s.contains("hello"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_graceful_shutdown_drains_stream() {
    // 进行中的慢 SSE 请求 + 触发 shutdown → 该流完整收完
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            0,
            vec![
                (b"data: first\n\n".to_vec(), 500),
                (b"data: second\n\n".to_vec(), 500),
            ],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(), body
    );
    // 不用 Connection: close，让流自然结束
    let mut stream = tokio::net::TcpStream::connect(gw).await.unwrap();
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();

    // 等待第一个数据到达
    tokio::time::sleep(Duration::from_millis(700)).await;
    // 触发 shutdown
    gw_tx.send(()).unwrap();

    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.unwrap();
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("first"), "should contain first event: {s}");
    assert!(
        s.contains("second"),
        "should contain second event after shutdown: {s}"
    );

    // shutdown 后新连接无法建立
    let new_conn = tokio::net::TcpStream::connect(gw).await;
    assert!(
        new_conn.is_err(),
        "new connection after shutdown should fail"
    );

    tx.send(()).unwrap();
}

#[tokio::test]
async fn test_sse_fast_path_identity() {
    // 快路径：上游立即返回未压缩 SSE，客户端接受 gzip, zstd, br, deflate
    // → SSE 下行一律 identity，无 Content-Encoding 头
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            0,
            vec![(b"data: hello\n\n".to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\nAccept-Encoding: gzip, zstd, br, deflate\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(),
        body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    assert_eq!(
        headers.iter().find(|(k, _)| k == "content-encoding"),
        None,
        "expected no content-encoding header, got {:?}",
        headers
    );
    assert_eq!(&dechunk(&resp_body)[..], b"data: hello\n\n");

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_sse_fast_path_upstream_gzip_decoded_to_identity() {
    // 快路径：上游返回真实 gzip 压缩 SSE（Content-Encoding: gzip）
    // 网关解压后以 identity 明文转发 → 无 Content-Encoding 头，body == data: hello\n\n
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    let payload = b"data: hello\n\n";
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    gz.write_all(payload).unwrap();
    let compressed = gz.finish().unwrap();

    let (upstream, tx) = start_mock(move |_req| {
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/event-stream")
            .header("content-encoding", "gzip")
            .body(Full::new(Bytes::from(compressed.clone())))
            .unwrap()
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 60).await;

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\nAccept-Encoding: gzip, zstd, br, deflate\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(),
        body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    assert_eq!(
        headers.iter().find(|(k, _)| k == "content-encoding"),
        None,
        "expected no content-encoding header, got {:?}",
        headers
    );
    assert_eq!(&dechunk(&resp_body)[..], b"data: hello\n\n");

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_sse_slow_path_identity() {
    // 慢路径：上游延迟超过心跳，网关自建 SSE 响应
    // 客户端接受 gzip, zstd, br, deflate → SSE 下行一律 identity，无 Content-Encoding 头
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            2500,
            vec![(b"data: hello\n\n".to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 1).await;

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: gzip, zstd, br, deflate\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(),
        body
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, resp_body) = parse_response(&resp);
    assert_eq!(status, 200);
    assert_eq!(
        headers.iter().find(|(k, _)| k == "content-encoding"),
        None,
        "expected no content-encoding header, got {:?}",
        headers
    );
    let body = dechunk(&resp_body);
    let body_str = String::from_utf8_lossy(&body);
    assert!(body_str.contains("hello"));
    let heartbeats = body_str.matches(":\n\n").count();
    assert!(
        heartbeats >= 1,
        "expected >= 1 heartbeat, got {} in {:?}",
        heartbeats,
        body_str
    );

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

/// 慢路径测试用配置：1s 心跳，迟到错误改写为 `overloaded_error`。
fn late_overloaded_cfg(upstream: SocketAddr) -> ResolvedConfig {
    ResolvedConfig {
        anthropic_late_error_type: Some("overloaded_error".to_string()),
        ..test_cfg(upstream, 1, 32 * 1024 * 1024)
    }
}

/// 对 Anthropic Messages 发流式请求，返回 (status, 去分块后的 body)。
async fn post_anthropic_stream(gw: SocketAddr) -> (u16, String) {
    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(),
        body
    );
    let resp = send_raw(gw, &req).await;
    let (status, _, resp_body) = parse_response(&resp);
    (
        status,
        String::from_utf8_lossy(&dechunk(&resp_body)).into_owned(),
    )
}

#[tokio::test]
async fn test_late_502_anthropic_overloaded() {
    // 慢上游 502（sub2api 格式错误体）→ 网关先发 200，错误事件 type 改写为 overloaded_error
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        let body = br#"{"type":"error","error":{"type":"upstream_error","message":"Upstream request failed"}}"#;
        (
            502,
            vec![("content-type", "application/json".to_string())],
            1500, // 响应头延迟 1.5s，超过 1s 窗口
            vec![(body.to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway_with(late_overloaded_cfg(upstream)).await;

    let (status, s) = post_anthropic_stream(gw).await;
    assert_eq!(status, 200);
    assert!(s.contains("\"type\":\"overloaded_error\""), "{s}");
    assert!(s.contains("Upstream request failed"), "{s}");
    assert!(!s.contains("upstream_error"), "{s}");

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_late_connect_failure_anthropic_overloaded() {
    // 上游收下请求后 1.5s 不回任何字节即关闭 → bridge 的连接失败分支，type 改写为 overloaded_error
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = listener.local_addr().unwrap();
    tokio::task::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let _ = stream.read(&mut buf).await;
        sleep(Duration::from_millis(1500)).await;
    });
    let (gw, gw_tx) = start_gateway_with(late_overloaded_cfg(upstream)).await;

    let (status, s) = post_anthropic_stream(gw).await;
    assert_eq!(status, 200);
    assert!(s.contains("\"type\":\"overloaded_error\""), "{s}");
    assert!(s.contains("upstream request failed"), "{s}");

    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_late_429_anthropic_not_rewritten() {
    // 慢上游 429 + 开关 → 4xx 不改写，错误事件保留上游的 rate_limit_error
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        let body = br#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
        (
            429,
            vec![("content-type", "application/json".to_string())],
            1500, // 响应头延迟 1.5s，超过 1s 窗口
            vec![(body.to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway_with(late_overloaded_cfg(upstream)).await;

    let (status, s) = post_anthropic_stream(gw).await;
    assert_eq!(status, 200);
    assert!(s.contains("\"type\":\"rate_limit_error\""), "{s}");
    assert!(!s.contains("overloaded_error"), "{s}");

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_late_stream_error_not_rewritten() {
    // 慢上游回 2xx SSE，发出一个事件后中途断开 → 内容可能已转发，不改写，仍为 api_error
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = listener.local_addr().unwrap();
    tokio::task::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let _ = stream.read(&mut buf).await;
        sleep(Duration::from_millis(1500)).await;
        let event = b"event: ping\ndata: {\"type\":\"ping\"}\n\n";
        let mut resp = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n".to_vec();
        resp.extend_from_slice(format!("{:x}\r\n", event.len()).as_bytes());
        resp.extend_from_slice(event);
        resp.extend_from_slice(b"\r\n");
        stream.write_all(&resp).await.unwrap();
        stream.flush().await.unwrap();
        // 不发结尾的 0 块，直接关闭连接。
    });
    let (gw, gw_tx) = start_gateway_with(late_overloaded_cfg(upstream)).await;

    let (status, s) = post_anthropic_stream(gw).await;
    assert_eq!(status, 200);
    assert!(s.contains("\"type\":\"ping\""), "{s}");
    assert!(s.contains("\"type\":\"api_error\""), "{s}");
    assert!(!s.contains("overloaded_error"), "{s}");

    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_late_undecodable_encoding_not_rewritten() {
    // 慢上游回 2xx SSE 但 Content-Encoding 不可解码 → 不改写，仍为 api_error
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![
                ("content-type", "text/event-stream".to_string()),
                ("content-encoding", "compress".to_string()),
            ],
            1500, // 响应头延迟 1.5s，超过 1s 窗口
            vec![(b"data: hello\n\n".to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway_with(late_overloaded_cfg(upstream)).await;

    let (status, s) = post_anthropic_stream(gw).await;
    assert_eq!(status, 200);
    assert!(s.contains("\"type\":\"api_error\""), "{s}");
    assert!(!s.contains("overloaded_error"), "{s}");

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

/// 读到响应头与第一个心跳为止；`deadline` 内读不到则测试失败。
async fn read_head_and_first_heartbeat(
    stream: &mut tokio::net::TcpStream,
    deadline: Duration,
) -> String {
    let got = tokio::time::timeout(deadline, async {
        let mut got = Vec::new();
        let mut buf = [0u8; 1024];
        while !got.windows(3).any(|w| w == b":\n\n") {
            let n = stream.read(&mut buf).await.unwrap();
            assert!(n > 0, "首个心跳之前连接就关闭了: {got:?}");
            got.extend_from_slice(&buf[..n]);
        }
        got
    })
    .await
    .expect("deadline 内应收到响应头与首个心跳");
    String::from_utf8_lossy(&got).into_owned()
}

/// 发一个 Anthropic 流式请求（Connection: close），返回连接。
async fn open_anthropic_stream(gw: SocketAddr) -> tokio::net::TcpStream {
    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(),
        body
    );
    let mut stream = tokio::net::TcpStream::connect(gw).await.unwrap();
    stream.write_all(req.as_bytes()).await.unwrap();
    stream
}

#[tokio::test]
async fn test_delayed_200_sent_at_window() {
    // 窗口超时网关发 200 时，响应头与首个心跳应在窗口到期时发出，而不是再等一个间隔
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            5000, // 响应头延迟 5s，超过两个 2s 间隔
            vec![(b"data: hello\n\n".to_vec(), 0)],
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 2).await;

    let mut stream = open_anthropic_stream(gw).await;
    // 2s 窗口到期即应收到；不立即写心跳时第一个字节要到 4s。
    let head = read_head_and_first_heartbeat(&mut stream, Duration::from_secs(3)).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");

    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).await.unwrap();
    assert!(String::from_utf8_lossy(&rest).contains("hello"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_passthrough_headers_sent_by_window_end() {
    // 窗口内上游回了 SSE 响应头、数据迟迟不来：透传的响应头应在窗口截止时随首个心跳发出
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![
                ("content-type", "text/event-stream".to_string()),
                ("x-request-id", "req-early".to_string()),
            ],
            1500,                                      // 响应头在 2s 窗口内的 1.5s 到达
            vec![(b"data: hello\n\n".to_vec(), 3500)], // 数据再过 3.5s 才到
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 2).await;

    let mut stream = open_anthropic_stream(gw).await;
    // 窗口截止（2s）即应收到；按响应头到达后再等一个间隔算，第一个字节要到 3.5s。
    let head = read_head_and_first_heartbeat(&mut stream, Duration::from_millis(2800)).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert!(head.contains("req-early"), "{head}");

    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).await.unwrap();
    assert!(String::from_utf8_lossy(&rest).contains("hello"));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_passthrough_byte_identical_before_window_end() {
    // 上游数据在窗口截止前到达：透传的响应体与上游逐字节相同，开头不插心跳
    let (upstream, tx) = start_mock_raw(|_b, _p| {
        (
            200,
            vec![("content-type", "text/event-stream".to_string())],
            0,
            vec![(b"data: hello\n\n".to_vec(), 300)], // 数据 0.3s 后到，早于 2s 窗口截止
        )
    })
    .await;
    let (gw, gw_tx) = start_gateway(upstream, 2).await;

    let mut stream = open_anthropic_stream(gw).await;
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.unwrap();
    let (status, _, body) = parse_response(&resp);
    assert_eq!(status, 200);
    assert_eq!(String::from_utf8_lossy(&dechunk(&body)), "data: hello\n\n");

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

// === 先攒后发 + 重试 ===

/// 一次 mock 回复：`cut` 为真时发完 `chunks` 直接关连接，不发结尾的 0 块（模拟上游流到一半断）。
struct Reply {
    status: u16,
    headers: Vec<(&'static str, String)>,
    header_delay_ms: u64,
    chunks: Vec<(Vec<u8>, u64)>,
    cut: bool,
}

/// `start_mock_seq` 的记录：每次请求的请求体，以及网关是否在回复发完前关掉了连接。
#[derive(Default)]
struct SeqLog {
    bodies: Vec<Vec<u8>>,
    closed_early: Vec<bool>,
}

type SharedLog = std::sync::Arc<parking_lot::Mutex<SeqLog>>;

/// 按请求序号（从 0 起）决定回复的 mock 上游（原始 TCP）。
async fn start_mock_seq<F>(reply: F) -> (SocketAddr, SharedLog, oneshot::Sender<()>)
where
    F: Fn(usize) -> Reply + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, mut rx) = oneshot::channel::<()>();
    let reply = std::sync::Arc::new(reply);
    let log = SharedLog::default();
    let log_srv = log.clone();
    tokio::task::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut rx => break,
                accept = listener.accept() => {
                    let (mut stream, _) = match accept { Ok(v) => v, Err(_) => continue };
                    let reply = reply.clone();
                    let log = log_srv.clone();
                    tokio::task::spawn(async move {
                        let mut buf = vec![0u8; 8192];
                        let mut req_data = Vec::new();
                        loop {
                            let n = stream.read(&mut buf).await.unwrap();
                            if n == 0 { return; }
                            req_data.extend_from_slice(&buf[..n]);
                            if req_data.windows(4).any(|w| w == b"\r\n\r\n") { break; }
                        }
                        let header_end = req_data.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                        let req_str = String::from_utf8_lossy(&req_data[..header_end]).into_owned();
                        let cl = req_str.lines()
                            .find(|l| l.to_lowercase().starts_with("content-length:"))
                            .and_then(|l| l.split(':').nth(1))
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        let mut body = req_data[header_end..].to_vec();
                        while body.len() < cl {
                            let n = stream.read(&mut buf).await.unwrap();
                            if n == 0 { break; }
                            body.extend_from_slice(&buf[..n]);
                        }
                        let idx = {
                            let mut l = log.lock();
                            l.bodies.push(body);
                            l.closed_early.push(false);
                            l.bodies.len() - 1
                        };
                        let r = reply(idx);
                        let (mut rd, mut wr) = stream.into_split();
                        let write = async move {
                            if r.header_delay_ms > 0 {
                                sleep(Duration::from_millis(r.header_delay_ms)).await;
                            }
                            let mut head = format!("HTTP/1.1 {}\r\n", r.status);
                            for (k, v) in &r.headers {
                                head.push_str(&format!("{}: {}\r\n", k, v));
                            }
                            head.push_str("transfer-encoding: chunked\r\n\r\n");
                            wr.write_all(head.as_bytes()).await?;
                            for (chunk, delay_ms) in &r.chunks {
                                if *delay_ms > 0 {
                                    sleep(Duration::from_millis(*delay_ms)).await;
                                }
                                wr.write_all(format!("{:x}\r\n", chunk.len()).as_bytes()).await?;
                                wr.write_all(chunk).await?;
                                wr.write_all(b"\r\n").await?;
                                wr.flush().await?;
                            }
                            if !r.cut {
                                wr.write_all(b"0\r\n\r\n").await?;
                                wr.flush().await?;
                            }
                            Ok::<_, std::io::Error>(())
                        };
                        // 网关关掉连接时读到 EOF。
                        let watch = async move {
                            let mut b = [0u8; 64];
                            loop {
                                match rd.read(&mut b).await {
                                    Ok(0) | Err(_) => break,
                                    Ok(_) => {}
                                }
                            }
                        };
                        tokio::select! {
                            _ = write => {}
                            _ = watch => log.lock().closed_early[idx] = true,
                        }
                    });
                }
            }
        }
    });
    (addr, log, tx)
}

/// 一段完整的 Anthropic 流式回复（逐事件），文本为 `text`。
fn anthropic_reply(text: &str) -> Vec<Vec<u8>> {
    vec![
        b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n".to_vec(),
        b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n".to_vec(),
        format!("event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"{text}\"}}}}\n\n").into_bytes(),
        b"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n".to_vec(),
        b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n".to_vec(),
        b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n".to_vec(),
    ]
}

/// Anthropic 流式 error 事件。
fn anthropic_error_event(etype: &str) -> Vec<u8> {
    format!("event: error\ndata: {{\"type\":\"error\",\"error\":{{\"type\":\"{etype}\",\"message\":\"boom\"}}}}\n\n").into_bytes()
}

/// 2xx SSE 回复，事件之间隔 `delay_ms`。
fn sse_reply(events: Vec<Vec<u8>>, delay_ms: u64, cut: bool) -> Reply {
    Reply {
        status: 200,
        headers: vec![("content-type", "text/event-stream".to_string())],
        header_delay_ms: 0,
        chunks: events.into_iter().map(|e| (e, delay_ms)).collect(),
        cut,
    }
}

/// 非 2xx JSON 回复。
fn status_reply(status: u16, body: &str) -> Reply {
    Reply {
        status,
        headers: vec![("content-type", "application/json".to_string())],
        header_delay_ms: 0,
        chunks: vec![(body.as_bytes().to_vec(), 0)],
        cut: false,
    }
}

const HOLD_REQ_BODY: &str =
    r#"{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
const SUB2API_502_BODY: &str =
    r#"{"type":"error","error":{"type":"upstream_error","message":"Upstream request failed"}}"#;

/// 先攒后发测试用配置：W = 60 s，迟到错误改写为 overloaded_error。
fn hold_cfg(upstream: SocketAddr, heartbeat: u64) -> ResolvedConfig {
    ResolvedConfig {
        anthropic_late_error_type: Some("overloaded_error".to_string()),
        hold_window: Duration::from_secs(60),
        ..test_cfg(upstream, heartbeat, 32 * 1024 * 1024)
    }
}

/// 发 `HOLD_REQ_BODY`，返回 (status, headers, 去分块后的 body)。
async fn post_hold(gw: SocketAddr) -> (u16, Vec<(String, String)>, String) {
    let req = format!(
        "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        HOLD_REQ_BODY.len(),
        HOLD_REQ_BODY
    );
    let resp = send_raw(gw, &req).await;
    let (status, headers, body) = parse_response(&resp);
    let is_chunked = headers
        .iter()
        .any(|(k, v)| k == "transfer-encoding" && v.contains("chunked"));
    let body = if is_chunked { dechunk(&body) } else { body };
    (status, headers, String::from_utf8_lossy(&body).into_owned())
}

fn concat(events: &[Vec<u8>]) -> String {
    String::from_utf8(events.concat()).unwrap()
}

#[tokio::test]
async fn test_hold_retry_after_mid_stream_cut() {
    // 第一次发出一半事件后断开，第二次完整 → 客户端只收到第二次的完整事件流
    let (upstream, log, tx) = start_mock_seq(|i| {
        if i == 0 {
            let mut ev = anthropic_reply("first");
            ev.truncate(3);
            sse_reply(ev, 50, true)
        } else {
            sse_reply(anthropic_reply("second"), 50, false)
        }
    })
    .await;
    let (gw, gw_tx) = start_gateway_with(hold_cfg(upstream, 60)).await;

    let (status, _, body) = post_hold(gw).await;
    assert_eq!(status, 200);
    assert_eq!(body, concat(&anthropic_reply("second")));
    let log = log.lock();
    assert_eq!(log.bodies.len(), 2);
    assert!(log.bodies.iter().all(|b| b == HOLD_REQ_BODY.as_bytes()));

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_hold_retry_after_502() {
    // 第一次 502，第二次成功 → 客户端收到成功的流
    let (upstream, log, tx) = start_mock_seq(|i| {
        if i == 0 {
            status_reply(502, SUB2API_502_BODY)
        } else {
            sse_reply(anthropic_reply("ok"), 0, false)
        }
    })
    .await;
    let (gw, gw_tx) = start_gateway_with(hold_cfg(upstream, 60)).await;

    let (status, _, body) = post_hold(gw).await;
    assert_eq!(status, 200);
    assert_eq!(body, concat(&anthropic_reply("ok")));
    assert_eq!(log.lock().bodies.len(), 2);

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_hold_400_passthrough_no_retry() {
    // 400 → 立即透传状态码与错误体，上游只收到 1 次
    let err = r#"{"type":"error","error":{"type":"invalid_request_error","message":"bad"}}"#;
    let (upstream, log, tx) = start_mock_seq(move |_| status_reply(400, err)).await;
    let (gw, gw_tx) = start_gateway_with(hold_cfg(upstream, 60)).await;

    let (status, headers, body) = post_hold(gw).await;
    assert_eq!(status, 400);
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-type")
            .map(|(_, v)| v.as_str()),
        Some("application/json")
    );
    assert_eq!(body, err);
    assert_eq!(log.lock().bodies.len(), 1);

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_hold_retry_after_api_error_event() {
    // 第一次中途收到 api_error 事件后正常结束，第二次完整 → 客户端收到完整流
    let (upstream, log, tx) = start_mock_seq(|i| {
        if i == 0 {
            let mut ev = anthropic_reply("first");
            ev.truncate(2);
            ev.push(anthropic_error_event("api_error"));
            sse_reply(ev, 0, false)
        } else {
            sse_reply(anthropic_reply("second"), 0, false)
        }
    })
    .await;
    let (gw, gw_tx) = start_gateway_with(hold_cfg(upstream, 60)).await;

    let (status, _, body) = post_hold(gw).await;
    assert_eq!(status, 200);
    assert_eq!(body, concat(&anthropic_reply("second")));
    assert_eq!(log.lock().bodies.len(), 2);

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_hold_final_error_event_not_retried() {
    // 中途收到 invalid_request_error / rate_limit_error 事件 → 不重试，客户端收到这个 error 事件
    for etype in ["invalid_request_error", "rate_limit_error"] {
        let mut events = anthropic_reply("x");
        events.truncate(1);
        events.push(anthropic_error_event(etype));
        let sent = events.clone();
        let (upstream, log, tx) =
            start_mock_seq(move |_| sse_reply(events.clone(), 0, false)).await;
        let (gw, gw_tx) = start_gateway_with(hold_cfg(upstream, 60)).await;

        let (status, _, body) = post_hold(gw).await;
        assert_eq!(status, 200, "{etype}");
        assert_eq!(body, concat(&sent), "{etype}");
        assert_eq!(log.lock().bodies.len(), 1, "{etype}");

        tx.send(()).unwrap();
        gw_tx.send(()).unwrap();
    }
}

/// 每次都在 0.2 s 后回响应头、3 个事件后断开的上游。
fn always_cut(_: usize) -> Reply {
    let mut ev = anthropic_reply("partial");
    ev.truncate(3);
    let mut r = sse_reply(ev, 0, true);
    r.header_delay_ms = 200;
    r
}

#[tokio::test]
async fn test_hold_stream_retries_exhausted_in_window() {
    // 流断次数超过上限、全在窗口内 → 502 + api_error JSON，客户端收不到半截内容
    let (upstream, log, tx) = start_mock_seq(always_cut).await;
    let cfg = ResolvedConfig {
        max_stream_retries: 2,
        ..hold_cfg(upstream, 60)
    };
    let (gw, gw_tx) = start_gateway_with(cfg).await;

    let (status, _, body) = post_hold(gw).await;
    assert_eq!(status, 502);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["type"], "error");
    assert_eq!(v["error"]["type"], "api_error");
    assert!(!body.contains("partial"), "{body}");
    assert_eq!(log.lock().bodies.len(), 3);

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_hold_stream_retries_exhausted_late() {
    // 流断次数超过上限、拖过 1 s 窗口 → 200、心跳和改写后的 overloaded_error 事件，没有半截内容
    let (upstream, log, tx) = start_mock_seq(always_cut).await;
    let cfg = ResolvedConfig {
        max_stream_retries: 1,
        ..hold_cfg(upstream, 1)
    };
    let (gw, gw_tx) = start_gateway_with(cfg).await;

    let (status, _, body) = post_hold(gw).await;
    assert_eq!(status, 200);
    assert!(body.starts_with(":\n\n"), "{body}");
    assert!(body.contains("\"type\":\"overloaded_error\""), "{body}");
    assert!(!body.contains("partial"), "{body}");
    assert!(!body.contains("message_start"), "{body}");
    assert_eq!(log.lock().bodies.len(), 2);

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_hold_status_retry_deadline() {
    // 一直 502、重试时限 2 s：0 s、1 s 各一次，下一次要到 3 s，超出时限 → 不再发起
    for heartbeat in [60, 1] {
        let (upstream, log, tx) = start_mock_seq(|_| status_reply(502, SUB2API_502_BODY)).await;
        let cfg = ResolvedConfig {
            retry_deadline: Duration::from_secs(2),
            ..hold_cfg(upstream, heartbeat)
        };
        let (gw, gw_tx) = start_gateway_with(cfg).await;

        let (status, _, body) = post_hold(gw).await;
        if heartbeat == 60 {
            // 窗口内：透传状态码与错误体
            assert_eq!(status, 502);
            assert_eq!(body, SUB2API_502_BODY);
        } else {
            // 窗口后：200 + 迟到改写的 error 事件
            assert_eq!(status, 200);
            assert!(body.contains("\"type\":\"overloaded_error\""), "{body}");
            assert!(body.contains("Upstream request failed"), "{body}");
        }
        assert_eq!(log.lock().bodies.len(), 2, "heartbeat {heartbeat}");

        tx.send(()).unwrap();
        gw_tx.send(()).unwrap();
    }
}

#[tokio::test]
async fn test_hold_stream_retry_after_deadline() {
    // 首字节晚：请求到达 1.5 s 后才第一次流断，已过 1 s 重试时限，仍然重试
    let (upstream, log, tx) = start_mock_seq(|i| {
        if i == 0 {
            let mut ev = anthropic_reply("first");
            ev.truncate(2);
            let mut r = sse_reply(ev, 0, true);
            r.header_delay_ms = 1500;
            r
        } else {
            sse_reply(anthropic_reply("second"), 0, false)
        }
    })
    .await;
    let cfg = ResolvedConfig {
        retry_deadline: Duration::from_secs(1),
        ..hold_cfg(upstream, 60)
    };
    let (gw, gw_tx) = start_gateway_with(cfg).await;

    let (status, _, body) = post_hold(gw).await;
    assert_eq!(status, 200);
    assert_eq!(body, concat(&anthropic_reply("second")));
    assert_eq!(log.lock().bodies.len(), 2);

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_hold_body_over_limit_not_held() {
    // 请求体超过 hold_max_body → 不攒、不重试：半截内容照常转发，随后是 api_error 事件
    let (upstream, log, tx) = start_mock_seq(always_cut).await;
    let cfg = ResolvedConfig {
        hold_max_body: 8,
        ..hold_cfg(upstream, 60)
    };
    let (gw, gw_tx) = start_gateway_with(cfg).await;

    let (status, _, body) = post_hold(gw).await;
    assert_eq!(status, 200);
    assert!(body.starts_with("event: message_start"), "{body}");
    assert!(body.contains("partial"), "{body}");
    assert!(body.contains("\"type\":\"api_error\""), "{body}");
    assert_eq!(log.lock().bodies.len(), 1);

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_hold_window_commits_then_live() {
    // W = 1 s：攒满 1 s 先发出已攒的部分，之后直播；随后断开 → api_error 事件，不重试
    let (upstream, log, tx) = start_mock_seq(|_| {
        let mut ev = anthropic_reply("long");
        ev.truncate(5);
        sse_reply(ev, 400, true)
    })
    .await;
    let cfg = ResolvedConfig {
        hold_window: Duration::from_secs(1),
        ..hold_cfg(upstream, 60)
    };
    let (gw, gw_tx) = start_gateway_with(cfg).await;

    let started = std::time::Instant::now();
    let mut stream = open_hold_stream(gw).await;
    let mut got = Vec::new();
    let mut buf = [0u8; 4096];
    while !got.windows(13).any(|w| w == b"message_start") {
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0, "连接在第一个事件之前关闭: {got:?}");
        got.extend_from_slice(&buf[..n]);
    }
    let first_event = started.elapsed();
    // 第一个事件在 0.4 s 到上游，攒满 W（第一个字节后 1 s）才发出；整段要到 2 s 才发完。
    assert!(
        first_event >= Duration::from_millis(1300) && first_event < Duration::from_millis(1900),
        "{first_event:?}"
    );
    stream.read_to_end(&mut got).await.unwrap();
    let (status, _, body) = parse_response(&got);
    assert_eq!(status, 200);
    let body = String::from_utf8(dechunk(&body)).unwrap();
    let mut events = anthropic_reply("long");
    events.truncate(5);
    assert!(body.starts_with(&concat(&events)), "{body}");
    assert!(body.contains("\"type\":\"api_error\""), "{body}");
    assert_eq!(log.lock().bodies.len(), 1);

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

/// 发 `HOLD_REQ_BODY`（Connection: close），返回连接。
async fn open_hold_stream(gw: SocketAddr) -> tokio::net::TcpStream {
    let req = format!(
        "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        HOLD_REQ_BODY.len(),
        HOLD_REQ_BODY
    );
    let mut stream = tokio::net::TcpStream::connect(gw).await.unwrap();
    stream.write_all(req.as_bytes()).await.unwrap();
    stream
}

#[tokio::test]
async fn test_hold_client_disconnect_cancels() {
    // 攒着时客户端断开 → 上游连接被关掉，之后没有新的请求（日志见 tests/hold_log.rs）
    let (upstream, log, tx) = start_mock_seq(|_| {
        let ev = anthropic_reply("slow");
        let mut r = sse_reply(vec![ev[0].clone(), ev[1].clone()], 0, false);
        r.chunks[1].1 = 5000; // 第二个事件 5 s 后才到
        r
    })
    .await;
    let (gw, gw_tx) = start_gateway_with(hold_cfg(upstream, 60)).await;

    let stream = open_hold_stream(gw).await;
    sleep(Duration::from_millis(500)).await;
    drop(stream);

    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        while !log.lock().closed_early.first().copied().unwrap_or(false) {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(closed.is_ok(), "客户端断开后上游连接应被关闭");
    sleep(Duration::from_millis(1500)).await;
    assert_eq!(log.lock().bodies.len(), 1);

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_hold_beyond_heartbeat_window() {
    // 攒着超过 1 s 心跳窗口 → 客户端先收到 200 和心跳，收全后收到完整事件流
    let (upstream, log, tx) =
        start_mock_seq(|_| sse_reply(anthropic_reply("late"), 500, false)).await;
    let (gw, gw_tx) = start_gateway_with(hold_cfg(upstream, 1)).await;

    let mut stream = open_hold_stream(gw).await;
    let head = read_head_and_first_heartbeat(&mut stream, Duration::from_millis(1500)).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert!(!head.contains("message_start"), "{head}");

    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).await.unwrap();
    let mut raw = head.into_bytes();
    raw.extend_from_slice(&rest);
    let (status, _, body) = parse_response(&raw);
    assert_eq!(status, 200);
    let body = String::from_utf8(dechunk(&body)).unwrap();
    let events = body.trim_start_matches(":\n\n");
    assert_eq!(events, concat(&anthropic_reply("late")));
    assert!(body.len() - events.len() >= 6, "至少两个心跳: {body:?}");
    assert_eq!(log.lock().bodies.len(), 1);

    tx.send(()).unwrap();
    gw_tx.send(()).unwrap();
}
