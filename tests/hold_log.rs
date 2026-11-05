//! 先攒后发的结束日志，单独一个测试二进制。
//!
//! 只注册了一个 dispatcher 时，tracing 按第一次命中 callsite 的线程上的默认 subscriber 缓存它的
//! interest；与 `tests/gateway.rs` 里的测试并行时，别的测试线程会先把这条日志缓存成 never。
//! 单独一个进程、只有这一个测试，就没有这个问题。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use keep_sse::config::ResolvedConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::sleep;

/// 把 `tracing` 输出写进内存。
#[derive(Clone, Default)]
struct LogBuf(Arc<parking_lot::Mutex<Vec<u8>>>);

impl std::io::Write for LogBuf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// 回 200 SSE 响应头和一个事件后不再发送的上游。
async fn start_stalling_upstream() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::task::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let _ = stream.read(&mut buf).await;
        let event = b"event: message_start\ndata: {\"type\":\"message_start\"}\n\n";
        let mut resp = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n".to_vec();
        resp.extend_from_slice(format!("{:x}\r\n", event.len()).as_bytes());
        resp.extend_from_slice(event);
        resp.extend_from_slice(b"\r\n");
        stream.write_all(&resp).await.unwrap();
        sleep(Duration::from_secs(10)).await;
    });
    addr
}

#[tokio::test]
async fn test_hold_cancel_logged() {
    // 攒着时客户端断开 → 结束日志 outcome=cancelled
    // `#[tokio::test]` 是单线程运行时，网关任务也在本线程上，线程内的默认 subscriber 能收到它们的日志。
    let logs = LogBuf::default();
    let writer = logs.clone();
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish(),
    );

    let upstream = start_stalling_upstream().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gw = listener.local_addr().unwrap();
    let cfg = ResolvedConfig {
        listen: gw,
        upstream: format!("http://{upstream}").parse().unwrap(),
        upstream_authority: upstream.to_string(),
        heartbeat_interval: Duration::from_secs(60),
        connect_timeout: Duration::from_secs(5),
        max_probe_body: 1024 * 1024,
        shutdown_timeout: Duration::from_secs(1),
        anthropic_late_error_type: None,
        hold_window: Duration::from_secs(60),
        retry_deadline: Duration::from_secs(180),
        max_stream_retries: 3,
        hold_max_bytes: 8 * 1024 * 1024,
        hold_max_body: 16 * 1024 * 1024,
    };
    tokio::task::spawn(keep_sse::server::run_with_shutdown(
        cfg,
        listener,
        std::future::pending(),
    ));

    let body = r#"{"stream":true}"#;
    let req = format!(
        "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
        body.len(),
        body
    );
    let mut stream = tokio::net::TcpStream::connect(gw).await.unwrap();
    stream.write_all(req.as_bytes()).await.unwrap();
    sleep(Duration::from_millis(500)).await;
    drop(stream);

    let logged = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let out = String::from_utf8_lossy(&logs.0.lock()).into_owned();
            if out.contains("hold finished") {
                return out;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("客户端断开后应写出结束日志");
    assert!(logged.contains("outcome=\"cancelled\""), "{logged}");
    assert!(logged.contains("attempts=1"), "{logged}");
}
