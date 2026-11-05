//! 先攒后发 + 重试：只用于 Anthropic Messages 流式请求，`--hold-window` 大于 0 时打开。
//!
//! 发给客户端的半截回复续不上，所以只重试客户端还没看到任何事件的回复：上游 2xx SSE 的事件
//! 先全部缓存，收到 `message_stop` 且上游正常结束才一次写给客户端；在那之前上游断了，就用同一个
//! 请求从头再发。等待期间与现有通路一样赛跑心跳窗口：窗口内有结论就透传上游的状态码，
//! 窗口到期则先回 200 和心跳。

use std::time::Duration;

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::header::{HeaderMap, CONTENT_TYPE};
use hyper::http::{request, response};
use hyper::{Method, Request, Response, StatusCode, Uri, Version};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_stream::StreamExt as _;

use crate::config::ResolvedConfig;
use crate::detect::EndpointKind;
use crate::encoding::{self, DecodedStream, SseWriter};
use crate::error_event;
use crate::proxy::{self, ReqBody, RespBody};
use crate::sse::{self, EventBoundaryTracker, Heartbeat, HEARTBEAT};

/// 可重试的上游状态码。
const RETRY_STATUS: &[u16] = &[500, 502, 503, 504, 529];

/// 不重试的 error 事件类型：重发同一个请求只会得到同样的错误。
const FINAL_ERROR_TYPES: &[&str] = &[
    "invalid_request_error",
    "authentication_error",
    "permission_error",
    "not_found_error",
    "request_too_large",
    "billing_error",
    "rate_limit_error",
];

/// [`EventScanner`] 单行、单个事件 data 的保留上限（字节）。
const MAX_FIELD: usize = 64 * 1024;

type UpstreamError = hyper_util::client::legacy::Error;

/// 已重试 `n` 次之后的退避：1、2、4、8 s，之后每次 8 s。
fn backoff(n: u32) -> Duration {
    Duration::from_secs(1 << n.min(3))
}

/// 先攒后发通路入口：后台任务驱动尝试与重试，这里等它交回响应。
///
/// 客户端在交回响应前断开时 hyper 丢弃本 future，后台任务随即发现并放弃上游请求。
pub(crate) async fn handle(
    cfg: ResolvedConfig,
    client: Client<HttpConnector, ReqBody>,
    parts: request::Parts,
    body: Bytes,
    kind: EndpointKind,
    arrived: Instant,
) -> Response<RespBody> {
    let started = Instant::now();
    let template = Template::new(&cfg, parts, body);
    let (tx, rx) = oneshot::channel();
    let down = Downstream {
        state: State::Pending(tx),
        timer: Box::pin(tokio::time::sleep_until(started + cfg.heartbeat_interval)),
        interval: cfg.heartbeat_interval,
    };
    tokio::task::spawn(drive(cfg, client, template, kind, arrived, started, down));
    rx.await
        .unwrap_or_else(|_| proxy::bad_gateway("hold task ended without a response"))
}

/// 每次尝试都从这里重建上游请求（`Parts` 不可 clone）。
struct Template {
    method: Method,
    uri: Uri,
    version: Version,
    headers: HeaderMap,
    body: Bytes,
}

impl Template {
    fn new(cfg: &ResolvedConfig, mut parts: request::Parts, body: Bytes) -> Self {
        sse::prepare_upstream_parts(cfg, &mut parts);
        Self {
            method: parts.method,
            uri: parts.uri,
            version: parts.version,
            headers: parts.headers,
            body,
        }
    }

    fn request(&self) -> Request<ReqBody> {
        let mut req = Request::new(sse::full_body(self.body.clone()));
        *req.method_mut() = self.method.clone();
        *req.uri_mut() = self.uri.clone();
        *req.version_mut() = self.version;
        *req.headers_mut() = self.headers.clone();
        req
    }
}

/// 一次尝试攒下的回复。
struct Held {
    head: response::Parts,
    chunks: Vec<Bytes>,
    /// 本次第一个字节到达的时刻。
    first_byte: Instant,
}

impl Held {
    fn new(head: response::Parts, chunks: Vec<Bytes>, first_byte: Option<Instant>) -> Self {
        Self {
            head,
            chunks,
            first_byte: first_byte.unwrap_or_else(Instant::now),
        }
    }
}

/// 不再攒、转为直播的原因。
#[derive(Clone, Copy)]
enum Commit {
    /// 从第一个字节起攒满了 `hold_window`。
    Window,
    /// 缓存超过 `hold_max_bytes`。
    MaxBytes,
    /// 不可重试的 error 事件：随已攒的事件交给客户端。
    FinalError,
}

/// 一次尝试的结论。
enum Attempt {
    /// 收到 `message_stop` 且上游正常结束。
    Complete(Held),
    /// 不再攒：写出已攒的部分后桥接剩下的流。
    Commit(Held, DecodedStream, Commit),
    /// 照现在的方式交出：不可重试的状态码，或 2xx 但不是可解码的 SSE。
    Deliver(Response<Incoming>),
    /// 可重试的状态码。
    Status(Response<Incoming>),
    /// 连接上游失败。
    Connect(UpstreamError),
    /// 流到一半断：读出错、没有 `message_stop` 就结束、可重试的 error 事件。
    Stream(String),
}

/// 发出一次请求，读到能下结论为止。`window` 从第一个字节起算。
async fn attempt(
    client: &Client<HttpConnector, ReqBody>,
    req: Request<ReqBody>,
    window: Duration,
    max_bytes: usize,
) -> Attempt {
    let resp = match client.request(req).await {
        Ok(r) => r,
        Err(e) => return Attempt::Connect(e),
    };
    let status = resp.status();
    if !status.is_success() {
        return if RETRY_STATUS.contains(&status.as_u16()) {
            Attempt::Status(resp)
        } else {
            Attempt::Deliver(resp)
        };
    }
    let coding = encoding::parse_content_encoding(resp.headers());
    let Some(coding) = coding.filter(|_| sse::is_event_stream(resp.headers())) else {
        return Attempt::Deliver(resp);
    };

    let (head, body) = resp.into_parts();
    let mut stream = encoding::decoder_stream(coding, body);
    let mut scanner = EventScanner::default();
    let mut chunks = Vec::new();
    let mut held = 0;
    let mut first_byte = None;
    // 攒满 W 的计时器，收到第一个字节后才启用；W 大到加不上去时不启用。
    let until = tokio::time::sleep(window);
    tokio::pin!(until);
    let mut windowed = false;

    loop {
        let next = tokio::select! {
            next = stream.next() => Some(next),
            _ = &mut until, if windowed => None,
        };
        let Some(next) = next else {
            return Attempt::Commit(Held::new(head, chunks, first_byte), stream, Commit::Window);
        };
        match next {
            Some(Ok(bytes)) => {
                if first_byte.is_none() {
                    let now = Instant::now();
                    first_byte = Some(now);
                    if let Some(at) = now.checked_add(window) {
                        until.as_mut().reset(at);
                        windowed = true;
                    }
                }
                scanner.feed(&bytes);
                held += bytes.len();
                chunks.push(bytes);
                if let Some(etype) = &scanner.error {
                    if retryable_error(etype.as_deref()) {
                        let etype = etype.as_deref().unwrap_or("(unknown type)");
                        return Attempt::Stream(format!("error event {etype}"));
                    }
                    let held = Held::new(head, chunks, first_byte);
                    return Attempt::Commit(held, stream, Commit::FinalError);
                }
                if held > max_bytes {
                    let held = Held::new(head, chunks, first_byte);
                    return Attempt::Commit(held, stream, Commit::MaxBytes);
                }
            }
            Some(Err(e)) => return Attempt::Stream(format!("stream read error: {e}")),
            None if scanner.saw_stop => {
                return Attempt::Complete(Held::new(head, chunks, first_byte));
            }
            None => return Attempt::Stream("stream ended without message_stop".to_string()),
        }
    }
}

/// 驱动的最终结论。
enum Finish {
    Complete(Held),
    Commit(Held, DecodedStream, Commit),
    Deliver(Result<Response<Incoming>, UpstreamError>),
    StreamFailed(String),
    Cancelled,
}

/// 发起—攒—判定—重试循环，然后把结论交给客户端。
async fn drive(
    cfg: ResolvedConfig,
    client: Client<HttpConnector, ReqBody>,
    template: Template,
    kind: EndpointKind,
    arrived: Instant,
    started: Instant,
    mut down: Downstream,
) {
    let late_type = cfg.anthropic_late_error_type.as_deref();
    // 时长大到加不上去时视为没有时限。
    let deadline = arrived.checked_add(cfg.retry_deadline);
    let mut report = Report {
        arrived,
        started,
        attempts: 0,
        stream_retries: 0,
        failures: Vec::new(),
        hold: None,
    };

    let finish = loop {
        report.attempts += 1;
        let result = {
            let attempt = attempt(
                &client,
                template.request(),
                cfg.hold_window,
                cfg.hold_max_bytes,
            );
            tokio::pin!(attempt);
            tokio::select! {
                result = &mut attempt => Some(result),
                _ = down.keep_alive() => None,
            }
        };
        let Some(result) = result else {
            break Finish::Cancelled;
        };

        let backoff = backoff(report.attempts - 1);
        let past_deadline = deadline.is_some_and(|d| Instant::now() + backoff > d);
        let (reason, give_up) = match result {
            Attempt::Complete(held) => break Finish::Complete(held),
            Attempt::Commit(held, stream, why) => break Finish::Commit(held, stream, why),
            Attempt::Deliver(resp) => break Finish::Deliver(Ok(resp)),
            // 状态码与连接失败：新的一次只能在重试时限内发起。
            Attempt::Status(resp) => (
                format!("status {}", resp.status().as_u16()),
                past_deadline.then(|| Finish::Deliver(Ok(resp))),
            ),
            Attempt::Connect(e) => (
                format!("connect error: {e}"),
                past_deadline.then(|| Finish::Deliver(Err(e))),
            ),
            // 流断：不受时限限制，只受次数限制。
            Attempt::Stream(reason) => {
                let give_up = report.stream_retries >= cfg.max_stream_retries;
                if !give_up {
                    report.stream_retries += 1;
                }
                (
                    reason.clone(),
                    give_up.then(|| Finish::StreamFailed(reason)),
                )
            }
        };
        report.failures.push(reason);
        if let Some(finish) = give_up {
            break finish;
        }

        tracing::warn!(
            attempt = report.attempts,
            reason = report.failures.last().map(String::as_str),
            backoff_ms = backoff.as_millis() as u64,
            stream_retries = report.stream_retries,
            "hold: retrying upstream request"
        );
        let wake = Instant::now() + backoff;
        let gone = tokio::select! {
            _ = tokio::time::sleep_until(wake) => false,
            _ = down.keep_alive() => true,
        };
        if gone {
            break Finish::Cancelled;
        }
    };

    let interval = cfg.heartbeat_interval;
    let (outcome, detail) = match finish {
        Finish::Cancelled => ("cancelled", "client gone"),
        Finish::Complete(held) => {
            report.hold = Some(held.first_byte.elapsed());
            if let Some((mut writer, _)) = down.into_writer(held.head) {
                for chunk in held.chunks {
                    if writer.write_event(chunk).await.is_err() {
                        break;
                    }
                }
                writer.end().await;
            }
            ("complete", "message_stop")
        }
        Finish::Commit(held, stream, why) => {
            report.hold = Some(held.first_byte.elapsed());
            if let Some((mut writer, mut heartbeat)) = down.into_writer(held.head) {
                let mut tracker = EventBoundaryTracker::new();
                let mut gone = false;
                for chunk in held.chunks {
                    tracker.feed(&chunk);
                    if writer.write_event(chunk).await.is_err() {
                        gone = true;
                        break;
                    }
                }
                if !gone {
                    heartbeat
                        .as_mut()
                        .reset(tokio::time::Instant::now() + interval);
                    sse::bridge_stream(
                        &mut writer,
                        &mut heartbeat,
                        interval,
                        kind,
                        stream,
                        tracker,
                    )
                    .await;
                }
                writer.end().await;
            }
            match why {
                Commit::Window => ("committed", "hold window"),
                Commit::MaxBytes => ("committed", "hold max bytes"),
                Commit::FinalError => ("error", "final error event"),
            }
        }
        Finish::Deliver(resp) => {
            let result = match &resp {
                Ok(r) if r.status().is_success() => ("committed", "not an event stream"),
                Ok(r) if RETRY_STATUS.contains(&r.status().as_u16()) => {
                    ("error", "status retries exhausted")
                }
                Ok(_) => ("error", "final status"),
                Err(_) => ("error", "connect retries exhausted"),
            };
            down.deliver(kind, late_type, resp).await;
            result
        }
        Finish::StreamFailed(reason) => {
            down.stream_failed(kind, late_type, &reason).await;
            ("error", "stream retries exhausted")
        }
    };
    report.finish(outcome, detail);
}

/// 每个请求结束时的一行日志。
struct Report {
    arrived: Instant,
    started: Instant,
    attempts: u32,
    stream_retries: u32,
    /// 每次失败的原因：状态码、错误类型、传输错误的描述，不含请求体、响应体和头。
    failures: Vec<String>,
    /// 最后一次从第一个字节到写给客户端。
    hold: Option<Duration>,
}

impl Report {
    fn finish(&self, outcome: &str, detail: &str) {
        tracing::info!(
            outcome,
            detail,
            attempts = self.attempts,
            stream_retries = self.stream_retries,
            hold_ms = self.hold.map_or(0, |d| d.as_millis() as u64),
            total_ms = self.arrived.elapsed().as_millis() as u64,
            body_read_ms = (self.started - self.arrived).as_millis() as u64,
            failures = ?self.failures,
            "hold finished"
        );
    }
}

/// 下行状态。
enum State {
    /// 还没回响应头：窗口截止前还能透传上游的状态码。
    Pending(oneshot::Sender<Response<RespBody>>),
    /// 网关已先回 200 SSE。
    Open(SseWriter),
}

/// 客户端一侧：心跳窗口、心跳与断开检测。
struct Downstream {
    state: State,
    /// `Pending` 时是心跳窗口截止，`Open` 时是下一个心跳。
    timer: Heartbeat,
    interval: Duration,
}

impl Downstream {
    /// 推进窗口与心跳，只在客户端断开时完成。
    ///
    /// `Pending` 时 hyper 在客户端断开后丢弃等响应的 future（oneshot 接收端），`Open` 时丢弃响应体
    /// （下行通道接收端），两者都能立即察觉。攒着期间什么都没写，心跳都落在事件边界上。
    async fn keep_alive(&mut self) {
        loop {
            match &mut self.state {
                State::Pending(tx) => {
                    let due = tokio::select! {
                        _ = &mut self.timer => true,
                        _ = tx.closed() => false,
                    };
                    if !due || !self.open() {
                        return;
                    }
                }
                State::Open(writer) => {
                    let due = tokio::select! {
                        _ = &mut self.timer => true,
                        _ = writer.closed() => false,
                    };
                    if !due
                        || writer
                            .write_event(Bytes::from_static(HEARTBEAT))
                            .await
                            .is_err()
                    {
                        return;
                    }
                    self.timer.as_mut().reset(Instant::now() + self.interval);
                }
            }
        }
    }

    /// 窗口到期：先回网关自己的 200 SSE，首个心跳立即到期。客户端已断开时返回 false。
    fn open(&mut self) -> bool {
        let (body_tx, body) = sse::sse_channel();
        let State::Pending(tx) =
            std::mem::replace(&mut self.state, State::Open(SseWriter::new(body_tx)))
        else {
            unreachable!("open() is only called while pending");
        };
        // hyper 在响应体出现第一个字节前不会写出响应头。
        self.timer.as_mut().reset(Instant::now());
        tx.send(sse::gateway_sse_response(body)).is_ok()
    }

    /// 写回复的入口：还没回响应头时按 `head` 透传上游的状态码与响应头，已回 200 时沿用。
    /// 客户端已断开时返回 `None`。
    fn into_writer(self, mut head: response::Parts) -> Option<(SseWriter, Heartbeat)> {
        match self.state {
            State::Pending(tx) => {
                sse::passthrough_head(&mut head);
                let (body_tx, body) = sse::sse_channel();
                tx.send(Response::from_parts(head, body)).ok()?;
                Some((SseWriter::new(body_tx), self.timer))
            }
            State::Open(writer) => Some((writer, self.timer)),
        }
    }

    /// 照现在的方式交出上游响应或连接错误。
    async fn deliver(
        self,
        kind: EndpointKind,
        late_type: Option<&str>,
        resp: Result<Response<Incoming>, UpstreamError>,
    ) {
        match self.state {
            State::Pending(tx) => {
                let resp = match resp {
                    Ok(r) => proxy::passthrough_response(r),
                    Err(e) => {
                        tracing::warn!(error = %e, "upstream request failed (sse)");
                        proxy::bad_gateway(&e.to_string())
                    }
                };
                let _ = tx.send(resp);
            }
            State::Open(mut writer) => {
                let mut timer = self.timer;
                sse::deliver_late(
                    &mut writer,
                    &mut timer,
                    self.interval,
                    kind,
                    late_type,
                    resp,
                )
                .await;
                writer.end().await;
            }
        }
    }

    /// 流断用完重试：丢掉半截回复，换成客户端会按流式重试的错误。
    async fn stream_failed(self, kind: EndpointKind, late_type: Option<&str>, reason: &str) {
        match self.state {
            State::Pending(tx) => {
                let _ = tx.send(stream_failed_response(reason));
            }
            State::Open(mut writer) => {
                let reason = format!("stream interrupted: {reason}");
                let ev = error_event::build_late(kind, None, None, Some(&reason), late_type);
                let _ = writer.write_event(ev).await;
                writer.end().await;
            }
        }
    }
}

/// 窗口内流断用完重试时的 502：Anthropic 错误 JSON（`api_error`）。
fn stream_failed_response(reason: &str) -> Response<RespBody> {
    let body = serde_json::json!({
        "type": "error",
        "error": {
            "type": "api_error",
            "message": format!("upstream stream interrupted: {reason}"),
        }
    });
    let bytes = serde_json::to_vec(&body).expect("json serializable");
    Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .header(CONTENT_TYPE, "application/json")
        .body(sse::full_body(Bytes::from(bytes)))
        .expect("response buildable")
}

/// error 事件是否值得重试：类型不在 [`FINAL_ERROR_TYPES`] 里（含解析不出类型）。
fn retryable_error(etype: Option<&str>) -> bool {
    !etype.is_some_and(|t| FINAL_ERROR_TYPES.contains(&t))
}

/// 跟踪解码后的 Anthropic SSE 事件流：是否出现过 `message_stop`，第一个 `error` 事件的类型。
///
/// 按 `\r`、`\n`、`\r\n` 切行，记下当前事件的 `event:` 与 `data:`，在空行处判定。
#[derive(Default)]
struct EventScanner {
    line: Vec<u8>,
    line_cut: bool,
    prev_cr: bool,
    event: Vec<u8>,
    data: Vec<u8>,
    data_cut: bool,
    saw_stop: bool,
    /// 第一个 error 事件；内层为 data 里的 `error.type`，解析不出时为 `None`。
    error: Option<Option<String>>,
}

impl EventScanner {
    fn feed(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            if std::mem::take(&mut self.prev_cr) && bytes[0] == b'\n' {
                // `\r\n` 的第二个字节，这一行已在 `\r` 处结束。
                bytes = &bytes[1..];
                continue;
            }
            match memchr::memchr2(b'\n', b'\r', bytes) {
                None => {
                    self.push(bytes);
                    return;
                }
                Some(i) => {
                    self.push(&bytes[..i]);
                    self.prev_cr = bytes[i] == b'\r';
                    self.end_line();
                    bytes = &bytes[i + 1..];
                }
            }
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        let room = MAX_FIELD.saturating_sub(self.line.len());
        self.line_cut |= bytes.len() > room;
        self.line.extend_from_slice(&bytes[..bytes.len().min(room)]);
    }

    fn end_line(&mut self) {
        let mut line = std::mem::take(&mut self.line);
        let cut = std::mem::take(&mut self.line_cut);
        if line.is_empty() {
            self.dispatch();
        } else {
            self.field(&line, cut);
        }
        line.clear();
        self.line = line;
    }

    fn field(&mut self, line: &[u8], cut: bool) {
        let (name, value) = match memchr::memchr(b':', line) {
            // 注释行。
            Some(0) => return,
            Some(i) => {
                let value = &line[i + 1..];
                (
                    &line[..i],
                    value.strip_prefix(b" ".as_slice()).unwrap_or(value),
                )
            }
            None => (line, &[][..]),
        };
        match name {
            b"event" => {
                self.event.clear();
                self.event.extend_from_slice(value);
            }
            b"data" => {
                if self.data_cut {
                    return;
                }
                let sep = usize::from(!self.data.is_empty());
                if cut || self.data.len() + sep + value.len() > MAX_FIELD {
                    // 超限的 data 解析不出类型，不再保留。
                    self.data_cut = true;
                    self.data.clear();
                    return;
                }
                if sep == 1 {
                    self.data.push(b'\n');
                }
                self.data.extend_from_slice(value);
            }
            _ => {}
        }
    }

    fn dispatch(&mut self) {
        match self.event.as_slice() {
            b"message_stop" => self.saw_stop = true,
            b"error" if self.error.is_none() => {
                let etype = if self.data_cut {
                    None
                } else {
                    error_type(&self.data)
                };
                self.error = Some(etype);
            }
            _ => {}
        }
        self.event.clear();
        self.data.clear();
        self.data_cut = false;
    }
}

/// 取 error 事件 data 里的 `error.type`。
fn error_type(data: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(data).ok()?;
    v.get("error")?.get("type")?.as_str().map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(chunks: &[&[u8]]) -> EventScanner {
        let mut s = EventScanner::default();
        for c in chunks {
            s.feed(c);
        }
        s
    }

    #[test]
    fn stop_detected_with_any_line_ending() {
        for ev in [
            &b"event: message_stop\ndata: {}\n\n"[..],
            b"event: message_stop\r\ndata: {}\r\n\r\n",
            b"event: message_stop\rdata: {}\r\r",
            b"event:message_stop\n\n",
        ] {
            assert!(scan(&[ev]).saw_stop, "{:?}", String::from_utf8_lossy(ev));
        }
    }

    #[test]
    fn stop_needs_event_terminator() {
        // 空行到达之前事件尚未完成。
        let s = scan(&[b"event: message_stop\ndata: {}\n"]);
        assert!(!s.saw_stop);
    }

    #[test]
    fn stop_split_across_chunks() {
        let s = scan(&[
            b"event: message_",
            b"stop\r",
            b"\ndata: {}\r",
            b"\n\r",
            b"\n",
        ]);
        assert!(s.saw_stop);
    }

    #[test]
    fn stop_inside_data_is_not_an_event() {
        let s = scan(&[
            b"event: content_block_delta\ndata: {\"text\":\"\\nevent: message_stop\\n\"}\n\n",
            b"data: event: message_stop\n\n",
        ]);
        assert!(!s.saw_stop);
    }

    #[test]
    fn comments_ignored() {
        let s = scan(&[b":\n\n: event: message_stop\n\n"]);
        assert!(!s.saw_stop);
        assert!(s.error.is_none());
    }

    #[test]
    fn error_type_extracted() {
        let s = scan(&[
            b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"x\"}}\n\n",
        ]);
        assert_eq!(s.error, Some(Some("overloaded_error".to_string())));
    }

    #[test]
    fn error_data_before_event_line() {
        let s = scan(&[b"data: {\"error\":{\"type\":\"rate_limit_error\"}}\nevent: error\n\n"]);
        assert_eq!(s.error, Some(Some("rate_limit_error".to_string())));
    }

    #[test]
    fn error_unparseable_type_is_unknown() {
        let s = scan(&[b"event: error\ndata: not json\n\n"]);
        assert_eq!(s.error, Some(None));
    }

    #[test]
    fn error_oversized_data_is_unknown() {
        let big = format!(
            "event: error\ndata: {{\"error\":{{\"type\":\"invalid_request_error\",\"message\":\"{}\"}}}}\n\n",
            "x".repeat(MAX_FIELD)
        );
        let s = scan(&[big.as_bytes()]);
        assert_eq!(s.error, Some(None));
    }

    #[test]
    fn data_stays_within_cap_over_many_lines() {
        let mut s = EventScanner::default();
        s.feed(b"event: error\n");
        for _ in 0..100_000 {
            s.feed(b"data: x\n");
        }
        assert!(s.data.len() <= MAX_FIELD, "{}", s.data.len());
        s.feed(b"\n");
        assert_eq!(s.error, Some(None));
    }

    #[test]
    fn first_error_kept() {
        let s = scan(&[
            b"event: error\ndata: {\"error\":{\"type\":\"api_error\"}}\n\n",
            b"event: error\ndata: {\"error\":{\"type\":\"invalid_request_error\"}}\n\n",
        ]);
        assert_eq!(s.error, Some(Some("api_error".to_string())));
    }

    #[test]
    fn retryable_error_types() {
        for t in ["api_error", "overloaded_error", "upstream_error"] {
            assert!(retryable_error(Some(t)), "{t}");
        }
        assert!(retryable_error(None));
        assert!(!retryable_error(Some("invalid_request_error")));
    }

    #[test]
    fn backoff_schedule() {
        let secs: Vec<u64> = (0..6).map(|n| backoff(n).as_secs()).collect();
        assert_eq!(secs, [1, 2, 4, 8, 8, 8]);
    }
}
