# Plan: Anthropic Messages 流式回复先攒后发 + 重试

## 1. `src/config.rs`

- `Config`、`ResolvedConfig` 加 `hold_window`、`retry_deadline`、`max_stream_retries`、`hold_max_bytes`、`hold_max_body`。

## 2. `src/sse.rs`、`src/encoding.rs` — 提取共用部分

- `prepare_upstream_parts`、`gateway_sse_response`、`passthrough_head`、`bridge_stream`、`deliver_late`、`SseWriter::closed`、`DecodedStream`，均不改行为。
- `handle_stream` 增加 `arrived` 参数，按条件分派到 `hold::handle`。
- 现有测试全部照旧通过。

## 3. `src/hold.rs`

- `EventScanner`（单元测试：`message_stop`、error 类型、事件跨块、CRLF/CR 行尾、注释、data 在 event 前、未知类型）。
- 模板、`attempt`、`Downstream`、驱动循环、日志。

## 4. `tests/gateway.rs` — 集成测试

mock 上游按请求序号决定每次回复，可以在发完若干块后直接关连接（不发结尾块），记录每次的请求体以及连接是否被网关提前关闭。

1. 第一次流到一半断，第二次完整：客户端只收到第二次的事件，没有 error 事件；上游收到 2 次、请求体相同。
2. 第一次 502、第二次成功：客户端收到成功的流。
3. 400：立即透传状态码与错误体，上游只收到 1 次。
4. 第一次中途 `api_error` 事件后结束，第二次完整：客户端收到完整流。
5. 中途 `invalid_request_error` / `rate_limit_error` 事件：不重试，客户端收到该事件。
6. 流断次数超过上限：上游收到 `max_stream_retries + 1` 次，客户端收不到半截内容；全在窗口内时为 502 + `api_error` JSON，拖过窗口时为 200、心跳和改写后的 `overloaded_error` 事件。
7. 一直 502、超过重试时限：不再发起新的一次；窗口内透传 502，窗口后发 error 事件。
8. 请求到达超过重试时限后才第一次流断：仍然重试。
9. 请求体超过 `hold_max_body`：不攒、不重试，流断时客户端收到的与现在相同。
10. 攒满 W：先发出已攒的部分，之后直播；再断按现在的方式发 error 事件，不重试。
11. 攒着时客户端断开：上游连接被关掉，之后没有新的请求；日志 `outcome=cancelled` 在单独的测试二进制 `tests/hold_log.rs` 里断言（只注册了一个 dispatcher 时，tracing 按第一次命中 callsite 的线程缓存 interest，与其他测试并行会被缓存成 never）。
12. 攒着超过心跳窗口：客户端先收到 200 和心跳，收全后收到完整事件流。
13. `hold_window = 0`：现有测试全部照旧通过。

## 5. `README.md`

配置表加五行；新增「断流重试」小节；「已知取舍」补上不再逐字显示、重试会让上游重新生成。

## 6. 验证

- 先写测试：未实现时新测试失败；只断言"不重试"的几条（3、5、9）在旧代码上本就成立，用临时改坏重试判定的方式确认它们能抓到误重试；日志测试用临时去掉结束日志的方式确认。
- `cargo fmt --check`、`cargo clippy --all-targets -- -D warnings`、`cargo test` 全部通过。
