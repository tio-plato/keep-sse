# Plan: SSE 包装通路的首个心跳立即到期

## 1. `tests/gateway.rs` — 先写测试

- 辅助函数 `read_head_and_first_heartbeat`：在期限内读到响应头与第一个 `:\n\n`，提前 EOF 或超时都失败。
- `test_delayed_200_sent_at_window`：2 s 间隔，上游 5 s 后才回响应头；3 s 内应收到 `HTTP/1.1 200` 与首个心跳（改前在 4 s）。
- `test_passthrough_headers_sent_without_waiting_for_data`：2 s 间隔，上游立即回响应头、3 s 后才发数据；1 s 内应收到透传的响应头（含 `x-request-id`）与首个心跳（改前在 2 s）。
- 各自在对应改动撤销时失败。

## 2. `src/sse.rs`

- `bridge` 与 `passthrough_sse_response` 的心跳计时器初值改为 `Duration::ZERO`。

## 3. `README.md`

- 「已知取舍」补上首个心跳立即到期、第一个字节不晚于一个间隔。

## 4. 验证

- `cargo test` 全部通过；`cargo clippy --all-targets` 无 warning；`cargo fmt --check` 通过。
