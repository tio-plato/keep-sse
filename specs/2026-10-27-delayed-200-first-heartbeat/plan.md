# Plan: 窗口超时发 200 时立即写一个心跳

## 1. `tests/gateway.rs` — 先写测试

- `test_delayed_200_sent_at_window`：1 s 间隔，上游 3 s 后才回响应头。客户端在 1.6 s 内应读到 `HTTP/1.1 200` 和第一个 `:\n\n`；随后仍能收到上游数据。
- 改前运行：1.6 s 超时失败（第一个字节在 2 s）。

## 2. `src/sse.rs` — `bridge` 开头写心跳

- 创建 `SseWriter` 后立即 `write_event(HEARTBEAT)`，失败则返回。

## 3. `README.md`

- 「已知取舍」中"窗口超时则网关先行发出 200"补上"并立即写一个心跳注释"。

## 4. 验证

- `cargo test` 全部通过；`cargo clippy --all-targets` 无 warning；`cargo fmt --check` 通过。
