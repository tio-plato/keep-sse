use std::net::SocketAddr;
use std::process::ExitCode;

use clap::Parser;
use hyper::Uri;

/// keep-sse — LLM SSE 保活网关。
#[derive(Debug, Clone, Parser)]
#[command(name = "keep-sse", version)]
pub struct Config {
    /// 监听地址。
    #[arg(long, env = "KEEP_SSE_LISTEN", default_value = "0.0.0.0:8080")]
    pub listen: SocketAddr,

    /// 上游 base 地址，仅接受 `http://` scheme，如 `http://host:port`。
    #[arg(long, env = "KEEP_SSE_UPSTREAM")]
    pub upstream: String,

    /// 空闲保活间隔（秒）。
    #[arg(long, env = "KEEP_SSE_HEARTBEAT_INTERVAL", default_value_t = 60)]
    pub heartbeat_interval: u64,

    /// 上游 TCP 连接超时（秒）；连接建立后不设整体超时。
    #[arg(long, env = "KEEP_SSE_CONNECT_TIMEOUT", default_value_t = 10)]
    pub connect_timeout: u64,

    /// 流式探测的请求体缓冲上限（字节），超限走透明代理。
    #[arg(long, env = "KEEP_SSE_MAX_PROBE_BODY", default_value_t = 32 * 1024 * 1024)]
    pub max_probe_body: usize,

    /// 收到 SIGTERM/SIGINT 后等待存量连接完成的时长（秒），超时强制退出。
    #[arg(long, env = "KEEP_SSE_SHUTDOWN_TIMEOUT", default_value_t = 30)]
    pub shutdown_timeout: u64,

    /// 网关已先回 200 之后，上游 5xx 或请求失败时，Anthropic 错误事件改写成的 `error.type`；
    /// 不设则按现有映射。
    #[arg(long, env = "KEEP_SSE_ANTHROPIC_LATE_ERROR_TYPE")]
    pub anthropic_late_error_type: Option<String>,

    /// 先攒后发的窗口（秒）：Anthropic Messages 流式回复从上游第一个字节起最多攒这么久，
    /// 收全再一次发出，没收全前上游断了就重试；0 关闭（默认）。
    #[arg(long, env = "KEEP_SSE_HOLD_WINDOW", default_value_t = 0)]
    pub hold_window: u64,

    /// 状态码失败或连接失败后，请求到达多少秒内还能发起新的一次。
    #[arg(long, env = "KEEP_SSE_RETRY_DEADLINE", default_value_t = 180)]
    pub retry_deadline: u64,

    /// 流到一半断（还没写给客户端）后最多重试几次，不受 `--retry-deadline` 限制。
    #[arg(long, env = "KEEP_SSE_MAX_STREAM_RETRIES", default_value_t = 3)]
    pub max_stream_retries: u32,

    /// 攒着的回复超过这么多字节就不再攒，转为直播。
    #[arg(long, env = "KEEP_SSE_HOLD_MAX_BYTES", default_value_t = 8 * 1024 * 1024)]
    pub hold_max_bytes: usize,

    /// 请求体超过这么多字节就不攒、不重试（攒着时请求体要一直留在内存里）。
    #[arg(long, env = "KEEP_SSE_HOLD_MAX_BODY", default_value_t = 16 * 1024 * 1024)]
    pub hold_max_body: usize,
}

/// 校验后的运行时配置。`upstream` 已解析为 `Uri`。
#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub listen: SocketAddr,
    pub upstream: Uri,
    pub upstream_authority: String,
    pub heartbeat_interval: std::time::Duration,
    pub connect_timeout: std::time::Duration,
    pub max_probe_body: usize,
    pub shutdown_timeout: std::time::Duration,
    pub anthropic_late_error_type: Option<String>,
    /// 为零时先攒后发关闭。
    pub hold_window: std::time::Duration,
    pub retry_deadline: std::time::Duration,
    pub max_stream_retries: u32,
    pub hold_max_bytes: usize,
    pub hold_max_body: usize,
}

impl ResolvedConfig {
    /// 解析并校验配置。失败时返回要打印到 stderr 的错误信息。
    pub fn parse(cfg: Config) -> Result<Self, String> {
        let upstream: Uri = cfg
            .upstream
            .parse()
            .map_err(|e| format!("invalid --upstream `{}`: {e}", cfg.upstream))?;

        let scheme = upstream.scheme_str();
        if scheme != Some("http") {
            return Err(format!(
                "--upstream must use `http://` scheme, got `{}`",
                scheme.unwrap_or("(none)")
            ));
        }
        let upstream_authority = upstream
            .authority()
            .ok_or_else(|| "--upstream missing authority (host[:port])".to_string())?
            .as_str()
            .to_owned();

        if upstream.path() != "/" && !upstream.path().is_empty() {
            return Err(format!(
                "--upstream must not carry a path; got `{}`",
                upstream.path()
            ));
        }

        // 空白视同未设置；非空值只允许 `[a-z_]+`（Anthropic 错误类型的写法）。
        let anthropic_late_error_type =
            match cfg.anthropic_late_error_type.as_deref().map(str::trim) {
                None | Some("") => None,
                Some(t) if t.bytes().all(|b| b.is_ascii_lowercase() || b == b'_') => {
                    Some(t.to_owned())
                }
                Some(t) => {
                    return Err(format!(
                        "invalid --anthropic-late-error-type `{t}`: expected [a-z_]+"
                    ))
                }
            };

        Ok(Self {
            listen: cfg.listen,
            upstream,
            upstream_authority,
            heartbeat_interval: std::time::Duration::from_secs(cfg.heartbeat_interval),
            connect_timeout: std::time::Duration::from_secs(cfg.connect_timeout),
            max_probe_body: cfg.max_probe_body,
            shutdown_timeout: std::time::Duration::from_secs(cfg.shutdown_timeout),
            anthropic_late_error_type,
            hold_window: std::time::Duration::from_secs(cfg.hold_window),
            retry_deadline: std::time::Duration::from_secs(cfg.retry_deadline),
            max_stream_retries: cfg.max_stream_retries,
            hold_max_bytes: cfg.hold_max_bytes,
            hold_max_body: cfg.hold_max_body,
        })
    }
}

/// 解析配置；失败时打印错误并返回 `ExitCode::FAILURE`。
pub fn parse_or_exit() -> Result<ResolvedConfig, ExitCode> {
    let cfg = Config::parse();
    match ResolvedConfig::parse(cfg) {
        Ok(rc) => Ok(rc),
        Err(e) => {
            eprintln!("keep-sse: {e}");
            Err(ExitCode::FAILURE)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve_late(value: &str) -> Result<ResolvedConfig, String> {
        let cfg = Config::try_parse_from([
            "keep-sse",
            "--upstream",
            "http://127.0.0.1:9",
            "--anthropic-late-error-type",
            value,
        ])
        .expect("命令行应能被 clap 解析");
        ResolvedConfig::parse(cfg)
    }

    #[test]
    fn late_error_type_rejects_invalid() {
        for value in ["overloaded error", "Overloaded"] {
            let err = resolve_late(value).expect_err(value);
            assert!(err.contains("--anthropic-late-error-type"), "{err}");
        }
    }

    #[test]
    fn late_error_type_blank_is_none() {
        let rc = resolve_late("  ").unwrap();
        assert_eq!(rc.anthropic_late_error_type, None);
    }

    #[test]
    fn late_error_type_valid_is_trimmed() {
        let rc = resolve_late(" overloaded_error ").unwrap();
        assert_eq!(
            rc.anthropic_late_error_type.as_deref(),
            Some("overloaded_error")
        );
    }
}
