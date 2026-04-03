use std::time::Duration;

use hyper::Uri;
use mesh_proto::{
    HealthCheckConfig, HealthCheckMode, HealthState, validate_health_target, validate_local_addr,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;

const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_STATUS_LINE_LEN: u64 = 8192;

/// Probes local services using the configured health-check mode.
pub struct HealthProber;

impl HealthProber {
    /// Execute a single health probe and return the resulting state plus the last error.
    pub async fn probe(config: &HealthCheckConfig) -> (HealthState, Option<String>) {
        let Some(target) = config.target.as_deref() else {
            return unhealthy("health check target is required");
        };

        if target.is_empty() {
            return unhealthy("health check target must not be empty");
        }

        match config.mode {
            HealthCheckMode::TcpConnect => probe_tcp_connect(target).await,
            HealthCheckMode::UnixConnect => probe_unix_connect(target).await,
            HealthCheckMode::HttpGet => probe_http_get(target).await,
        }
    }
}

async fn probe_tcp_connect(target: &str) -> (HealthState, Option<String>) {
    if let Err(error) = validate_tcp_target(target) {
        return unhealthy(error);
    }

    match timeout(PROBE_TIMEOUT, TcpStream::connect(target)).await {
        Ok(Ok(_stream)) => healthy(),
        Ok(Err(error)) => unhealthy(format!("tcp connect failed: {error}")),
        Err(_elapsed) => unhealthy(timeout_error("tcp connect")),
    }
}

#[cfg(unix)]
async fn probe_unix_connect(target: &str) -> (HealthState, Option<String>) {
    if let Err(error) = validate_local_addr(target) {
        return unhealthy(format!("invalid Unix health check target: {error}"));
    }

    match timeout(PROBE_TIMEOUT, tokio::net::UnixStream::connect(target)).await {
        Ok(Ok(_stream)) => healthy(),
        Ok(Err(error)) => unhealthy(format!("unix connect failed: {error}")),
        Err(_elapsed) => unhealthy(timeout_error("unix connect")),
    }
}

#[cfg(not(unix))]
async fn probe_unix_connect(_target: &str) -> (HealthState, Option<String>) {
    unhealthy("unix socket health probes are only supported on Unix")
}

async fn probe_http_get(target: &str) -> (HealthState, Option<String>) {
    let request = match build_http_probe_request(target) {
        Ok(request) => request,
        Err(error) => return unhealthy(error),
    };

    let status_line = match timeout(PROBE_TIMEOUT, execute_http_probe(&request)).await {
        Ok(Ok(status_line)) => status_line,
        Ok(Err(error)) => return unhealthy(format!("http GET failed: {error}")),
        Err(_elapsed) => return unhealthy(timeout_error("http GET")),
    };

    match parse_status_code(&status_line) {
        Ok(status) if (200..300).contains(&status) => healthy(),
        Ok(status) => unhealthy(format!("unexpected HTTP status: {status}")),
        Err(error) => unhealthy(error),
    }
}

fn validate_tcp_target(target: &str) -> Result<(), String> {
    let uri = format!("http://{target}")
        .parse::<Uri>()
        .map_err(|_| "invalid TCP health check target: expected host:port".to_string())?;

    if uri.path() != "/" || uri.query().is_some() {
        return Err("invalid TCP health check target: expected host:port".to_string());
    }

    let host = uri
        .host()
        .ok_or_else(|| "invalid TCP health check target: missing host".to_string())?;
    let Some(_port) = uri.port_u16() else {
        return Err("invalid TCP health check target: missing port".to_string());
    };
    let host = normalize_uri_host(host);

    validate_health_target(host)
        .map_err(|error| format!("invalid TCP health check target: {error}"))?;

    Ok(())
}

fn build_http_probe_request(target: &str) -> Result<HttpProbeRequest, String> {
    let uri = target.parse::<Uri>().map_err(|_| {
        "invalid HTTP health check target: expected absolute http:// URL".to_string()
    })?;

    match uri.scheme_str() {
        Some("http") => {}
        Some("https") => {
            return Err(
                "invalid HTTP health check target: https is not supported by the local prober"
                    .to_string(),
            );
        }
        _ => {
            return Err(
                "invalid HTTP health check target: expected absolute http:// URL".to_string(),
            );
        }
    }

    let host = uri
        .host()
        .ok_or_else(|| "invalid HTTP health check target: missing host".to_string())?;
    let host = normalize_uri_host(host);
    validate_health_target(host)
        .map_err(|error| format!("invalid HTTP health check target: {error}"))?;

    let authority = uri
        .authority()
        .ok_or_else(|| "invalid HTTP health check target: missing authority".to_string())?;
    let path_and_query = uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/");
    let connect_addr = if authority.port_u16().is_some() {
        authority.as_str().to_string()
    } else {
        format_host_port(host, 80)
    };

    Ok(HttpProbeRequest {
        connect_addr,
        host_header: authority.as_str().to_string(),
        path_and_query: path_and_query.to_string(),
    })
}

async fn execute_http_probe(request: &HttpProbeRequest) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(&request.connect_addr).await?;
    let request_bytes = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        request.path_and_query, request.host_header
    );
    stream.write_all(request_bytes.as_bytes()).await?;

    let mut reader = BufReader::new(stream).take(MAX_STATUS_LINE_LEN);
    let mut status_line = String::new();
    let bytes_read = reader.read_line(&mut status_line).await?;
    if bytes_read == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing HTTP status line",
        ));
    }
    if bytes_read == MAX_STATUS_LINE_LEN as usize && !status_line.ends_with('\n') {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "HTTP status line exceeds maximum length",
        ));
    }

    Ok(status_line)
}

fn parse_status_code(status_line: &str) -> Result<u16, String> {
    let mut parts = status_line.split_whitespace();
    let version = parts
        .next()
        .ok_or_else(|| "invalid HTTP response: missing status line".to_string())?;
    if !version.starts_with("HTTP/") {
        return Err("invalid HTTP response: missing HTTP version".to_string());
    }

    let status = parts
        .next()
        .ok_or_else(|| "invalid HTTP response: missing status code".to_string())?
        .parse::<u16>()
        .map_err(|_| "invalid HTTP response: invalid status code".to_string())?;

    Ok(status)
}

fn format_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn normalize_uri_host(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host)
}

fn healthy() -> (HealthState, Option<String>) {
    (HealthState::Healthy, None)
}

fn unhealthy(error: impl Into<String>) -> (HealthState, Option<String>) {
    (HealthState::Unhealthy, Some(error.into()))
}

fn timeout_error(label: &str) -> String {
    format!("{label} timed out after {}s", PROBE_TIMEOUT.as_secs())
}

struct HttpProbeRequest {
    connect_addr: String,
    host_header: String,
    path_and_query: String,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use mesh_proto::{HealthCheckConfig, HealthCheckMode, HealthState};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use super::HealthProber;

    #[tokio::test]
    async fn test_probe_tcp_connect_success() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should expose local addr");

        let accept_task = tokio::spawn(async move {
            let (_stream, _peer) = listener.accept().await.expect("probe should connect");
        });

        let config = HealthCheckConfig {
            mode: HealthCheckMode::TcpConnect,
            target: Some(addr.to_string()),
            interval_seconds: 10,
        };

        let result = HealthProber::probe(&config).await;
        accept_task.await.expect("accept task should complete");

        assert_eq!(result, (HealthState::Healthy, None));
    }

    #[tokio::test]
    async fn test_probe_tcp_connect_failure() {
        let unused_port = reserve_unused_tcp_port();
        let config = HealthCheckConfig {
            mode: HealthCheckMode::TcpConnect,
            target: Some(format!("127.0.0.1:{unused_port}")),
            interval_seconds: 10,
        };

        let (state, last_error) = HealthProber::probe(&config).await;

        assert_eq!(state, HealthState::Unhealthy);
        assert!(last_error.is_some(), "failed probe should report an error");
    }

    #[tokio::test]
    async fn test_probe_http_get_success() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should expose local addr");

        let server_task = tokio::spawn(async move {
            let (mut stream, _peer) = listener.accept().await.expect("probe should connect");
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .expect("server should write response");
        });

        let config = HealthCheckConfig {
            mode: HealthCheckMode::HttpGet,
            target: Some(format!("http://{addr}/healthz")),
            interval_seconds: 10,
        };

        let result = HealthProber::probe(&config).await;
        server_task.await.expect("server task should complete");

        assert_eq!(result, (HealthState::Healthy, None));
    }

    #[tokio::test]
    async fn test_probe_http_get_failure() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should expose local addr");

        let server_task = tokio::spawn(async move {
            let (_stream, _peer) = listener.accept().await.expect("probe should connect");
            tokio::time::sleep(super::PROBE_TIMEOUT + Duration::from_secs(1)).await;
        });

        let config = HealthCheckConfig {
            mode: HealthCheckMode::HttpGet,
            target: Some(format!("http://{addr}/healthz")),
            interval_seconds: 10,
        };

        let (state, last_error) = HealthProber::probe(&config).await;
        server_task.await.expect("server task should complete");

        assert_eq!(state, HealthState::Unhealthy);
        assert!(
            last_error
                .as_deref()
                .is_some_and(|error| error.contains("timed out")),
            "expected timeout error, got {last_error:?}"
        );
    }

    #[test]
    fn test_build_http_probe_request_normalizes_ipv6_host_without_port() {
        let request =
            super::build_http_probe_request("http://[::1]/healthz").expect("request should build");

        assert_eq!(request.connect_addr, "[::1]:80");
        assert_eq!(request.host_header, "[::1]");
        assert_eq!(request.path_and_query, "/healthz");
    }

    #[tokio::test]
    async fn test_probe_http_get_rejects_overlong_status_line() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should expose local addr");

        let server_task = tokio::spawn(async move {
            let (mut stream, _peer) = listener.accept().await.expect("probe should connect");
            let overlong_reason = "A".repeat(super::MAX_STATUS_LINE_LEN as usize);
            let response = format!(
                "HTTP/1.1 200 {overlong_reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("server should write response");
        });

        let config = HealthCheckConfig {
            mode: HealthCheckMode::HttpGet,
            target: Some(format!("http://{addr}/healthz")),
            interval_seconds: 10,
        };

        let (state, last_error) = HealthProber::probe(&config).await;
        server_task.await.expect("server task should complete");

        assert_eq!(state, HealthState::Unhealthy);
        assert!(
            last_error
                .as_deref()
                .is_some_and(|error| error.contains("exceeds maximum length")),
            "expected status line length error, got {last_error:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_probe_unix_connect() {
        let tempdir = tempfile::Builder::new()
            .prefix("mesh-core-health-prober-")
            .tempdir_in("/tmp")
            .expect("tempdir should be created");
        let socket_path = tempdir.path().join("probe.sock");
        let listener =
            tokio::net::UnixListener::bind(&socket_path).expect("listener should bind socket");

        let accept_task = tokio::spawn(async move {
            let (_stream, _addr) = listener.accept().await.expect("probe should connect");
        });

        let config = HealthCheckConfig {
            mode: HealthCheckMode::UnixConnect,
            target: Some(path_to_string(&socket_path)),
            interval_seconds: 10,
        };

        let result = HealthProber::probe(&config).await;
        accept_task.await.expect("accept task should complete");

        assert_eq!(result, (HealthState::Healthy, None));
    }

    fn reserve_unused_tcp_port() -> u16 {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("temporary listener should bind");
        let port = listener
            .local_addr()
            .expect("listener should expose local addr")
            .port();
        drop(listener);
        port
    }

    #[cfg(unix)]
    fn path_to_string(path: &std::path::Path) -> String {
        PathBuf::from(path)
            .into_os_string()
            .into_string()
            .expect("socket path should be valid UTF-8")
    }
}
