use std::net::{IpAddr, Ipv4Addr};

use crate::{SERVICE_PORT_END, SERVICE_PORT_START};

/// Maximum length of a service name in bytes.
pub const MAX_SERVICE_NAME_LEN: usize = 128;

/// Maximum length of a local address string in bytes.
pub const MAX_LOCAL_ADDR_LEN: usize = 256;

/// Maximum number of services in a single registration message.
pub const MAX_SERVICES_PER_REGISTRATION: usize = 32;

/// Maximum length of a serialized join ticket in bytes.
pub const MAX_TICKET_LEN: usize = 512;

/// Validate a service name.
///
/// A valid service name is non-empty, within [`MAX_SERVICE_NAME_LEN`], and
/// contains only ASCII alphanumeric characters, hyphens, or underscores.
pub fn validate_service_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("service name must not be empty");
    }
    if name.len() > MAX_SERVICE_NAME_LEN {
        return Err("service name exceeds maximum length");
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(
            "service name contains invalid characters (allowed: alphanumeric, hyphen, underscore)",
        );
    }
    Ok(())
}

/// Validate a local address string.
///
/// A valid local address is non-empty and within [`MAX_LOCAL_ADDR_LEN`].
pub fn validate_local_addr(addr: &str) -> Result<(), &'static str> {
    if addr.is_empty() {
        return Err("local address must not be empty");
    }
    if addr.len() > MAX_LOCAL_ADDR_LEN {
        return Err("local address exceeds maximum length");
    }
    Ok(())
}

/// Validate that a port is within the dynamic service listener range.
pub fn validate_port_in_service_range(port: u16) -> Result<(), String> {
    if (SERVICE_PORT_START..=SERVICE_PORT_END).contains(&port) {
        return Ok(());
    }

    Err(format!(
        "port must be in service range {SERVICE_PORT_START}..={SERVICE_PORT_END}"
    ))
}

/// Validate that a health-check target resolves to localhost or a private IP.
pub fn validate_health_target(target: &str) -> Result<(), String> {
    if target == "localhost" {
        return Ok(());
    }

    let ip = target
        .parse::<IpAddr>()
        .map_err(|_| "health target must be localhost or a private/loopback IP".to_string())?;

    if is_allowed_health_ip(ip) {
        return Ok(());
    }

    Err("health target must be localhost or a private/loopback IP".to_string())
}

fn is_allowed_health_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ipv4) => is_allowed_health_ipv4(ipv4),
        IpAddr::V6(ipv6) => ipv6.is_loopback(),
    }
}

fn is_allowed_health_ipv4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();

    octets[0] == 127
        || octets[0] == 10
        || (octets[0] == 172 && (16..=31).contains(&octets[1]))
        || (octets[0] == 192 && octets[1] == 168)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_service_name_valid() {
        assert!(validate_service_name("llama3_api").is_ok());
        assert!(validate_service_name("my-service").is_ok());
        assert!(validate_service_name("svc01").is_ok());
    }

    #[test]
    fn test_validate_service_name_empty() {
        assert_eq!(
            validate_service_name(""),
            Err("service name must not be empty")
        );
    }

    #[test]
    fn test_validate_service_name_too_long() {
        let long_name = "a".repeat(MAX_SERVICE_NAME_LEN + 1);
        assert_eq!(
            validate_service_name(&long_name),
            Err("service name exceeds maximum length")
        );
    }

    #[test]
    fn test_validate_service_name_at_limit() {
        let name = "a".repeat(MAX_SERVICE_NAME_LEN);
        assert!(validate_service_name(&name).is_ok());
    }

    #[test]
    fn test_validate_service_name_invalid_chars() {
        assert!(validate_service_name("has space").is_err());
        assert!(validate_service_name("has/slash").is_err());
        assert!(validate_service_name("has.dot").is_err());
    }

    #[test]
    fn test_validate_local_addr_valid() {
        assert!(validate_local_addr("127.0.0.1:8080").is_ok());
        assert!(validate_local_addr("/tmp/foo.sock").is_ok());
    }

    #[test]
    fn test_validate_local_addr_empty() {
        assert_eq!(
            validate_local_addr(""),
            Err("local address must not be empty")
        );
    }

    #[test]
    fn test_validate_local_addr_too_long() {
        let long_addr = "x".repeat(MAX_LOCAL_ADDR_LEN + 1);
        assert_eq!(
            validate_local_addr(&long_addr),
            Err("local address exceeds maximum length")
        );
    }

    #[test]
    fn test_validate_port_in_service_range_valid() {
        assert!(validate_port_in_service_range(40000).is_ok());
        assert!(validate_port_in_service_range(45000).is_ok());
        assert!(validate_port_in_service_range(48999).is_ok());
    }

    #[test]
    fn test_validate_port_in_service_range_rejects_out_of_range_values() {
        assert_eq!(
            validate_port_in_service_range(39999),
            Err("port must be in service range 40000..=48999".to_string())
        );
        assert_eq!(
            validate_port_in_service_range(49000),
            Err("port must be in service range 40000..=48999".to_string())
        );
    }

    #[test]
    fn test_validate_health_target_accepts_allowed_inputs() {
        for target in [
            "localhost",
            "127.0.0.1",
            "127.255.255.255",
            "::1",
            "10.0.0.0",
            "10.255.255.255",
            "172.16.0.0",
            "172.31.255.255",
            "192.168.0.0",
            "192.168.255.255",
        ] {
            assert!(
                validate_health_target(target).is_ok(),
                "expected {target} to be accepted"
            );
        }
    }

    #[test]
    fn test_validate_health_target_rejects_disallowed_ips() {
        for target in [
            "",
            "8.8.8.8",
            "126.255.255.255",
            "128.0.0.1",
            "::",
            "172.15.255.255",
            "172.32.0.0",
            "192.167.255.255",
            "192.169.0.0",
        ] {
            assert!(
                validate_health_target(target).is_err(),
                "expected {target} to be rejected"
            );
        }
    }

    #[test]
    fn test_validate_health_target_rejects_non_local_hostnames() {
        for target in ["example.com", "mesh-proxy.local", "localhost.localdomain"] {
            assert!(
                validate_health_target(target).is_err(),
                "expected {target} to be rejected"
            );
        }
    }
}
