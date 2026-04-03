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
}
