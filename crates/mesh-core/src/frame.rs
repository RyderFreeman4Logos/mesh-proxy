use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_FRAME_SIZE: u32 = 1024 * 1024; // 1 MiB

/// Read a length-prefixed frame: 4-byte big-endian length + payload.
pub async fn read_frame<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .context("failed to read frame length")?;
    let len = u32::from_be_bytes(len_buf);

    if len > MAX_FRAME_SIZE {
        bail!("frame too large: {len} bytes (max {MAX_FRAME_SIZE})");
    }

    let mut payload = vec![0u8; len as usize];
    reader
        .read_exact(&mut payload)
        .await
        .context("failed to read frame payload")?;
    Ok(payload)
}

/// Write a length-prefixed frame: 4-byte big-endian length + payload.
pub async fn write_frame<W: AsyncWriteExt + Unpin>(writer: &mut W, payload: &[u8]) -> Result<()> {
    let len = u32::try_from(payload.len()).context("payload too large for frame")?;
    writer
        .write_all(&len.to_be_bytes())
        .await
        .context("failed to write frame length")?;
    writer
        .write_all(payload)
        .await
        .context("failed to write frame payload")?;
    writer.flush().await.context("failed to flush frame")?;
    Ok(())
}

/// Read a frame and deserialize as JSON.
pub async fn read_json<R: AsyncReadExt + Unpin, T: serde::de::DeserializeOwned>(
    reader: &mut R,
) -> Result<T> {
    let payload = read_frame(reader).await?;
    serde_json::from_slice(&payload).context("failed to deserialize frame payload")
}

/// Serialize as JSON and write as a frame.
pub async fn write_json<W: AsyncWriteExt + Unpin, T: serde::Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<()> {
    let payload = serde_json::to_vec(value).context("failed to serialize frame payload")?;
    write_frame(writer, &payload).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_frame_roundtrip() {
        let (mut writer, mut reader) = tokio::io::duplex(1024);

        let payload = b"hello mesh-proxy";
        write_frame(&mut writer, payload).await.unwrap();

        let received = read_frame(&mut reader).await.unwrap();
        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn test_json_roundtrip() {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        struct Msg {
            value: u32,
            text: String,
        }

        let (mut writer, mut reader) = tokio::io::duplex(1024);

        let msg = Msg {
            value: 42,
            text: "hello".into(),
        };
        write_json(&mut writer, &msg).await.unwrap();

        let received: Msg = read_json(&mut reader).await.unwrap();
        assert_eq!(received, msg);
    }

    #[tokio::test]
    async fn test_frame_too_large_rejected() {
        let (mut writer, mut reader) = tokio::io::duplex(64);

        // Write a length header claiming a payload larger than MAX_FRAME_SIZE
        let fake_len = (MAX_FRAME_SIZE + 1).to_be_bytes();
        writer.write_all(&fake_len).await.unwrap();

        let result = read_frame(&mut reader).await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("frame too large"),
            "error should mention frame size"
        );
    }

    #[tokio::test]
    async fn test_empty_frame() {
        let (mut writer, mut reader) = tokio::io::duplex(1024);

        write_frame(&mut writer, b"").await.unwrap();

        let received = read_frame(&mut reader).await.unwrap();
        assert!(received.is_empty());
    }
}
