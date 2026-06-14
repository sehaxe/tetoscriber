use anyhow::{bail, Context, Result};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AudioInfo {
    pub sample_rate_hz: u32,
    pub channels: u16,
    pub bits_per_sample: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedAudio {
    pub info: AudioInfo,
    pub samples: Vec<u8>,
}

pub async fn read_audio_file(path: &Path, default_info: AudioInfo) -> Result<DecodedAudio> {
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("failed to read audio file '{}'", path.display()))?;

    decode_audio_bytes(&bytes, default_info)
}

pub fn decode_audio_bytes(bytes: &[u8], default_info: AudioInfo) -> Result<DecodedAudio> {
    if looks_like_wav(bytes) {
        decode_wav_pcm(bytes)
    } else {
        Ok(DecodedAudio {
            info: default_info,
            samples: bytes.to_vec(),
        })
    }
}

fn looks_like_wav(bytes: &[u8]) -> bool {
    bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WAVE"
}

fn decode_wav_pcm(bytes: &[u8]) -> Result<DecodedAudio> {
    let mut offset = 12usize;
    let mut fmt: Option<WavFmt> = None;
    let mut data: Option<(usize, usize)> = None;

    while offset + 8 <= bytes.len() {
        let chunk_id = read_u32_le(bytes, offset)?;
        let chunk_size = read_u32_le(bytes, offset + 4)? as usize;
        offset += 8;

        if offset + chunk_size > bytes.len() {
            bail!("invalid WAV chunk size for chunk 0x{chunk_id:08x}");
        }

        match chunk_id {
            0x20746D66 => {
                // "fmt "
                if chunk_size < 16 {
                    bail!("WAV fmt chunk is too small: {chunk_size} bytes");
                }

                let audio_format = read_u16_le(bytes, offset)?;
                let channels = read_u16_le(bytes, offset + 2)?;
                let sample_rate_hz = read_u32_le(bytes, offset + 4)?;
                let bits_per_sample = read_u16_le(bytes, offset + 14)?;

                if audio_format != 1 {
                    bail!("unsupported WAV audio format {audio_format}; expected PCM format 1");
                }
                if channels == 0 || sample_rate_hz == 0 || bits_per_sample == 0 {
                    bail!("invalid WAV fmt values: channels={channels}, sample_rate={sample_rate_hz}, bits={bits_per_sample}");
                }
                if bits_per_sample % 8 != 0 {
                    bail!("unsupported WAV bit depth {bits_per_sample}; expected byte-aligned PCM");
                }

                fmt = Some(WavFmt {
                    channels,
                    sample_rate_hz,
                    bits_per_sample,
                });
            }
            0x61746164 => {
                // "data"
                data = Some((offset, chunk_size));
            }
            _ => {}
        }

        offset += chunk_size + (chunk_size % 2);
    }

    let Some(fmt) = fmt else {
        bail!("WAV file is missing fmt chunk");
    };
    let Some((data_offset, data_size)) = data else {
        bail!("WAV file is missing data chunk");
    };

    let expected_block_align = fmt.channels as usize * (fmt.bits_per_sample as usize / 8);
    if data_size % expected_block_align != 0 {
        bail!(
            "WAV data chunk has {} bytes, not divisible by block align {expected_block_align}",
            data_size
        );
    }

    Ok(DecodedAudio {
        info: AudioInfo {
            sample_rate_hz: fmt.sample_rate_hz,
            channels: fmt.channels,
            bits_per_sample: fmt.bits_per_sample,
        },
        samples: bytes[data_offset..data_offset + data_size].to_vec(),
    })
}

#[derive(Debug, Clone, Copy)]
struct WavFmt {
    channels: u16,
    sample_rate_hz: u32,
    bits_per_sample: u16,
}

fn read_u16_le(bytes: &[u8], offset: usize) -> Result<u16> {
    let raw = bytes
        .get(offset..offset + 2)
        .with_context(|| format!("failed to read u16 at byte offset {offset}"))?;

    Ok(u16::from_le_bytes([raw[0], raw[1]]))
}

fn read_u32_le(bytes: &[u8], offset: usize) -> Result<u32> {
    let raw = bytes
        .get(offset..offset + 4)
        .with_context(|| format!("failed to read u32 at byte offset {offset}"))?;

    Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_pcm_wav_header() {
        let wav = wav_with_data(&[0, 0, 0x80, 0x7f, 0, 0]);
        let decoded = decode_audio_bytes(
            &wav,
            AudioInfo {
                sample_rate_hz: 16_000,
                channels: 1,
                bits_per_sample: 16,
            },
        )
        .unwrap();

        assert_eq!(decoded.info.sample_rate_hz, 16_000);
        assert_eq!(decoded.info.channels, 1);
        assert_eq!(decoded.info.bits_per_sample, 16);
        assert_eq!(decoded.samples, vec![0, 0, 0x80, 0x7f, 0, 0]);
    }

    #[test]
    fn treats_non_wav_bytes_as_raw_pcm_with_default_info() {
        let decoded = decode_audio_bytes(
            &[1, 2, 3, 4],
            AudioInfo {
                sample_rate_hz: 8_000,
                channels: 1,
                bits_per_sample: 16,
            },
        )
        .unwrap();

        assert_eq!(decoded.samples, vec![1, 2, 3, 4]);
        assert_eq!(decoded.info.sample_rate_hz, 8_000);
    }

    fn wav_with_data(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"RIFF");
        out.extend_from_slice(&(36 + data.len() as u32).to_le_bytes());
        out.extend_from_slice(b"WAVE");
        out.extend_from_slice(b"fmt ");
        out.extend_from_slice(&16u32.to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes()); // PCM
        out.extend_from_slice(&1u16.to_le_bytes()); // mono
        out.extend_from_slice(&16_000u32.to_le_bytes());
        out.extend_from_slice(&32_000u32.to_le_bytes()); // byte rate
        out.extend_from_slice(&2u16.to_le_bytes()); // block align
        out.extend_from_slice(&16u16.to_le_bytes());
        out.extend_from_slice(b"data");
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(data);
        out
    }
}
