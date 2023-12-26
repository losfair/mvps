use std::time::Duration;

use bytestring::ByteString;
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum ImageStoreProvider {
  LocalFs {
    path: ByteString,
  },
  S3 {
    bucket: ByteString,
    prefix: ByteString,
  },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LayeredStoreConfig {
  pub compaction_thresholds: Vec<CompactionThreshold>,
  pub compaction_interval: JitteredInterval,
  pub disable_image_store_write: bool,
}

impl Default for LayeredStoreConfig {
  fn default() -> Self {
    Self {
      compaction_thresholds: vec![
        CompactionThreshold {
          max_input_size: 64 * 1024,
          min_output_size: 256 * 1024,
          max_output_size: 16 * 1024 * 1024,
          pressure_level: 0,
        },
        CompactionThreshold {
          max_input_size: 1 * 1024 * 1024,
          min_output_size: 8 * 1024 * 1024,
          max_output_size: 32 * 1024 * 1024,
          pressure_level: 8,
        },
        CompactionThreshold {
          max_input_size: 256 * 1024,
          min_output_size: 512 * 1024,
          max_output_size: 8 * 1024 * 1024,
          pressure_level: 8,
        },
        CompactionThreshold {
          max_input_size: 8 * 1024 * 1024,
          min_output_size: 32 * 1024 * 1024,
          max_output_size: 128 * 1024 * 1024,
          pressure_level: 16,
        },
        CompactionThreshold {
          max_input_size: 32 * 1024 * 1024,
          min_output_size: 128 * 1024 * 1024,
          max_output_size: 512 * 1024 * 1024,
          pressure_level: 16,
        },
        CompactionThreshold {
          max_input_size: 256 * 1024 * 1024,
          min_output_size: 512 * 1024 * 1024,
          max_output_size: 1024 * 1024 * 1024,
          pressure_level: 24,
        },
        CompactionThreshold {
          max_input_size: 1024 * 1024 * 1024,
          min_output_size: 2048 * 1024 * 1024,
          max_output_size: 8192 * 1024 * 1024,
          pressure_level: 32,
        },
      ],
      compaction_interval: JitteredInterval {
        min_ms: 30000,
        jitter_ms: 10000,
      },
      disable_image_store_write: false,
    }
  }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JitteredInterval {
  pub min_ms: u64,
  pub jitter_ms: u64,
}

impl JitteredInterval {
  pub fn generate(&self) -> Duration {
    let jitter = rand::thread_rng().gen_range(0..self.jitter_ms);
    Duration::from_millis(self.min_ms + jitter)
  }

  pub async fn sleep(&self) {
    tokio::time::sleep(self.generate()).await;
  }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionThreshold {
  pub max_input_size: u64,
  pub min_output_size: u64,
  pub max_output_size: u64,
  pub pressure_level: u64,
}
