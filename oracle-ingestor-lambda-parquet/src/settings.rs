use config::{Config, Environment, File};
use file_store::Settings as FSettings;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Settings {
    // Configure logging level = debug
    #[serde(default = "default_log")]
    pub log: String,
    // Configure ingest file store settings
    pub ingest: FSettings,
    // Configure cache path (tmp store for output parquet files before upload)
    #[serde(default = "default_cache")]
    pub cache: String,
    // Configure output bucket
    #[serde(default = "default_output_bucket")]
    pub output_bucket: String,
    // Configure output region
    #[serde(default = "default_output_region")]
    pub output_region: String,
    // Configure output bucket endpoint
    pub output_endpoint: Option<String>,
    #[cfg(feature = "local")]
    pub output_secret_access_key: Option<String>,
    #[cfg(feature = "local")]
    pub output_access_key_id: Option<String>,
    #[cfg(feature = "local")]
    pub output_session_token: Option<String>,
}

pub fn default_log() -> String {
    "oracle_ingestor_lambda=debug".to_string()
}

pub fn default_cache() -> String {
    "/tmp".to_string()
}

pub fn default_output_bucket() -> String {
    "oracle-parquet-test".to_string()
}

pub fn default_output_region() -> String {
    "us-west-2".to_string()
}

impl Settings {
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, config::ConfigError> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        builder
            .add_source(Environment::with_prefix("LAMBDA_PARQUET").separator("_"))
            .build()
            .and_then(|config| config.try_deserialize())
    }
}
