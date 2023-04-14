use config::{Config, Environment, File};
use file_store::Settings as FSettings;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Settings {
    #[serde(default = "default_log")]
    pub log: String,
    pub ingest: FSettings,
    #[serde(default = "default_output_path")]
    pub output_path: String,
}

pub fn default_log() -> String {
    "oracle_ingestor_lambda=debug".to_string()
}

pub fn default_output_path() -> String {
    "/tmp".to_string()
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
