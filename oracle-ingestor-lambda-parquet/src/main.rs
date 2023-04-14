use anyhow::Result;
use clap::Parser;
use oracle_ingestor_lambda::{
    cli::{current, history},
    settings::Settings,
};
use serde::Serialize;
use std::path;

#[derive(Debug, Serialize)]
struct FailureResponse {
    pub body: String,
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    /// Run in historical data gathering mode
    History(history::Cmd),
    /// Start from current given timestamp
    Current(current::Cmd),
}

impl Cmd {
    pub async fn run(self, settings: Settings) -> Result<()> {
        match self {
            Self::History(cmd) => cmd.run(&settings).await,
            Self::Current(cmd) => cmd.run(&settings).await,
        }
    }
}

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Oracles Parquet Parser")]
pub struct Cli {
    /// Optional configuration file to use. If present the toml file at the
    /// given path will be loaded. Environemnt variables can override the
    /// settins in the given file.
    #[clap(short = 'c')]
    config: Option<path::PathBuf>,

    #[clap(subcommand)]
    cmd: Cmd,
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        let settings = Settings::new(self.config)?;
        self.cmd.run(settings).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
