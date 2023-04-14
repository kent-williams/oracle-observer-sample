use crate::{
    handler::{Handler, Mode},
    settings::Settings,
};
use anyhow::Result;
use chrono::{NaiveDateTime, TimeZone, Utc};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(long)]
    from_ts: NaiveDateTime,
}

impl Cmd {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&settings.log))
            .with(tracing_subscriber::fmt::layer())
            .init();

        let from_utc = Utc.from_utc_datetime(&self.from_ts);

        let handler_mode = Mode::Current(from_utc);
        let handler = Handler::new(settings.clone(), handler_mode).await?;
        handler.run().await?;
        Ok(())
    }
}
