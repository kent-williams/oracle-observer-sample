use crate::{
    handler::{Handler, Mode},
    settings::Settings,
};
use anyhow::Result;
use chrono::{NaiveDateTime, TimeZone, Utc};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Required start time to look for (inclusive)
    #[clap(long)]
    after: NaiveDateTime,
    /// Required before time to look for (inclusive)
    #[clap(long)]
    before: NaiveDateTime,
}

impl Cmd {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&settings.log))
            .with(tracing_subscriber::fmt::layer())
            .init();

        let after_utc = Utc.from_utc_datetime(&self.after);
        let before_utc = Utc.from_utc_datetime(&self.before);

        let handler_mode = Mode::Historical(before_utc, after_utc);
        let handler = Handler::new(settings.clone(), handler_mode).await?;
        handler.run(None).await?;
        Ok(())
    }
}
