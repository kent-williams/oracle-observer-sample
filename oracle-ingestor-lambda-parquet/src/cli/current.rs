use crate::{
    handler::{Handler, Mode},
    settings::Settings,
};
use chrono::{NaiveDateTime, TimeZone, Utc};
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde_json::Value;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(long)]
    from_ts: NaiveDateTime,
}

impl Cmd {
    pub async fn run(&self, settings: &Settings) -> Result<(), Error> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(&settings.log))
            .with(tracing_subscriber::fmt::layer())
            .init();

        let from_utc = Utc.from_utc_datetime(&self.from_ts);

        let handler_mode = Mode::Current(from_utc);
        let handler = Arc::new(Handler::new(settings.clone(), handler_mode).await?);

        let service_fn = service_fn(move |event| {
            let handler = Arc::clone(&handler);
            async move { handler_fn(event, handler).await }
        });
        run(service_fn).await?;

        Ok(())
    }
}

async fn handler_fn(event: LambdaEvent<Value>, handler: Arc<Handler>) -> Result<(), Error> {
    handler.as_ref().run(Some(event)).await?;
    Ok(())
}
