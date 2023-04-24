use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde_json::Value;

async fn function_handler(event: LambdaEvent<Value>) -> Result<(), Error> {
    tracing::info!("event: {:?}", event);
    // TODO
    // - Get the settings and create handler
    // - Invoke handler::run with current mode
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    run(service_fn(function_handler)).await
}
