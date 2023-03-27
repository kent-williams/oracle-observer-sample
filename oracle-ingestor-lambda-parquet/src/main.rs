use anyhow::anyhow;
use chrono::{TimeZone, Utc};
use file_store::{FileStore, FileType, Settings};
use futures::StreamExt;
use helium_crypto::PublicKey;
use helium_proto::{
    services::{poc_lora::GatewayRewardShare, poc_mobile::RadioRewardShare},
    Message,
};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::{json, Value};
// use sqlx::postgres::PgPoolOptions;
use std::{env, str::FromStr};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let handler = service_fn(handler);
    lambda_runtime::run(handler).await?;
    Ok(())
}

async fn handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let (event, _context) = event.into_parts();

    // guard against empty records
    if event["Records"].is_null() {
        return Err(anyhow!("Event records are unexpectedly null.").into());
    }

    let record = &event["Records"][0];
    let bucket = record["s3"]["bucket"]["name"]
        .as_str()
        .unwrap_or("bucket not found");
    let key = record["s3"]["object"]["key"]
        .as_str()
        .unwrap_or("key not found");
    let region = record["awsRegion"].as_str().unwrap_or("region not found");

    let settings = &Settings {
        region: region.to_string(),
        bucket: bucket.to_string(),
        endpoint: None,
    };

    let prefix = key.split('.').next().unwrap_or("");
    let file_type = FileType::from_str(prefix)?;
    let store = FileStore::from_settings(settings).await?;
    let mut file_stream = store.get(key).await?;

    println!("bucket is {}", bucket);
    println!("key is {}", key);
    println!("region is {}", region);

    let mut count = 0;
    while let Some(result) = file_stream.next().await {
        let msg = result?;
        count += 1;
        match file_type {
            FileType::RadioRewardShare => {
                let reward = RadioRewardShare::decode(msg)?;
                let end_epoch = Utc.timestamp_opt(reward.end_epoch as i64, 0);
                if let chrono::LocalResult::Single(end_epoch) = end_epoch {
                    println!("Do something");
                } else {
                    return Err(anyhow!("Unexpected end_epoch: {end_epoch:?}").into());
                }
            }
            FileType::GatewayRewardShare => {
                let reward = GatewayRewardShare::decode(msg)?;
                let end_period = Utc.timestamp_opt(reward.end_period as i64, 0);
                if let chrono::LocalResult::Single(end_period) = end_period {
                    println!("Do something");
                } else {
                    return Err(anyhow!("Unexpected end_epoch: {end_period:?}").into());
                }
            }
            _ => (),
        }
    }

    let message = format!("{count} rows of {prefix} processed.");
    Ok(json!({ "message": message }))
}
