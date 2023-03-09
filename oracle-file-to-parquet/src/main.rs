use anyhow::anyhow;
use chrono::{TimeZone, Utc};
use file_store::{FileStore, FileType, FileInfo, FileInfoStream, Settings};
use futures::StreamExt;
use helium_crypto::PublicKey;
use helium_proto::{
    services::{poc_lora::GatewayRewardShare, poc_mobile::RadioRewardShare},
    Message,
};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::{json, Value};
use std::{env, str::FromStr};

#[tokio::main]
async fn main() -> Result<(), Error> {
    handler().await?;
    Ok(())
}

fn infos(names: &'static [&str]) -> FileInfoStream {
    futures::stream::iter(names.iter().map(|v| FileInfo::from_str(v))).boxed()
}

async fn handler() -> Result<Value, Error> {
    let file_store = FileStore::new(None, "us-west-2", "mainnet-pociot-verified")
            .await
            .expect("file store");

    let key = "gateway_reward_share.1671643842138.gz";

    let prefix = key.split('.').next().unwrap_or("");
    let file_type = FileType::from_str(prefix)?;
    // let store = FileStore::from_settings(settings).await?;

    let mut file_stream = file_store.source(infos(&["gateway_reward_share.1671643842138.gz"]));

    // let mut file_stream = store.get(key).await?;

    // println!("bucket is {}", bucket);
    println!("key is {}", key);
    // println!("region is {}", region);

    let mut count = 0;
    while let Some(result) = file_stream.next().await {
        let msg = result?;
        count += 1;
        match file_type {
            FileType::RadioRewardShare => {
                let reward = RadioRewardShare::decode(msg)?;
                let end_epoch = Utc.timestamp_opt(reward.end_epoch as i64, 0);
                if let chrono::LocalResult::Single(end_epoch) = end_epoch {
                    // sqlx::query(
                    //     r#"
                    //     INSERT INTO mobile_poc_rewards (amount, epoch_end, hotspot_key, cbsd_id)
                    //     VALUES ($1, $2, $3, $4)
                    //     ON CONFLICT
                    //     DO NOTHING
                    //     "#,
                    // )
                    // .bind(reward.amount as i64)
                    // .bind(end_epoch)
                    // .bind(PublicKey::try_from(reward.hotspot_key)?)
                    // .bind(reward.cbsd_id)
                    // .execute(&pool)
                    // .await?;
                } else {
                    return Err(anyhow!("Unexpected end_epoch: {end_epoch:?}").into());
                }
            }
            FileType::GatewayRewardShare => {
                let reward = GatewayRewardShare::decode(msg)?;
                let end_period = Utc.timestamp_opt(reward.end_period as i64, 0);
                if let chrono::LocalResult::Single(end_period) = end_period {
                    // sqlx::query(
                    //     r#"
                    //     INSERT INTO iot_poc_rewards (beacon_amount, witness_amount, epoch_end, hotspot_key)
                    //     VALUES ($1, $2, $3, $4)
                    //     ON CONFLICT
                    //     DO NOTHING
                    //     "#
                    //     , )
                    //     .bind(reward.beacon_amount as i64)
                    //     .bind(reward.witness_amount as i64)
                    //     .bind(end_period)
                    //     .bind(PublicKey::try_from(reward.hotspot_key)?)
                    //     .execute(&pool).await?;
                    println!("hotspot_key {}", reward.beacon_amount)
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
