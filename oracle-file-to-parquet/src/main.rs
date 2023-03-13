use anyhow::anyhow;
use chrono::{TimeZone, Utc};
use file_store::{FileInfo, FileInfoStream, FileStore, FileType};
use futures::StreamExt;
use helium_proto::{
    services::{poc_lora::GatewayRewardShare},
    Message,
};
use lambda_runtime::{Error};
use serde_json::{json, Value};
use std::{str::FromStr};

use parquet::{
    data_type::{ByteArray, ByteArrayType, Int64Type},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use std::{fs, path::Path, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Error> {
    gateway_reward_share_handler().await?;
    Ok(())
}

fn infos(names: &'static [&str]) -> FileInfoStream {
    futures::stream::iter(names.iter().map(|v| FileInfo::from_str(v))).boxed()
}

async fn gateway_reward_share_handler() -> Result<Value, Error> {
    let path = Path::new("gateway_reward_share_1671643842138.parquet");

    let message_type = "
    message schema {
        REQUIRED BYTE_ARRAY hotspot_key (UTF8);
        REQUIRED INT64 beacon_amount;
        REQUIRED INT64 witness_amount;
        REQUIRED INT64 start_period;
        REQUIRED INT64 end_period;
    }
    ";

    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();

    let file_store = FileStore::new(None, "us-west-2", "mainnet-pociot-verified")
        .await
        .expect("file store");

    let key = "gateway_reward_share.1671643842138.gz";
    let prefix = key.split('.').next().unwrap_or("");
    let file_type = FileType::from_str(prefix)?;
    let mut file_stream = file_store.source(infos(&["gateway_reward_share.1671643842138.gz"]));

    println!("key is {}", key);

    let mut hotspot_key_vec: Vec<ByteArray> = Vec::new();
    let mut beacon_amount_vec: Vec<i64> = Vec::new();
    let mut witness_amount_vec: Vec<i64> = Vec::new();
    let mut start_period_vec: Vec<i64> = Vec::new();
    let mut end_period_vec: Vec<i64> = Vec::new();

    let mut count = 0;
    while let Some(result) = file_stream.next().await {
        let msg = result?;
        count += 1;
        match file_type {
            FileType::GatewayRewardShare => {
                let reward = GatewayRewardShare::decode(msg)?;
                let end_period = Utc.timestamp_opt(reward.end_period as i64, 0);
                if let chrono::LocalResult::Single(_end_period) = end_period {
                    hotspot_key_vec.push(ByteArray::from(reward.hotspot_key.clone()));
                    beacon_amount_vec.push(reward.beacon_amount.clone() as i64);
                    witness_amount_vec.push(reward.witness_amount.clone() as i64);
                    start_period_vec.push(reward.start_period.clone() as i64);
                    end_period_vec.push(reward.end_period.clone() as i64);
                } else {
                    return Err(anyhow!("Unexpected end_epoch: {end_period:?}").into());
                }
            }
            _ => (),
        }
    }

    let mut col_number = 0;
    while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        col_number = col_number + 1;

        match col_number {
            1 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(hotspot_key_vec.as_slice(), None, None)
                    .unwrap();
            }
            2 => {
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(beacon_amount_vec.as_slice(), None, None)
                    .unwrap();
            }
            3 => {
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(witness_amount_vec.as_slice(), None, None)
                    .unwrap();
            }
            4 => {
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(start_period_vec.as_slice(), None, None)
                    .unwrap();
            }
            5 => {
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(end_period_vec.as_slice(), None, None)
                    .unwrap();
            }
            _ => println!("no column match"),
        }
        col_writer.close().unwrap();
    }

    row_group_writer.close().unwrap();
    writer.close().unwrap();

    let message = format!("{count} rows of {prefix} processed.");
    Ok(json!({ "message": message }))
}
