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
use std::{env, str::FromStr};

// new
use aws_sdk_s3 as s3;
use aws_sdk_s3::types::ByteStream;
use parquet::{
    data_type::{AsBytes, ByteArray, ByteArrayType, Int32Type, Int64Type},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path, sync::Arc};

#[derive(Debug, Serialize)]
struct FailureResponse {
    pub body: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();
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

    let path = Path::new("/tmp/gateway_reward_share_1671643842138.parquet");

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
                if let chrono::LocalResult::Single(end_period) = end_period {
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

    let bucket_name = "oracle-parquet-test";
    let config = aws_config::load_from_env().await;
    let s3_client = s3::Client::new(&config);

    let filename = format!("parquet_output_test/test-output.txt");
    let body =
        ByteStream::from_path(Path::new("/tmp/gateway_reward_share_1671643842138.parquet")).await;

    let _ = s3_client
        .put_object()
        .bucket(bucket_name)
        .body(body.unwrap()) 
        .key(&filename)
        .content_type("text/plain")
        .send()
        .await
        .map_err(|err| {
            // In case of failure, log a detailed error to CloudWatch.
            println!(
                "failed to upload file '{}' to S3 with error: {}",
                &filename, err
            );
            //The sender of the request receives this message in response.
            FailureResponse {
                body: "The lambda encountered an error and your message was not saved".to_owned(),
            }
        });

    println!(
        "Successfully stored the incoming request in S3 with the name '{}'",
        &filename
    );

    let message = format!("{count} rows of {prefix} processed.");
    println!("{}", message);
    Ok(json!({ "message": message }))
}
