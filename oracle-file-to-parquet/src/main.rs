use anyhow::anyhow;
use chrono::{TimeZone, Utc};
use file_store::{FileInfo, FileInfoStream, FileStore, FileType};
use futures::StreamExt;
use helium_proto::{
    services::{poc_lora::{GatewayRewardShare, LoraBeaconIngestReportV1, LoraWitnessIngestReportV1, LoraPocV1}},
    Message,
};
use lambda_runtime::{Error};
use serde_json::{json, Value};
use std::{str::FromStr};

use parquet::{
    data_type::{ByteArray, ByteArrayType, Int64Type, Int32Type},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use std::{fs, path::Path, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // gateway_reward_share_handler().await?;
    // lora_beacon_ingest_report_v1_handler().await?;
    // lora_witness_ingest_report_v1_handler().await?;
    lora_valid_beacon_report_v1().await?;
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

async fn lora_beacon_ingest_report_v1_handler() -> Result<Value, Error> {
    let path = Path::new("iot_beacon_ingest_report.1674760102047.parquet");

    let message_type = "
    message schema {
        REQUIRED INT64 ingest_timestamp;
        REQUIRED BYTE_ARRAY pub_key (UTF8);
        REQUIRED BYTE_ARRAY local_entropy (UTF8);
        REQUIRED BYTE_ARRAY remote_entropy (UTF8);
        REQUIRED BYTE_ARRAY data (UTF8);

        REQUIRED INT64 frequency;
        REQUIRED INT32 channel;
        REQUIRED INT32 datarate;

        REQUIRED INT32 tx_power;
        REQUIRED INT64 hotspot_timestamp;
        REQUIRED BYTE_ARRAY signature (UTF8);
        REQUIRED INT32 tmst;
    }
    ";

    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();

    let file_store = FileStore::new(None, "us-west-2", "oracle-ingest-lamda-test")
        .await
        .expect("file store");

    let key = "iot_beacon_ingest_report.1674760102047.gz";
    let prefix = key.split('.').next().unwrap_or("");
    let file_type = FileType::from_str(prefix)?;
    let mut file_stream = file_store.source(infos(&["iot_beacon_ingest_report.1674760102047.gz"]));

    println!("key is {}", key);

    let mut ingest_timestamp_vec: Vec<i64> = Vec::new();

    let mut pub_key_vec: Vec<ByteArray> = Vec::new();
    let mut local_entropy_vec: Vec<ByteArray> = Vec::new();
    let mut remote_entropy_vec: Vec<ByteArray> = Vec::new();
    let mut data_vec: Vec<ByteArray> = Vec::new();

    let mut frequency_vec: Vec<i64> = Vec::new();
    let mut channel_vec: Vec<i32> = Vec::new();
    let mut datarate_vec: Vec<i32> = Vec::new();
    let mut tx_power_vec: Vec<i32> = Vec::new();

    let mut hotspot_timestamp_vec: Vec<i64> = Vec::new();
    
    let mut signature_vec: Vec<ByteArray> = Vec::new();
    let mut tmst_vec: Vec<i32> = Vec::new();

    let mut count = 0;
    while let Some(result) = file_stream.next().await {
        let msg = result?;
        count += 1;
        match file_type {
            FileType::IotBeaconIngestReport => {
                let beacon_ingest_report = LoraBeaconIngestReportV1::decode(msg)?;

                    // println!("beacon_ingest_report.timestamp: {:?}", beacon_ingest_report.received_timestamp);
                    ingest_timestamp_vec.push(beacon_ingest_report.received_timestamp as i64);
                    let report = beacon_ingest_report.report.unwrap();

                    pub_key_vec.push(ByteArray::from(report.pub_key));
                    local_entropy_vec.push(ByteArray::from(report.local_entropy));
                    remote_entropy_vec.push(ByteArray::from(report.remote_entropy));
                    data_vec.push(ByteArray::from(report.data));

                    frequency_vec.push(report.frequency as i64);
                    channel_vec.push(report.channel as i32);
                    datarate_vec.push(report.datarate as i32);
                    tx_power_vec.push(report.tx_power as i32);

                    hotspot_timestamp_vec.push(report.timestamp as i64);

                    signature_vec.push(ByteArray::from(report.signature));
                    tmst_vec.push(report.tmst as i32);
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
                    .typed::<Int64Type>()
                    .write_batch(ingest_timestamp_vec.as_slice(), None, None)
                    .unwrap();
            }
            2 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(pub_key_vec.as_slice(), None, None)
                    .unwrap();
            }
            3 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(local_entropy_vec.as_slice(), None, None)
                    .unwrap();
            }
            4 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(remote_entropy_vec.as_slice(), None, None)
                    .unwrap();
            }
            5 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(data_vec.as_slice(), None, None)
                    .unwrap();
            }
            6 => {
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(frequency_vec.as_slice(), None, None)
                    .unwrap();
            }
            7 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(channel_vec.as_slice(), None, None)
                    .unwrap();
            }
            8 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(datarate_vec.as_slice(), None, None)
                    .unwrap();
            }
            9 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(tx_power_vec.as_slice(), None, None)
                    .unwrap();
            }
            10 => {
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(hotspot_timestamp_vec.as_slice(), None, None)
                    .unwrap();
            }
            11 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(signature_vec.as_slice(), None, None)
                    .unwrap();
            }
            12 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(tmst_vec.as_slice(), None, None)
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

async fn lora_witness_ingest_report_v1_handler() -> Result<Value, Error> {
    let path = Path::new("iot_witness_ingest_report.1674760087890.parquet");

    let message_type = "
    message schema {
        REQUIRED INT64 ingest_timestamp;

        REQUIRED BYTE_ARRAY pub_key (UTF8);
        REQUIRED BYTE_ARRAY data (UTF8);

        REQUIRED INT64 hotspot_timestamp;
        REQUIRED INT32 tmst;

        REQUIRED INT32 signal;
        REQUIRED INT32 snr;

        REQUIRED INT64 frequency;
        REQUIRED INT32 datarate;
        REQUIRED BYTE_ARRAY signature (UTF8);

    }
    ";

    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();

    let file_store = FileStore::new(None, "us-west-2", "oracle-ingest-lamda-test")
        .await
        .expect("file store");

    let key = "iot_witness_ingest_report.1674760087890.gz";
    let prefix = key.split('.').next().unwrap_or("");
    let file_type = FileType::from_str(prefix)?;
    let mut file_stream = file_store.source(infos(&["iot_witness_ingest_report.1674760087890.gz"]));

    println!("key is {}", key);

    let mut ingest_timestamp_vec: Vec<i64> = Vec::new();

    let mut pub_key_vec: Vec<ByteArray> = Vec::new();
    let mut data_vec: Vec<ByteArray> = Vec::new();

    let mut hotspot_timestamp_vec: Vec<i64> = Vec::new();
    let mut tmst_vec: Vec<i32> = Vec::new();

    let mut signal_vec: Vec<i32> = Vec::new();
    let mut snr_vec: Vec<i32> = Vec::new();
    
    let mut frequency_vec: Vec<i64> = Vec::new();
    let mut datarate_vec: Vec<i32> = Vec::new();
    
    let mut signature_vec: Vec<ByteArray> = Vec::new();

    let mut count = 0;
    while let Some(result) = file_stream.next().await {
        let msg = result?;
        count += 1;
        match file_type {
            FileType::IotWitnessIngestReport => {
                let beacon_ingest_report = LoraWitnessIngestReportV1::decode(msg)?;

                    // println!("beacon_ingest_report.timestamp: {:?}", beacon_ingest_report.received_timestamp);
                    ingest_timestamp_vec.push(beacon_ingest_report.received_timestamp as i64);
                    let report = beacon_ingest_report.report.unwrap();

                    pub_key_vec.push(ByteArray::from(report.pub_key));
                    data_vec.push(ByteArray::from(report.data));

                    hotspot_timestamp_vec.push(report.timestamp as i64);
                    tmst_vec.push(report.snr as i32);

                    signal_vec.push(report.signal as i32);
                    snr_vec.push(report.snr as i32);
                    
                    frequency_vec.push(report.frequency as i64);
                    datarate_vec.push(report.datarate as i32);

                    signature_vec.push(ByteArray::from(report.signature));
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
                    .typed::<Int64Type>()
                    .write_batch(ingest_timestamp_vec.as_slice(), None, None)
                    .unwrap();
            }
            2 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(pub_key_vec.as_slice(), None, None)
                    .unwrap();
            }
            3 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(data_vec.as_slice(), None, None)
                    .unwrap();
            }
            4 => {
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(hotspot_timestamp_vec.as_slice(), None, None)
                    .unwrap();
            }
            5 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(tmst_vec.as_slice(), None, None)
                    .unwrap();
            }
            6 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(signal_vec.as_slice(), None, None)
                    .unwrap();
            }
            7 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(snr_vec.as_slice(), None, None)
                    .unwrap();
            }
            8 => {
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(frequency_vec.as_slice(), None, None)
                    .unwrap();
            }
            9 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(datarate_vec.as_slice(), None, None)
                    .unwrap();
            }
            10 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(signature_vec.as_slice(), None, None)
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

async fn lora_valid_beacon_report_v1() -> Result<Value, Error> {
    let path = Path::new("iot_poc.1674753963119.parquet");

    let message_type = "
    message schema {
        REQUIRED INT64 received_timestamp;
        REQUIRED BYTE_ARRAY location (UTF8);
        REQUIRED INT32 hex_scale;

        REQUIRED BYTE_ARRAY pub_key (UTF8);
        REQUIRED BYTE_ARRAY local_entropy (UTF8);
        REQUIRED BYTE_ARRAY remote_entropy (UTF8);
        REQUIRED BYTE_ARRAY data (UTF8);
        REQUIRED INT64 frequency;
        REQUIRED INT32 channel;
        REQUIRED INT32 datarate;
        REQUIRED INT32 tx_power;
        REQUIRED INT64 hotspot_timestamp;
        REQUIRED BYTE_ARRAY signature (UTF8);
        REQUIRED INT32 tmst;

        REQUIRED INT32 reward_unit;
    }
    ";

    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();

    let file_store = FileStore::new(None, "us-west-2", "oracle-ingest-lamda-test")
        .await
        .expect("file store");

    let key = "iot_poc.1674753963119.gz";
    let prefix = key.split('.').next().unwrap_or("");
    let file_type = FileType::from_str(prefix)?;
    let mut file_stream = file_store.source(infos(&["iot_poc.1674753963119.gz"]));

    println!("key is {}", key);

    let mut ingest_timestamp_vec: Vec<i64> = Vec::new();
    let mut location_vec: Vec<ByteArray> = Vec::new();
    let mut hex_scale_vec: Vec<i32> = Vec::new();

    let mut pub_key_vec: Vec<ByteArray> = Vec::new();
    let mut local_entropy_vec: Vec<ByteArray> = Vec::new();
    let mut remote_entropy_vec: Vec<ByteArray> = Vec::new();
    let mut data_vec: Vec<ByteArray> = Vec::new();
    let mut frequency_vec: Vec<i64> = Vec::new();
    let mut channel_vec: Vec<i32> = Vec::new();
    let mut datarate_vec: Vec<i32> = Vec::new();
    let mut tx_power_vec: Vec<i32> = Vec::new();
    let mut hotspot_timestamp_vec: Vec<i64> = Vec::new();
    let mut signature_vec: Vec<ByteArray> = Vec::new();
    let mut tmst_vec: Vec<i32> = Vec::new();

    let mut reward_unit_vec: Vec<i32> = Vec::new();

    let mut count = 0;
    while let Some(result) = file_stream.next().await {
        let msg = result?;
        count += 1;
        match file_type {
            FileType::IotPoc => {
                let lora_poc = LoraPocV1::decode(msg)?;

                let valid_beacon_report = lora_poc.beacon_report.unwrap();


                ingest_timestamp_vec.push(valid_beacon_report.received_timestamp as i64);
                location_vec.push(ByteArray::from(valid_beacon_report.location.clone().into_bytes()));
                // println!("{}", valid_beacon_report.location.parse::<u64>().unwrap());
                hex_scale_vec.push(valid_beacon_report.hex_scale as i32);

                let report = valid_beacon_report.report.unwrap();

                pub_key_vec.push(ByteArray::from(report.pub_key));
                local_entropy_vec.push(ByteArray::from(report.local_entropy));
                remote_entropy_vec.push(ByteArray::from(report.remote_entropy));
                data_vec.push(ByteArray::from(report.data));
                frequency_vec.push(report.frequency as i64);
                channel_vec.push(report.channel as i32);
                datarate_vec.push(report.datarate as i32);
                tx_power_vec.push(report.tx_power as i32);
                hotspot_timestamp_vec.push(report.timestamp as i64);
                signature_vec.push(ByteArray::from(report.signature));
                tmst_vec.push(report.tmst as i32);

                reward_unit_vec.push(valid_beacon_report.reward_unit as i32);
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
                    .typed::<Int64Type>()
                    .write_batch(ingest_timestamp_vec.as_slice(), None, None)
                    .unwrap();
            }
            2 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(location_vec.as_slice(), None, None)
                    .unwrap();
            }
            3 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(hex_scale_vec.as_slice(), None, None)
                    .unwrap();
            }
            4 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(pub_key_vec.as_slice(), None, None)
                    .unwrap();
            }
            5 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(local_entropy_vec.as_slice(), None, None)
                    .unwrap();
            }
            6 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(remote_entropy_vec.as_slice(), None, None)
                    .unwrap();
            }
            7 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(data_vec.as_slice(), None, None)
                    .unwrap();
            }
            8 => {
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(frequency_vec.as_slice(), None, None)
                    .unwrap();
            }
            9 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(channel_vec.as_slice(), None, None)
                    .unwrap();
            }
            10 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(datarate_vec.as_slice(), None, None)
                    .unwrap();
            }
            11 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(tx_power_vec.as_slice(), None, None)
                    .unwrap();
            }
            12 => {
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(hotspot_timestamp_vec.as_slice(), None, None)
                    .unwrap();
            }
            13 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(signature_vec.as_slice(), None, None)
                    .unwrap();
            }
            14 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(tmst_vec.as_slice(), None, None)
                    .unwrap();
            }
            15 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(reward_unit_vec.as_slice(), None, None)
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