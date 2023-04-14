use crate::{settings::Settings, LOADER_WORKERS};
use anyhow::{Error, Result};
use chrono::{DateTime, Utc};
use file_store::{FileInfo, FileStore, FileType};
use futures::stream::{self, StreamExt};
use helium_proto::{services::poc_lora::LoraPocV1, Message};
use parquet::{
    data_type::{BoolType, ByteArray, ByteArrayType, Int32Type, Int64Type},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use std::{fs, path::Path, sync::Arc};

const BEACON_MSG_TYPE: &str = "
message schema {
    REQUIRED BYTE_ARRAY poc_id;
    REQUIRED INT64 ingest_time;
    REQUIRED INT64 beacon_location;
    REQUIRED BYTE_ARRAY pub_key;
    REQUIRED INT64 frequency;
    REQUIRED INT32 channel;
    REQUIRED INT32 tx_power;
    REQUIRED INT64 timestamp;
    REQUIRED INT32 tmst;
}";

const WITNESS_MSG_TYPE: &str = "
message schema {
    required byte_array poc_id;
    required byte_array pub_key;
    required int64 ingest_time;
    required int64 witness_location;
    required int64 timestamp;
    required int32 tmst;
    required int32 signal;
    required int32 snr;
    required int64 frequency;
    required boolean selected;
}";

#[derive(Debug)]
pub struct Handler {
    store: FileStore,
    mode: Mode,
    settings: Settings,
}

#[derive(Debug)]
pub enum Mode {
    Historical(DateTime<Utc>, DateTime<Utc>),
    Current(DateTime<Utc>),
}

impl Handler {
    pub async fn new(settings: Settings, mode: Mode) -> Result<Self> {
        let store = FileStore::from_settings(&settings.ingest).await?;
        Ok(Self {
            store,
            mode,
            settings,
        })
    }

    pub async fn run(self) -> Result<()> {
        match self.mode {
            Mode::Current(from_ts) => self.handle_current(from_ts).await,
            Mode::Historical(before_ts, after_ts) => self.handle_history(before_ts, after_ts).await,
        }
    }

    async fn handle_current(&self, _from_ts: DateTime<Utc>) -> Result<()> {
        // TODO
        Ok(())
    }

    async fn handle_history(
        &self,
        before_ts: DateTime<Utc>,
        after_ts: DateTime<Utc>,
    ) -> Result<()> {
        tracing::debug!("before_ts: {:?}", before_ts);
        tracing::debug!("after_ts: {:?}", after_ts);

        let file_list = self
            .store
            .list_all(FileType::IotPoc, after_ts, before_ts)
            .await?;

        let tasks = file_list.into_iter().map(|file_info| async move {
            let stamp = file_info.key.split('.').collect::<Vec<_>>()[1];
            tracing::debug!("parsing iot_poc with timestamp: {:?}", stamp);

            let beacon_file = format!(
                "/{}/valid_beacons.{}.parquet",
                self.settings.output_path, stamp
            );
            let beacon_path = Path::new(&beacon_file);
            let witness_file = format!(
                "/{}/valid_witnesses.{}.parquet",
                self.settings.output_path, stamp
            );
            let witness_path = Path::new(&witness_file);

            write_parquet(self.store.clone(), file_info, beacon_path, witness_path).await?;

            Ok::<(), Error>(())
        });

        let _results: Vec<Result<(), Error>> = stream::iter(tasks)
            .buffer_unordered(LOADER_WORKERS)
            .collect()
            .await;

        Ok(())
    }
}

async fn write_parquet(
    store: FileStore,
    file_info: FileInfo,
    beacon_path: &Path,
    witness_path: &Path,
) -> Result<()> {
    let mut file_stream = store.get(file_info.key.clone()).await?;

    let beacon_schema = Arc::new(parse_message_type(BEACON_MSG_TYPE)?);
    let witness_schema = Arc::new(parse_message_type(WITNESS_MSG_TYPE)?);
    let beacon_props = Arc::new(WriterProperties::builder().build());
    let witness_props = Arc::new(WriterProperties::builder().build());
    let beacon_file = fs::File::create(beacon_path)?;
    let witness_file = fs::File::create(witness_path)?;

    let mut beacon_writer = SerializedFileWriter::new(beacon_file, beacon_schema, beacon_props)?;
    let mut beacon_row_group_writer = beacon_writer.next_row_group()?;

    let mut witness_writer =
        SerializedFileWriter::new(witness_file, witness_schema, witness_props)?;
    let mut witness_row_group_writer = witness_writer.next_row_group()?;

    let mut poc_id: Vec<ByteArray> = Vec::new();
    let mut ingest_time: Vec<i64> = Vec::new();
    let mut beacon_location: Vec<i64> = Vec::new();
    let mut pub_key: Vec<ByteArray> = Vec::new();
    let mut frequency: Vec<i64> = Vec::new();
    let mut channel: Vec<i32> = Vec::new();
    let mut tx_power: Vec<i32> = Vec::new();
    let mut timestamp: Vec<i64> = Vec::new();
    let mut tmst: Vec<i32> = Vec::new();

    let mut witness_poc_id: Vec<ByteArray> = Vec::new();
    let mut witness_pub_key: Vec<ByteArray> = Vec::new();
    let mut witness_ingest_time: Vec<i64> = Vec::new();
    let mut witness_location: Vec<i64> = Vec::new();
    let mut witness_timestamp: Vec<i64> = Vec::new();
    let mut witness_tmst: Vec<i32> = Vec::new();
    let mut witness_signal: Vec<i32> = Vec::new();
    let mut witness_snr: Vec<i32> = Vec::new();
    let mut witness_frequency: Vec<i64> = Vec::new();
    let mut selected: Vec<bool> = Vec::new();

    while let Some(result) = file_stream.next().await {
        let msg = result?;
        let poc = LoraPocV1::decode(msg)?;
        if poc.selected_witnesses.is_empty() {
            continue;
        }
        poc_id.push(ByteArray::from(poc.poc_id.clone()));
        ingest_time.push(poc.beacon_report.clone().unwrap().received_timestamp as i64);
        beacon_location.push(poc.beacon_report.clone().unwrap().location.parse::<i64>()?);
        pub_key.push(ByteArray::from(
            poc.beacon_report.clone().unwrap().report.unwrap().pub_key,
        ));
        frequency.push(poc.beacon_report.clone().unwrap().report.unwrap().frequency as i64);
        channel.push(poc.beacon_report.clone().unwrap().report.unwrap().channel);
        tx_power.push(poc.beacon_report.clone().unwrap().report.unwrap().tx_power);
        timestamp.push(poc.beacon_report.clone().unwrap().report.unwrap().timestamp as i64);
        tmst.push(poc.beacon_report.clone().unwrap().report.unwrap().tmst as i32);
        for witness in poc.selected_witnesses {
            witness_poc_id.push(ByteArray::from(poc.poc_id.clone()));
            witness_pub_key.push(ByteArray::from(witness.report.clone().unwrap().pub_key));
            witness_ingest_time.push(witness.received_timestamp as i64);
            witness_location.push(witness.location.parse::<i64>().unwrap_or(0));
            witness_timestamp.push(witness.report.clone().unwrap().timestamp as i64);
            witness_tmst.push(witness.report.clone().unwrap().tmst as i32);
            witness_signal.push(witness.report.clone().unwrap().signal);
            witness_snr.push(witness.report.clone().unwrap().snr);
            witness_frequency.push(witness.report.clone().unwrap().frequency as i64);
            selected.push(true);
        }
        for witness in poc.unselected_witnesses {
            witness_poc_id.push(ByteArray::from(poc.poc_id.clone()));
            witness_pub_key.push(ByteArray::from(witness.report.clone().unwrap().pub_key));
            witness_ingest_time.push(witness.received_timestamp as i64);
            witness_location.push(witness.location.parse::<i64>().unwrap_or(0));
            witness_timestamp.push(witness.report.clone().unwrap().timestamp as i64);
            witness_tmst.push(witness.report.clone().unwrap().tmst as i32);
            witness_signal.push(witness.report.clone().unwrap().signal);
            witness_snr.push(witness.report.clone().unwrap().snr);
            witness_frequency.push(witness.report.clone().unwrap().frequency as i64);
            selected.push(false);
        }
    }

    let mut col_number = 0;
    while let Some(mut col_writer) = beacon_row_group_writer.next_column()? {
        col_number += 1;

        match col_number {
            1 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(poc_id.as_slice(), None, None)?;
            }
            2 => {
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(ingest_time.as_slice(), None, None)?;
            }
            3 => {
                col_writer.typed::<Int64Type>().write_batch(
                    beacon_location.as_slice(),
                    None,
                    None,
                )?;
            }
            4 => {
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(pub_key.as_slice(), None, None)?;
            }
            5 => {
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(frequency.as_slice(), None, None)?;
            }
            6 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(channel.as_slice(), None, None)?;
            }
            7 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(tx_power.as_slice(), None, None)?;
            }
            8 => {
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(timestamp.as_slice(), None, None)?;
            }
            9 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(tmst.as_slice(), None, None)?;
            }
            _e => tracing::warn!("no column match {:?}", _e),
        }
        col_writer.close()?;
    }

    beacon_row_group_writer.close()?;
    beacon_writer.close()?;

    let mut col_number = 0;
    while let Some(mut col_writer) = witness_row_group_writer.next_column()? {
        col_number += 1;

        match col_number {
            1 => {
                col_writer.typed::<ByteArrayType>().write_batch(
                    witness_poc_id.as_slice(),
                    None,
                    None,
                )?;
            }
            2 => {
                col_writer.typed::<ByteArrayType>().write_batch(
                    witness_pub_key.as_slice(),
                    None,
                    None,
                )?;
            }
            3 => {
                col_writer.typed::<Int64Type>().write_batch(
                    witness_ingest_time.as_slice(),
                    None,
                    None,
                )?;
            }
            4 => {
                col_writer.typed::<Int64Type>().write_batch(
                    witness_location.as_slice(),
                    None,
                    None,
                )?;
            }
            5 => {
                col_writer.typed::<Int64Type>().write_batch(
                    witness_timestamp.as_slice(),
                    None,
                    None,
                )?;
            }
            6 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(witness_tmst.as_slice(), None, None)?;
            }
            7 => {
                col_writer.typed::<Int32Type>().write_batch(
                    witness_signal.as_slice(),
                    None,
                    None,
                )?;
            }
            8 => {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(witness_snr.as_slice(), None, None)?;
            }
            9 => {
                col_writer.typed::<Int64Type>().write_batch(
                    witness_frequency.as_slice(),
                    None,
                    None,
                )?;
            }
            10 => {
                col_writer
                    .typed::<BoolType>()
                    .write_batch(selected.as_slice(), None, None)?;
            }
            _e => tracing::warn!("no column match {:?}", _e),
        }
        col_writer.close()?;
    }

    witness_row_group_writer.close()?;
    witness_writer.close()?;
    Ok(())
}
