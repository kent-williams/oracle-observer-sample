use crate::{settings::Settings, LOADER_WORKERS};
use anyhow::{bail, Error, Result};
use aws_config::meta::region::RegionProviderChain;
#[cfg(feature = "local")]
use aws_sdk_s3::Credentials;
use aws_sdk_s3::{types::ByteStream, Client, Endpoint, Region};
use chrono::{DateTime, Utc};
use file_store::{BytesMutStream, FileStore, FileType, Settings as FSettings};
use futures::stream::{self, StreamExt};
use helium_proto::{services::poc_lora::LoraPocV1, Message};
use http::Uri;
use lambda_runtime::LambdaEvent;
use parquet::{
    data_type::{BoolType, ByteArray, ByteArrayType, Int32Type, Int64Type},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use serde_json::Value;
use std::{fs, path::Path, str::FromStr, sync::Arc};

#[derive(thiserror::Error, Debug)]
pub enum DecodeError {
    #[error("uri error")]
    Uri(#[from] http::uri::InvalidUri),
}

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

#[derive(Debug, Clone)]
pub struct Handler {
    store: FileStore,
    mode: Mode,
    settings: Settings,
    client: Client,
}

#[derive(Debug, Clone)]
pub enum Mode {
    Historical(DateTime<Utc>, DateTime<Utc>),
    Current(DateTime<Utc>),
}

impl Handler {
    pub async fn new(settings: Settings, mode: Mode) -> Result<Self> {
        let store = FileStore::from_settings(&settings.ingest).await?;

        let endpoint: Option<Endpoint> = match &settings.output_endpoint {
            Some(endpoint) => Uri::from_str(endpoint)
                .map(Endpoint::immutable)
                .map(Some)
                .map_err(DecodeError::from)?,
            _ => None,
        };
        let region = Region::new(settings.output_region.clone());
        let region_provider = RegionProviderChain::first_try(region).or_default_provider();

        let mut config = aws_config::from_env().region(region_provider);
        if let Some(endpoint) = endpoint {
            config = config.endpoint_resolver(endpoint);
        }

        #[cfg(feature = "local")]
        if settings.output_access_key_id.is_some() && settings.output_secret_access_key.is_some() {
            let creds = Credentials::new(
                settings.output_access_key_id.clone().unwrap(),
                settings.output_secret_access_key.clone().unwrap(),
                settings.output_session_token.clone(),
                None,
                "local",
            );
            config = config.credentials_provider(creds);
        }

        let config = config.load().await;

        let client = Client::new(&config);

        Ok(Self {
            store,
            mode,
            settings,
            client,
        })
    }

    pub async fn run(&self, event: Option<LambdaEvent<Value>>) -> Result<()> {
        match self.mode {
            Mode::Current(from_ts) => {
                if let Some(e) = event {
                    self.handle_current(from_ts, e).await?;
                } else {
                    bail!("lambda event not provided")
                }
            }
            Mode::Historical(before_ts, after_ts) => {
                self.handle_history(before_ts, after_ts).await?
            }
        }
        Ok(())
    }

    async fn handle_current(
        &self,
        from_ts: DateTime<Utc>,
        event: LambdaEvent<Value>,
    ) -> Result<()> {
        tracing::debug!("from_ts: {:?}", from_ts);

        // Get the actual event
        let (event, _context) = event.into_parts();
        tracing::debug!("event: {:?}", event);

        // Bail if there are no records
        if event["Records"].is_null() {
            bail!("Event records are unexpectedly null.");
        }

        let record = &event["Records"][0];
        tracing::debug!("record: {:?}", record);
        let bucket = record["s3"]["bucket"]["name"].clone().to_string();
        tracing::debug!("bucket: {:?}", bucket);
        let key = record["s3"]["object"]["key"].clone().to_string();
        tracing::debug!("key: {:?}", key);
        let region = record["awsRegion"].clone().to_string();
        tracing::debug!("region: {:?}", region);

        let settings = &FSettings {
            region,
            bucket,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
        };

        let prefix = key.split('.').next().unwrap_or("");
        tracing::debug!("prefix: {:?}", prefix);
        let file_type = FileType::from_str(prefix)?;
        tracing::debug!("file_type: {:?}", file_type);

        if file_type == FileType::IotPoc {
            let stamp = key.split('.').collect::<Vec<_>>()[1];
            tracing::debug!("stamp: {:?}", stamp);
            let beacon_file = format!("/{}/valid_beacons.{}.parquet", self.settings.cache, stamp);
            let beacon_path = Path::new(&beacon_file);
            let witness_file =
                format!("/{}/valid_witnesses.{}.parquet", self.settings.cache, stamp);
            let witness_path = Path::new(&witness_file);

            let store = FileStore::from_settings(settings).await?;
            let mut file_stream = store.get(key).await?;

            self.write_parquet(&mut file_stream, beacon_path, witness_path)
                .await?;
            tracing::debug!("successfully wrote {:?}", beacon_path);
            tracing::debug!("successfully wrote {:?}", witness_path);

            self.upload_parquet("valid_beacon", beacon_path).await?;
            self.upload_parquet("valid_witness", witness_path).await?;

            self.cleanup_parquet_cache(beacon_path, witness_path)
                .await?;
        }

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

            let beacon_file = format!("/{}/valid_beacons.{}.parquet", self.settings.cache, stamp);
            let beacon_path = Path::new(&beacon_file);
            let witness_file =
                format!("/{}/valid_witnesses.{}.parquet", self.settings.cache, stamp);
            let witness_path = Path::new(&witness_file);

            let mut file_stream = self.store.get(file_info.key.clone()).await?;

            self.write_parquet(&mut file_stream, beacon_path, witness_path)
                .await?;
            tracing::debug!("successfully wrote {:?}", beacon_path);
            tracing::debug!("successfully wrote {:?}", witness_path);

            self.upload_parquet("valid_beacon", beacon_path).await?;
            self.upload_parquet("valid_witness", witness_path).await?;

            self.cleanup_parquet_cache(beacon_path, witness_path)
                .await?;

            Ok::<(), Error>(())
        });

        let _results: Vec<Result<(), Error>> = stream::iter(tasks)
            .buffer_unordered(LOADER_WORKERS)
            .collect()
            .await;

        Ok(())
    }

    async fn cleanup_parquet_cache(&self, beacon_path: &Path, witness_path: &Path) -> Result<()> {
        fs::remove_file(beacon_path)?;
        tracing::debug!("successfully removed tmp {:?}", beacon_path);
        fs::remove_file(witness_path)?;
        tracing::debug!("successfully removed tmp {:?}", witness_path);
        Ok(())
    }

    async fn upload_parquet(&self, folder_name: &str, file_path: &Path) -> Result<()> {
        if let Some(key) = file_path.file_name() {
            let body = ByteStream::from_path(file_path).await?;

            if let Some(key_str) = key.to_str() {
                let keyname = format!("{}/{}", folder_name, key_str);

                self.client
                    .put_object()
                    .bucket(self.settings.output_bucket.clone())
                    .body(body)
                    .key(&keyname)
                    .content_type("text/plain")
                    .send()
                    .await?;

                tracing::debug!("successfully stored {} in s3 {}", key_str, keyname);
            }
        }

        Ok(())
    }

    async fn write_parquet(
        &self,
        file_stream: &mut BytesMutStream,
        beacon_path: &Path,
        witness_path: &Path,
    ) -> Result<()> {
        let beacon_schema = Arc::new(parse_message_type(BEACON_MSG_TYPE)?);
        let witness_schema = Arc::new(parse_message_type(WITNESS_MSG_TYPE)?);
        let beacon_props = Arc::new(WriterProperties::builder().build());
        let witness_props = Arc::new(WriterProperties::builder().build());
        let beacon_file = fs::File::create(beacon_path)?;
        let witness_file = fs::File::create(witness_path)?;

        let mut beacon_writer =
            SerializedFileWriter::new(beacon_file, beacon_schema, beacon_props)?;
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
                    col_writer.typed::<ByteArrayType>().write_batch(
                        poc_id.as_slice(),
                        None,
                        None,
                    )?;
                }
                2 => {
                    col_writer.typed::<Int64Type>().write_batch(
                        ingest_time.as_slice(),
                        None,
                        None,
                    )?;
                }
                3 => {
                    col_writer.typed::<Int64Type>().write_batch(
                        beacon_location.as_slice(),
                        None,
                        None,
                    )?;
                }
                4 => {
                    col_writer.typed::<ByteArrayType>().write_batch(
                        pub_key.as_slice(),
                        None,
                        None,
                    )?;
                }
                5 => {
                    col_writer.typed::<Int64Type>().write_batch(
                        frequency.as_slice(),
                        None,
                        None,
                    )?;
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
                    col_writer.typed::<Int64Type>().write_batch(
                        timestamp.as_slice(),
                        None,
                        None,
                    )?;
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
                    col_writer.typed::<Int32Type>().write_batch(
                        witness_tmst.as_slice(),
                        None,
                        None,
                    )?;
                }
                7 => {
                    col_writer.typed::<Int32Type>().write_batch(
                        witness_signal.as_slice(),
                        None,
                        None,
                    )?;
                }
                8 => {
                    col_writer.typed::<Int32Type>().write_batch(
                        witness_snr.as_slice(),
                        None,
                        None,
                    )?;
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
}
