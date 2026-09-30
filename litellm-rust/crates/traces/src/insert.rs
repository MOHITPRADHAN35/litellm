use std::{collections::BTreeMap, io::Write};

use flate2::{Compression, write::GzEncoder};
use litellm_http::Client;
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{Connection, Error};

pub enum InsertTable {
    OtelTraces,
    SpendLogs,
}

impl InsertTable {
    pub fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "otel_traces" => Ok(Self::OtelTraces),
            "spend_logs" => Ok(Self::SpendLogs),
            _ => Err(Error::InvalidTable),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::OtelTraces => "otel_traces",
            Self::SpendLogs => "spend_logs",
        }
    }
}

pub async fn insert_rows(
    client: &Client,
    connection: &Connection,
    database: &str,
    table: InsertTable,
    rows: Vec<BTreeMap<String, Value>>,
) -> Result<(), Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(encode_rows(rows)?.as_bytes())
        .map_err(|_| Error::InvalidRow)?;
    let body = encoder.finish().map_err(|_| Error::InvalidRow)?;
    let mut url = connection.url().clone();
    url.query_pairs_mut()
        .append_pair(
            "query",
            &format!(
                "INSERT INTO `{database}`.{} FORMAT JSONEachRow",
                table.name()
            ),
        )
        .append_pair("async_insert", "1")
        .append_pair("wait_for_async_insert", "1")
        .append_pair("input_format_skip_unknown_fields", "1")
        .append_pair("date_time_input_format", "best_effort");
    let response = client
        .post(url)
        .header("Content-Encoding", "gzip")
        .body(body)
        .send()
        .await
        .map_err(|_| Error::Transport)?;
    if !response.status().is_success() {
        return Err(Error::InsertFailed(response.status().as_u16()));
    }
    Ok(())
}

pub fn encode_rows(rows: Vec<BTreeMap<String, Value>>) -> Result<String, Error> {
    rows.into_iter()
        .map(|row| {
            let encoded = row
                .into_iter()
                .map(|(name, value)| insert_value(&name, value).map(|value| (name, value)))
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            serde_json::to_string(&encoded).map_err(|_| Error::InvalidRow)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|rows| rows.join("\n"))
}

fn insert_value(name: &str, value: Value) -> Result<Value, Error> {
    let multiplier = match name {
        "Timestamp" => 1,
        "start_time" | "end_time" | "completion_start_time" => 1_000_000,
        _ => return Ok(value),
    };
    if name == "completion_start_time" && value.is_null() {
        return Ok(value);
    }
    let timestamp = value.as_i64().ok_or(Error::InvalidRow)?;
    let datetime = OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp) * multiplier)
        .map_err(|_| Error::InvalidRow)?;
    datetime
        .format(&Rfc3339)
        .map(Value::String)
        .map_err(|_| Error::InvalidRow)
}
