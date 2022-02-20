A simple library to send rows to clickhouse in RowBinary format

## Usage
```sql
create table if not exists drops
(
    id          UUID default generateUUIDv4(),
    character   String,
    timestamp   DateTime64(3, 'UTC'),
    item_id     Nullable(UInt32),
    currency_id Nullable(UInt8),
    count       Int32,
    mf          UInt16
)
    engine = MergeTree ORDER BY (character, timestamp, id)
        SETTINGS index_granularity = 8192;
```
```rs
use chrono::NaiveDateTime;
use futures::SinkExt;
use simple_clickhouse::{Client, RowBinary};
#[derive(RowBinary)]
pub struct ClickhouseSnap {
    character:   String,
    timestamp:   NaiveDateTime,
    item_id:     Option<u32>,
    currency_id: Option<u8>,
    count:       i32,
    mf:          u16,
}

async fn send() {
    let client = Client::new("http://localhost:8123".to_string());
    let mut inserter = client.inserter("drops");
    let snap = ClickhouseSnap {
        character:   String::from("test"),
        timestamp:   NaiveDateTime::from_timestamp(16000000, 0),
        item_id:     None,
        currency_id: Some(80),
        count:       -150,
        mf:          300,
    };
    inserter.send(snap).await.ok();
}
```
This library is capable of inserting partial rows as long as the missing columns have a default set.

Not many types are implemented currently, neither is anything gated behind features, but feel free to create a PR.
