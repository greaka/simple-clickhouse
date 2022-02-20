//! A simple library to send rows to clickhouse in RowBinary format
//!
//! ## Usage
//! ```sql
//! create table if not exists drops
//! (
//!     id          UUID default generateUUIDv4(),
//!     character   String,
//!     timestamp   DateTime64(3, 'UTC'),
//!     item_id     Nullable(UInt32),
//!     currency_id Nullable(UInt8),
//!     count       Int32,
//!     mf          UInt16
//! )
//!     engine = MergeTree ORDER BY (character, timestamp, id)
//!         SETTINGS index_granularity = 8192;
//! ```
//! ```
//! use chrono::NaiveDateTime;
//! use futures::SinkExt;
//! use simple_clickhouse::{Client, RowBinary};
//! #[derive(RowBinary)]
//! pub struct ClickhouseSnap {
//!     character:   String,
//!     timestamp:   NaiveDateTime,
//!     item_id:     Option<u32>,
//!     currency_id: Option<u8>,
//!     count:       i32,
//!     mf:          u16,
//! }
//!
//! async fn send() {
//!     let client = Client::new("http://localhost:8123".to_string());
//!     let mut inserter = client.inserter("drops");
//!     let snap = ClickhouseSnap {
//!         character:   String::from("test"),
//!         timestamp:   NaiveDateTime::from_timestamp(16000000, 0),
//!         item_id:     None,
//!         currency_id: Some(80),
//!         count:       -150,
//!         mf:          300,
//!     };
//!     inserter.send(snap).await.ok();
//! }
//! ```
//! This library is capable of inserting partial rows as long as the missing
//! columns have a default set.

use std::time::Duration;

use futures::{channel::mpsc::UnboundedSender, stream::iter, StreamExt};
use hyper::{client::HttpConnector, header::HeaderValue, Body, Request};
pub use simple_clickhouse_codegen::RowBinary;
use tokio::{pin, time::interval};

#[derive(Debug)]
pub struct Client {
    client:   hyper::Client<HttpConnector>,
    host:     String,
    user:     Option<HeaderValue>,
    password: Option<HeaderValue>,
}

impl Client {
    pub fn new(host: String) -> Self {
        Self {
            client: hyper::Client::new(),
            host,
            user: None,
            password: None,
        }
    }

    pub fn authentication<V>(&mut self, user: V, password: V) -> Result<(), hyper::http::Error>
    where
        HeaderValue: TryFrom<V>,
        <HeaderValue as TryFrom<V>>::Error: Into<hyper::http::Error>,
    {
        self.user = Some(user.try_into().map_err(Into::into)?);
        self.password = Some(password.try_into().map_err(Into::into)?);
        Ok(())
    }

    /// expects the column names in order
    pub fn inserter<T: RowBinary + Send + 'static + ColumnNames<N>, const N: usize>(
        &self,
        table: &str,
    ) -> UnboundedSender<T> {
        let url = format!(
            "{}?query=insert%20into%20{}%20({})%20format%20RowBinary",
            self.host,
            table,
            <T as ColumnNames<N>>::NAMES.join(",")
        );
        let (tx, rx) = futures::channel::mpsc::unbounded();
        let client = self.client.clone();
        let user = self.user.clone();
        let password = self.password.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(1));
            let rx = rx.map(to_vec).ready_chunks(10000);
            pin!(rx);

            while let Some(f) = rx.next().await {
                interval.tick().await;

                let mut req = Request::post(&url);
                if let (Some(user), Some(password)) = (&user, &password) {
                    req = req.header("X-ClickHouse-User", user);
                    req = req.header("X-ClickHouse-Key", password);
                }

                let req = req
                    .body(Body::wrap_stream(iter(f)))
                    .expect("invalid request built");
                client.request(req).await.ok();
            }
        });
        tx
    }
}

fn to_vec(r: impl RowBinary) -> std::io::Result<Vec<u8>> {
    let mut vec = Vec::with_capacity(r.size_hint());
    r.encode(&mut vec)?;
    Ok(vec)
}

pub trait ColumnNames<const N: usize> {
    const NAMES: [&'static str; N];
}

pub trait RowBinary {
    fn encode(&self, buf: &mut impl std::io::Write) -> std::io::Result<()>;

    fn size_hint(&self) -> usize;
}

macro_rules! ints {
    ($($t:ty),+) => {
        $(impl RowBinary for $t {
            fn encode(&self, buf: &mut impl std::io::Write) -> std::io::Result<()> {
                buf.write_all(&self.to_le_bytes()[..])
            }

            fn size_hint(&self) -> usize {
                std::mem::size_of::<Self>()
            }
        })+
    }
}

macro_rules! floats {
    ($($t:ty),+) => {
        $(impl RowBinary for $t {
            fn encode(&self, buf: &mut impl std::io::Write) -> std::io::Result<()> {
                buf.write_all(&self.to_bits().to_le_bytes()[..])
            }

            fn size_hint(&self) -> usize {
                std::mem::size_of::<Self>()
            }
        })+
    }
}

ints!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);
floats!(f32, f64);

impl RowBinary for String {
    fn encode(&self, buf: &mut impl std::io::Write) -> std::io::Result<()> {
        let str_slice = &self[..];
        str_slice.encode(buf)
    }

    fn size_hint(&self) -> usize {
        <str as RowBinary>::size_hint(self)
    }
}

impl RowBinary for str {
    fn encode(&self, buf: &mut impl std::io::Write) -> std::io::Result<()> {
        let length = self.as_bytes().len() as u64;
        leb128::write::unsigned(buf, length)?;
        buf.write_all(self.as_bytes())
    }

    fn size_hint(&self) -> usize {
        self.as_bytes().len()
    }
}

impl RowBinary for chrono::NaiveDateTime {
    fn encode(&self, buf: &mut impl std::io::Write) -> std::io::Result<()> {
        // Clickhouse expects the timestamp for DateTime64 with 3 digits precision as
        // signed 64 bits integers for Row Binary format
        let timestamp = self.timestamp_millis();
        buf.write_all(&timestamp.to_le_bytes()[..])
    }

    fn size_hint(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl<T: RowBinary> RowBinary for Option<T> {
    fn encode(&self, buf: &mut impl std::io::Write) -> std::io::Result<()> {
        match self {
            None => buf.write_all(&[1]),
            Some(value) => {
                buf.write_all(&[0])?;
                value.encode(buf)
            }
        }
    }

    fn size_hint(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
