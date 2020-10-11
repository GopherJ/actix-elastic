pub mod client;
pub mod error;
pub mod response;

pub use client::{EsClient, EsCmd, EsResult};
pub use error::{Error, Result};
pub use response::{
    BulkResponse, Hit, HitsTotal, HitsWrapper, ItemError, ScrollResponse, SearchResponse, Shards,
};
