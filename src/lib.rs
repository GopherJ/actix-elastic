pub mod client;
pub mod error;
pub mod response;

pub use client::{EsClient, EsCmd, EsResult};
pub use error::{Error, Result};
pub use response::{
    BulkResponse, CatIndicesResponse, Hit, HitsWrapper, ItemError, ScrollResponse, SearchResponse,
    Shards,
};

#[cfg(not(feature = "v5"))]
pub use response::HitsTotal;
