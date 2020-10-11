use serde::{Deserialize, Serialize};

use crate::response::common::{AggsWrapper, HitsWrapper, Shards};

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResponse<T> {
    pub took: u64,
    pub timed_out: bool,
    #[serde(rename = "_shards")]
    pub shards: Shards,
    pub hits: HitsWrapper<T>,
    pub aggregations: Option<AggsWrapper>,
    pub status: Option<u16>,
}
