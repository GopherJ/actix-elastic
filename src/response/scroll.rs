use serde::{Deserialize, Serialize};

use crate::response::common::{AggsWrapper, HitsWrapper, Shards};

#[derive(Serialize, Deserialize, Debug)]
pub struct ScrollResponse<T> {
    #[serde(rename = "_scroll_id")]
    pub scroll_id: String,
    pub took: u64,
    pub timed_out: bool,
    #[serde(rename = "_shards")]
    pub shards: Shards,
    pub hits: HitsWrapper<T>,
    pub aggregations: Option<AggsWrapper>,
    pub status: Option<u16>,
}
