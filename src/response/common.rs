use serde::{Deserialize, Serialize};

use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct Shards {
    pub total: u32,
    pub skipped: Option<u32>,
    pub successful: u32,
    pub failed: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HitsWrapper<T> {
    pub total: HitsTotal,
    pub max_score: Option<f32>,
    pub hits: Vec<Hit<T>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HitsTotal {
    pub value: usize,
    pub relation: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Hit<T> {
    #[serde(rename = "_index")]
    pub index: String,
    #[serde(rename = "_type")]
    pub ty: String,
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(rename = "_score")]
    pub score: Option<f32>,
    #[serde(rename = "_source")]
    pub source: Option<T>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AggsWrapper(Value);
