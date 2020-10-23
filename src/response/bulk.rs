use serde::{Deserialize, Serialize};

use crate::response::common::Shards;

use std::fmt;

#[derive(Debug, Serialize, Deserialize)]
pub struct BulkResponse {
    pub took: i32,
    pub errors: bool,
    pub items: Vec<Item>,
}

impl BulkResponse {
    pub fn succeed_items(&self) -> impl Iterator<Item = &Item> {
        self.items.iter().filter(|x| x.success())
    }

    pub fn first_error(&self) -> Option<ItemError> {
        self.items
            .iter()
            .find(|x| !x.success())
            .and_then(|x| x.error())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Item {
    #[serde(rename = "create")]
    Create(ItemBody),
    #[serde(rename = "delete")]
    Delete(ItemBody),
    #[serde(rename = "index")]
    Index(ItemBody),
    #[serde(rename = "update")]
    Update(ItemBody),
}

impl Item {
    pub fn success(&self) -> bool {
        match *self {
            Item::Create(ref body)
            | Item::Delete(ref body)
            | Item::Index(ref body)
            | Item::Update(ref body) => body.status >= 200 && body.status < 400,
        }
    }

    pub fn error(&self) -> Option<ItemError> {
        match *self {
            Item::Create(ref body)
            | Item::Delete(ref body)
            | Item::Index(ref body)
            | Item::Update(ref body) => body.error.clone(),
        }
    }

    pub fn get_id(&self) -> String {
        match *self {
            Item::Create(ref body)
            | Item::Delete(ref body)
            | Item::Index(ref body)
            | Item::Update(ref body) => body.id.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ItemBody {
    #[serde(rename = "_index")]
    pub index: String,
    #[serde(rename = "_type")]
    pub ty: String,
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(rename = "_version")]
    pub version: Option<i32>,
    #[serde(rename = "_shards")]
    pub shards: Option<Shards>,
    #[serde(rename = "_seq_no")]
    pub seq_no: Option<i64>,
    #[serde(rename = "_primary_term")]
    pub primary_term: Option<i64>,
    pub result: Option<String>,
    pub status: i32,
    pub error: Option<ItemError>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ItemError {
    #[serde(rename = "type")]
    ty: String,
    reason: String,
    index_uuid: String,
    shard: String,
    index: String,
}

impl std::error::Error for ItemError {}

impl fmt::Display for ItemError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "reason: {}", self.reason)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_bulk_response_all_success() {
        let resp_str = r#"{
   "took": 30,
   "errors": false,
   "items": [
      {
         "index": {
            "_index": "test",
            "_type": "_doc",
            "_id": "1",
            "_version": 1,
            "result": "created",
            "_shards": {
               "total": 2,
               "successful": 1,
               "failed": 0
            },
            "status": 201,
            "_seq_no" : 0,
            "_primary_term": 1
         }
      },
      {
         "delete": {
            "_index": "test",
            "_type": "_doc",
            "_id": "2",
            "_version": 1,
            "result": "not_found",
            "_shards": {
               "total": 2,
               "successful": 1,
               "failed": 0
            },
            "status": 404,
            "_seq_no" : 1,
            "_primary_term" : 2
         }
      },
      {
         "create": {
            "_index": "test",
            "_type": "_doc",
            "_id": "3",
            "_version": 1,
            "result": "created",
            "_shards": {
               "total": 2,
               "successful": 1,
               "failed": 0
            },
            "status": 201,
            "_seq_no" : 2,
            "_primary_term" : 3
         }
      },
      {
         "update": {
            "_index": "test",
            "_type": "_doc",
            "_id": "1",
            "_version": 2,
            "result": "updated",
            "_shards": {
                "total": 2,
                "successful": 1,
                "failed": 0
            },
            "status": 200,
            "_seq_no" : 3,
            "_primary_term" : 4
         }
      }
   ]
}"#;

        assert!(serde_json::from_str::<BulkResponse>(resp_str).is_ok());
    }

    #[test]
    fn test_deserialize_bulk_response_with_error() {
        let resp_str_1 = r#"{
  "took": 486,
  "errors": true,
  "items": [
    {
      "update": {
        "_index": "index1",
        "_type" : "_doc",
        "_id": "5",
        "status": 404,
        "error": {
          "type": "document_missing_exception",
          "reason": "[_doc][5]: document missing",
          "index_uuid": "aAsFqTI0Tc2W0LCWgPNrOA",
          "shard": "0",
          "index": "index1"
        }
      }
    },
    {
      "update": {
        "_index": "index1",
        "_type" : "_doc",
        "_id": "6",
        "status": 404,
        "error": {
          "type": "document_missing_exception",
          "reason": "[_doc][6]: document missing",
          "index_uuid": "aAsFqTI0Tc2W0LCWgPNrOA",
          "shard": "0",
          "index": "index1"
        }
      }
    },
    {
      "create": {
        "_index": "index1",
        "_type" : "_doc",
        "_id": "7",
        "_version": 1,
        "result": "created",
        "_shards": {
          "total": 2,
          "successful": 1,
          "failed": 0
        },
        "_seq_no": 0,
        "_primary_term": 1,
        "status": 201
      }
    }
  ]
}"#;
        assert!(serde_json::from_str::<BulkResponse>(resp_str_1).is_ok());

        let resp_str_2 = r#"{
  "took": 486,
  "errors": true,
  "items": [
    {
      "update": {
        "_index": "index1",
        "_type" : "_doc",
        "_id": "5",
        "status": 404,
        "error": {
          "type": "document_missing_exception",
          "reason": "[_doc][5]: document missing",
          "index_uuid": "aAsFqTI0Tc2W0LCWgPNrOA",
          "shard": "0",
          "index": "index1"
        }
      }
    },
    {
      "update": {
        "_index": "index1",
        "_type" : "_doc",
        "_id": "6",
        "status": 404,
        "error": {
          "type": "document_missing_exception",
          "reason": "[_doc][6]: document missing",
          "index_uuid": "aAsFqTI0Tc2W0LCWgPNrOA",
          "shard": "0",
          "index": "index1"
        }
      }
    },
    {
      "create": {
        "_index": "index1",
        "_type" : "_doc",
        "_id": "7",
        "_version": 1,
        "result": "created",
        "_shards": {
          "total": 2,
          "successful": 1,
          "failed": 0
        },
        "_seq_no": 0,
        "_primary_term": 1,
        "status": 201
      }
    }
  ]
}"#;

        assert!(serde_json::from_str::<BulkResponse>(resp_str_2).is_ok());
    }
}
