# actix-elastic


## Installation

Add this package to `Cargo.toml` of your project. (Check https://crates.io/crates/actix-elastic for right version)

```toml
[dependencies]
actix = "0.10.0"
actix-elastic = "1.0.0"
```

## Get started

1. Create an elastic actix actor

```rust
use actix::{Arbiter, Addr, Supervisor};
use actix_elastic::{EsClient, EsCmd, EsResult};

let arb = Arbiter::new();
let elastic_url = "http://127.0.0.1:9200"

let addr: Addr<EsClient<CustomType>> = Supervisor::start_in_arbiter(&arb, |_| EsClient::new(elastic_url);
```

2. Send elastic command

```rust
use serde_json::json;

addr.send(EsCmd::Index("example_index_1", ("id_1", CustomType))).await??;

addr.send(EsCmd::Search("example_index_1", json!({
    "query": {
        "match_all": {}
    }
})).await??;

addr.send(EsCmd::DeleteByQuery("example_index_1", json!({
    "query": {
        "bool": {
            "must": [
                {
                    "match_phase": {
                        "name": "alice"
                    }
                }
            ]
        }
    }
}))).await??;
```

3. Scroll All Hits And Give Every Chunk To Client

```rust
use actix_web::HttpResponse;

match addr.send(EsCmd::ScrollBytes("example_index_1", json!({
    "query": {
        "match_all": {}
    }
}))).await?? {
    EsResult::ScrollBytes(stream) => Ok(HttpResponse::Ok().content_type("application/json").streaming_response(stream)),
        _ => unreachable!()
}
```

4. Scroll Deserialized Items

```rust

match addr.send(EsCmd::ScrollItems("example_index_1", json!({
    "query": {
        "match_all": {}
    }
}))).await?? {
    EsResult::ScrollItems(mut stream) => {
        while let Some(hits) = stream.next().await {
            println!("{:?}", hits.len());
        }
    },
        _ => unreachable!()
}
```
