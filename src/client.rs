use actix::{
    fut::{err, ok},
    prelude::*,
};
use backoff::{backoff::Backoff, ExponentialBackoff};
use bytes::{BufMut, Bytes, BytesMut};
use elasticsearch::{
    cat::{CatIndices, CatIndicesParts},
    http::{
        request::JsonBody,
        transport::{SingleNodeConnectionPool, TransportBuilder},
    },
    BulkParts, ClearScrollParts, DeleteByQueryParts, Elasticsearch, IndexParts, ScrollParts,
    SearchParts, UpdateByQueryParts,
};
use futures::{
    future::{BoxFuture, Future, FutureExt},
    stream::Stream,
};
use log::error;
use pin_project::pin_project;
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::{json, Value};
use url::Url;

use crate::{
    error::{Error, Result},
    response::{BulkResponse, CatIndicesResponse, Hit, ScrollResponse, SearchResponse},
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

use std::{
    borrow::Cow,
    io::{Error as IoError, ErrorKind},
    marker::PhantomData,
    pin::Pin,
    task::{Context as StdContext, Poll},
    time::{Duration, Instant},
};

#[derive(Debug)]
pub struct EsClient<T> {
    url: Url,
    hb: Instant,
    client: Option<Elasticsearch>,
    backoff: ExponentialBackoff,
    _marker: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Unpin + 'static> EsClient<T> {
    pub fn new(url: &str) -> Result<EsClient<T>> {
        let url = Url::parse(url)?;

        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = None;

        Ok(Self {
            url,
            hb: Instant::now(),
            client: None,
            backoff,
            _marker: PhantomData,
        })
    }

    fn hb(&self, ctx: &mut <Self as Actor>::Context) {
        ctx.notify(EsCmd::<T>::Ping);

        ctx.run_later(HEARTBEAT_INTERVAL, |act, ctx| {
            if Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
                error!("elasticsearch heartbeat failed, disconnecting!");
                ctx.stop()
            } else {
                act.hb(ctx)
            }
        });
    }

    fn init(&mut self, ctx: &mut <Self as Actor>::Context) {
        let url = self.url.clone();
        match TransportBuilder::new(SingleNodeConnectionPool::new(url))
            .disable_proxy()
            .build()
        {
            Ok(transport) => {
                self.client = Some(Elasticsearch::new(transport));
            }
            Err(err) => {
                error!("Cannot create elasticsearch transport: {}", err);
                if let Some(timeout) = self.backoff.next_backoff() {
                    ctx.run_later(timeout, |_, ctx| ctx.stop());
                } else {
                    ctx.stop();
                }
            }
        }
    }
}

impl<T: Serialize + DeserializeOwned + Unpin + 'static> Actor for EsClient<T> {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.init(ctx);
        self.hb(ctx);
    }
}

impl<T: Serialize + DeserializeOwned + Unpin + 'static> Supervised for EsClient<T> {
    fn restarting(&mut self, _: &mut Self::Context) {
        self.client.take();
        error!("reconnecting to elasticsearch at: `{}`", self.url);
    }
}

#[derive(Message, Debug)]
#[rtype(result = "Result<EsResult<T>>")]
pub enum EsCmd<T: Serialize + DeserializeOwned + 'static> {
    Ping,
    Index(Cow<'static, str>, (String, T)),
    BulkIndex(Cow<'static, str>, Vec<(String, T)>),
    BulkDelete(Cow<'static, str>, Vec<String>),
    Search(Cow<'static, str>, Value),
    SearchHits(Cow<'static, str>, Value),
    DeleteByQuery(Cow<'static, str>, Value),
    UpdateByQuery(Cow<'static, str>, Value),
    ScrollBytes(String, Value),
    ScrollItems(String, Value),
    CatIndices,
}

pub enum EsResult<T: Serialize + DeserializeOwned + 'static> {
    Ping,
    Index,
    BulkIndex(Vec<String>),
    BulkDelete(Vec<String>),
    Search(SearchResponse<T>),
    SearchHits(Vec<T>),
    DeleteByQuery,
    UpdateByQuery,
    ScrollBytes(ScrollBytesStream<T>),
    ScrollItems(ScrollItemsStream<T>),
    CatIndices(Vec<CatIndicesResponse>),
}

impl<T: Serialize + DeserializeOwned + Unpin + 'static> Handler<EsCmd<T>> for EsClient<T> {
    type Result = ResponseActFuture<Self, Result<EsResult<T>>>;

    fn handle(&mut self, msg: EsCmd<T>, _ctx: &mut Self::Context) -> Self::Result {
        let client = match self.client {
            Some(ref x) => x.clone(),
            None => {
                return Box::pin(err(Error::IoError(IoError::new(
                    ErrorKind::NotConnected,
                    "Elasticsearch node disconnected",
                ))));
            }
        };

        let res = async move {
            match msg {
                EsCmd::Ping => Ok(client.ping().send().await.map(|_| EsResult::Ping)?),
                EsCmd::Index(index, (id, body)) => Ok(client
                    .index(IndexParts::IndexId(&index, &id))
                    .body(body)
                    .send()
                    .await
                    .and_then(|resp| match resp.error_for_status_code() {
                        Ok(_) => Ok(EsResult::Index),
                        Err(err) => Err(err),
                    })?),
                EsCmd::BulkIndex(index, body) => client
                    .bulk(BulkParts::Index(&index))
                    .body(
                        body.into_iter()
                            .map(|(id, x)| {
                                vec![
                                    JsonBody::from(json!({"index": {"_id": id }})),
                                    JsonBody::from(json!(x)),
                                ]
                            })
                            .flatten()
                            .collect(),
                    )
                    .send()
                    .await
                    .and_then(|resp| match resp.error_for_status_code() {
                        Ok(res) => Ok(res),
                        Err(err) => Err(err),
                    })?
                    .json::<BulkResponse>()
                    .await
                    .map_err(Error::from)
                    .and_then(|resp| {
                        if let Some(first_error) = resp.first_error() {
                            Err(Error::from(first_error))
                        } else {
                            Ok(EsResult::BulkIndex(
                                resp.succeed_items().map(|x| x.get_id()).collect(),
                            ))
                        }
                    }),
                EsCmd::BulkDelete(index, body) => client
                    .bulk(BulkParts::Index(&index))
                    .body(
                        body.into_iter()
                            .map(|id| vec![JsonBody::from(json!({"delete": {"_id": id }}))])
                            .flatten()
                            .collect(),
                    )
                    .send()
                    .await
                    .and_then(|resp| match resp.error_for_status_code() {
                        Ok(res) => Ok(res),
                        Err(err) => Err(err),
                    })?
                    .json::<BulkResponse>()
                    .await
                    .map_err(Error::from)
                    .and_then(|resp| {
                        if let Some(first_error) = resp.first_error() {
                            Err(Error::from(first_error))
                        } else {
                            Ok(EsResult::BulkDelete(
                                resp.succeed_items().map(|x| x.get_id()).collect(),
                            ))
                        }
                    }),
                EsCmd::Search(index, body) => Ok(client
                    .search(SearchParts::Index(&[&index]))
                    .body(body)
                    .send()
                    .await
                    .and_then(|resp| match resp.error_for_status_code() {
                        Ok(res) => Ok(res),
                        Err(err) => Err(err),
                    })?
                    .json::<SearchResponse<T>>()
                    .await
                    .map(|res| EsResult::Search(res))?),
                EsCmd::SearchHits(index, body) => Ok(client
                    .search(SearchParts::Index(&[&index]))
                    .body(body)
                    .send()
                    .await
                    .and_then(|resp| match resp.error_for_status_code() {
                        Ok(res) => Ok(res),
                        Err(err) => Err(err),
                    })?
                    .json::<SearchResponse<T>>()
                    .await
                    .map(|res| {
                        EsResult::SearchHits(
                            res.hits.hits.into_iter().filter_map(|x| x.source).collect(),
                        )
                    })?),
                EsCmd::DeleteByQuery(index, query) => Ok(client
                    .delete_by_query(DeleteByQueryParts::Index(&[&index]))
                    .body(query)
                    .send()
                    .await
                    .and_then(|resp| match resp.error_for_status_code() {
                        Ok(_) => Ok(EsResult::DeleteByQuery),
                        Err(err) => Err(err.into()),
                    })?),
                EsCmd::UpdateByQuery(index, body) => Ok(client
                    .update_by_query(UpdateByQueryParts::Index(&[&index]))
                    .body(body)
                    .send()
                    .await
                    .and_then(|resp| match resp.error_for_status_code() {
                        Ok(_) => Ok(EsResult::UpdateByQuery),
                        Err(err) => Err(err.into()),
                    })?),
                EsCmd::ScrollBytes(index, body) => Ok(EsResult::ScrollBytes(
                    ScrollBytesStream::new(index, body, client),
                )),
                EsCmd::ScrollItems(index, body) => Ok(EsResult::ScrollItems(
                    ScrollItemsStream::new(index, body, client),
                )),
                EsCmd::CatIndices => Ok(CatIndices::new(client.transport(), CatIndicesParts::None)
                    .format("json")
                    .send()
                    .await
                    .and_then(|resp| match resp.error_for_status_code() {
                        Ok(res) => Ok(res),
                        Err(err) => Err(err),
                    })?
                    .json::<Vec<CatIndicesResponse>>()
                    .await
                    .map(|res| EsResult::CatIndices(res))?),
            }
        }
        .into_actor(self)
        .then(|res, act, _ctx| match res {
            Ok(res) => ok({
                if matches!(res, EsResult::Ping) {
                    act.hb = Instant::now();
                }
                res
            }),
            Err(error) => err(error.into()),
        });

        Box::pin(res)
    }
}

#[pin_project]
pub struct ScrollBytesStream<T: Serialize + DeserializeOwned + 'static> {
    #[pin]
    fut: Option<BoxFuture<'static, Result<Option<ScrollResponse<T>>>>>,
    index: String,
    query: Value,
    start_bytes_sent: bool,
    current: usize,
    total: Option<usize>,
    client: Elasticsearch,
}

impl<T: Serialize + DeserializeOwned + 'static> ScrollBytesStream<T> {
    pub fn new(index: String, query: Value, client: Elasticsearch) -> Self {
        Self {
            fut: None,
            current: 0,
            total: None,
            query,
            index,
            start_bytes_sent: false,
            client,
        }
    }
}

#[pin_project]
pub struct ScrollItemsStream<T: Serialize + DeserializeOwned + 'static> {
    #[pin]
    fut: Option<BoxFuture<'static, Result<Option<ScrollResponse<T>>>>>,
    index: String,
    query: Value,
    current: usize,
    total: Option<usize>,
    client: Elasticsearch,
}

impl<T: Serialize + DeserializeOwned + 'static> ScrollItemsStream<T> {
    pub fn new(index: String, query: Value, client: Elasticsearch) -> Self {
        Self {
            fut: None,
            current: 0,
            total: None,
            query,
            index,
            client,
        }
    }
}

impl<T: Serialize + DeserializeOwned + 'static> Stream for ScrollBytesStream<T> {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut StdContext) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if this.fut.is_none() && *this.current == 0 && this.total.is_none() {
            this.fut.as_mut().set(Some(
                scroll_start_response(this.index.clone(), this.query.clone(), this.client.clone())
                    .boxed(),
            ));
        }

        match this.fut.as_mut().as_pin_mut() {
            None => Poll::Ready(None),
            Some(f) => match f.poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(err)) => {
                    this.fut.as_mut().set(None);
                    Poll::Ready(Some(Err(err.into())))
                }
                Poll::Ready(Ok(None)) => {
                    this.fut.as_mut().set(None);
                    Poll::Ready(None)
                }
                Poll::Ready(Ok(Some(scroll_resp))) => {
                    let scroll_id = scroll_resp.scroll_id;
                    let hits = scroll_resp.hits.hits;
                    let hits_len = hits.len();

                    let total = *this.total.get_or_insert(scroll_resp.hits.total.value);

                    if total == 0 {
                        this.fut
                            .as_mut()
                            .set(Some(clear_scroll(scroll_id, this.client.clone()).boxed()));
                        let mut b = BytesMut::with_capacity(2);
                        b.put_u8(b'[');
                        b.put_u8(b']');
                        return Poll::Ready(Some(Ok::<_, Error>(Bytes::from(b))));
                    }

                    if hits_len == total {
                        this.fut
                            .as_mut()
                            .set(Some(clear_scroll(scroll_id, this.client.clone()).boxed()));
                        *this.current = total;
                        return Poll::Ready(Some(
                            serde_json::to_vec(&hits)
                                .map(Bytes::from)
                                .map_err(Into::into),
                        ));
                    }

                    if !*this.start_bytes_sent {
                        return match gen_start_bytes(hits) {
                            Ok(b) => {
                                *this.current = hits_len;
                                *this.start_bytes_sent = true;
                                Poll::Ready(Some(Ok(Bytes::from(b))))
                            }
                            Err(err) => Poll::Ready(Some(Err(err.into()))),
                        };
                    }

                    let next_current = *this.current + hits_len;

                    return match gen_middle_bytes(hits) {
                        Err(err) => {
                            this.fut.as_mut().set(None);
                            Poll::Ready(Some(Err(err.into())))
                        }
                        Ok(mut b) => {
                            if total > next_current {
                                *this.current = next_current;

                                this.fut.as_mut().set(Some(
                                    scroll_next_response(scroll_id, this.client.clone()).boxed(),
                                ));
                            } else {
                                *this.current = total;
                                b.truncate(b.len() - 1);
                                b.put_u8(b']');

                                this.fut.as_mut().set(Some(
                                    clear_scroll(scroll_id, this.client.clone()).boxed(),
                                ));
                            }

                            Poll::Ready(Some(Ok(b.into())))
                        }
                    };
                }
            },
        }
    }
}

impl<T: Serialize + DeserializeOwned + 'static> Stream for ScrollItemsStream<T> {
    type Item = Result<Vec<Hit<T>>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut StdContext) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if this.fut.is_none() && *this.current == 0 && this.total.is_none() {
            this.fut.as_mut().set(Some(
                scroll_start_response(this.index.clone(), this.query.clone(), this.client.clone())
                    .boxed(),
            ));
        }

        match this.fut.as_mut().as_pin_mut() {
            None => Poll::Ready(None),
            Some(f) => match f.poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(err)) => {
                    this.fut.as_mut().set(None);
                    Poll::Ready(Some(Err(err.into())))
                }
                Poll::Ready(Ok(None)) => {
                    this.fut.as_mut().set(None);
                    Poll::Ready(None)
                }
                Poll::Ready(Ok(Some(scroll_resp))) => {
                    let scroll_id = scroll_resp.scroll_id;
                    let hits = scroll_resp.hits.hits;
                    let hits_len = hits.len();

                    let total = *this.total.get_or_insert(scroll_resp.hits.total.value);

                    if total == 0 {
                        this.fut
                            .as_mut()
                            .set(Some(clear_scroll(scroll_id, this.client.clone()).boxed()));
                        return Poll::Ready(Some(Ok::<_, Error>(vec![])));
                    }

                    if hits_len == total {
                        this.fut
                            .as_mut()
                            .set(Some(clear_scroll(scroll_id, this.client.clone()).boxed()));
                        *this.current = total;
                        return Poll::Ready(Some(Ok::<_, Error>(hits)));
                    }

                    let next_current = *this.current + hits_len;

                    if total > next_current {
                        *this.current = next_current;

                        this.fut.as_mut().set(Some(
                            scroll_next_response(scroll_id, this.client.clone()).boxed(),
                        ));
                    } else {
                        *this.current = total;

                        this.fut
                            .as_mut()
                            .set(Some(clear_scroll(scroll_id, this.client.clone()).boxed()));
                    }

                    Poll::Ready(Some(Ok(hits)))
                }
            },
        }
    }
}

async fn scroll_start_response<T: DeserializeOwned>(
    index: String,
    query: Value,
    client: Elasticsearch,
) -> Result<Option<ScrollResponse<T>>> {
    Ok(Some(
        client
            .search(SearchParts::Index(&[&index]))
            .scroll("30s")
            .body(query)
            .send()
            .await
            .and_then(|resp| match resp.error_for_status_code() {
                Ok(resp) => Ok(resp),
                Err(err) => Err(err),
            })?
            .json::<ScrollResponse<T>>()
            .await?,
    ))
}

async fn scroll_next_response<T: DeserializeOwned>(
    scroll_id: String,
    client: Elasticsearch,
) -> Result<Option<ScrollResponse<T>>> {
    Ok(Some(
        client
            .scroll(ScrollParts::None)
            .body(json!({
                "scroll": "30s",
                "scroll_id": scroll_id
            }))
            .send()
            .await
            .and_then(|resp| match resp.error_for_status_code() {
                Ok(resp) => Ok(resp),
                Err(err) => Err(err),
            })?
            .json::<ScrollResponse<T>>()
            .await?,
    ))
}

async fn clear_scroll<T: DeserializeOwned>(
    scroll_id: String,
    client: Elasticsearch,
) -> Result<Option<ScrollResponse<T>>> {
    Ok(client
        .clear_scroll(ClearScrollParts::None)
        .body(json!({ "scroll_id": [scroll_id] }))
        .send()
        .await
        .and_then(|resp| match resp.error_for_status_code() {
            Ok(_) => Ok(None),
            Err(err) => Err(err),
        })?)
}

fn gen_start_bytes<T: Serialize>(hits: Vec<Hit<T>>) -> Result<BytesMut> {
    if hits.is_empty() {
        let mut b = BytesMut::with_capacity(2);
        b.put_u8(b'[');
        b.put_u8(b',');
        return Ok(b);
    }

    Ok(serde_json::to_vec(&hits).map(|v| {
        let mut b = BytesMut::from(&v[..]);
        b.truncate(b.len() - 1);
        b.put_u8(b',');
        b
    })?)
}

fn gen_middle_bytes<T: Serialize>(hits: Vec<Hit<T>>) -> Result<BytesMut> {
    if hits.is_empty() {
        return Ok(BytesMut::new());
    }

    Ok(serde_json::to_vec(&hits).map(|v| {
        let mut b = BytesMut::from(&v[..]);
        let _ = b.split_to(1);
        b.truncate(b.len() - 1);
        b.put_u8(b',');
        b
    })?)
}
