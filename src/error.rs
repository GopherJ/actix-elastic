use actix::MailboxError;
use elasticsearch::Error as ElasticError;
use serde_json::error::Error as SerializeJsonError;
use thiserror::Error as ThisError;
use url::ParseError as UrlParseError;

use crate::response::ItemError;

use core::fmt::Error as SerializeError;
use std::io::Error as IoError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("Io Error: `{0:?}`")]
    IoError(#[from] IoError),
    #[error("Elastic Error: `{0:?}`")]
    ElasticError(#[from] ElasticError),
    #[error("Url Parse Error: `{0:?}`")]
    UrlParseError(#[from] UrlParseError),
    #[error("Serialize Error: `{0:?}`")]
    SerializeError(#[from] SerializeError),
    #[error("Serialize Json Error: `{0:?}`")]
    SerializeJsonError(#[from] SerializeJsonError),
    #[error("Mailbox Error: `{0:?}`")]
    MailboxError(#[from] MailboxError),
    #[error("Elasitc Bulk Item Error: `{0:?}`")]
    BulkItemError(#[from] ItemError),
}

pub type Result<T> = std::result::Result<T, Error>;
