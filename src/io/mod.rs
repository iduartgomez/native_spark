use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use crate::context::Context;
use crate::rdd::{Rdd, RddBase};
use crate::serializable_traits::{Data, SerFunc};
use downcast_rs::Downcast;
use serde_traitobject::Arc as SerArc;
use thiserror::Error;

mod local_file_reader;
pub use local_file_reader::{LocalFsReader, LocalFsReaderConfig};
#[cfg(aws_connector)]
mod aws;

pub trait ReaderConfiguration<I: Data> {
    fn make_reader<F, O>(self, context: Arc<Context>, decoder: F) -> SerArc<dyn Rdd<Item = O>>
    where
        O: Data,
        F: SerFunc(I) -> O;
}

#[derive(Debug, Error)]
pub enum IOError {
    #[error("file not found")]
    FileNotFound,
}
