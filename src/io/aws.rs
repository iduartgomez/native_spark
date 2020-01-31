use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use super::{IOError, ReaderConfiguration};
use crate::context::Context;
use crate::dependency::Dependency;
use crate::error::Result;
use crate::rdd::{Rdd, RddBase};
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use rusoto_core::credential::{
    AutoRefreshingProvider, AutoRefreshingProviderFuture, ChainProvider, ProvideAwsCredentials,
};
use rusoto_core::{HttpClient, Region};
use rusoto_s3::{GetObjectRequest, S3Client, S3};
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Arc as SerArc, Box as SerBox};

/// A connector which allows to read/write from S3 storage in AWS.
///
/// # Reading:
/// In order to fetch objects from S3 an strategy must be established in the configuration:
/// * Single object path, it will read a single file from storage and decode appropiatelly
///   the given content. Is ill-advised to storage data in a single large file if you would
///   like to process it in parallel.
/// * Read evey object in a logical path is given within a bucket, the reader will scan the bucket
///   for objects to read and load them in a balanced manner, incurs the upfront cost of having
///   to previously find the objects.
/// * Read all the objects in a bucket. Will attempt to read every file in a bucket without the need
///   to fetch a list of existing objects first.
#[derive(Clone, Serialize, Deserialize)]
struct S3Connector {
    strategy: FetchStrategy,
}

#[derive(Clone, Serialize, Deserialize)]
enum FetchStrategy {
    SingleObject,
    LogicalPath(PathBuf),
    Bucket,
}

impl S3Connector {
    fn new(strategy: FetchStrategy) -> S3Connector {
        S3Connector { strategy }
    }

    fn get_client() -> Result<S3Client> {
        let credentials = AutoRefreshingProvider::new(ChainProvider::new())
            .map_err(|_| IOError::CredentialsNotFound)?;
        let region: Region = Region::default();
        Ok(S3Client::new_with(
            HttpClient::new().map_err(|_| IOError::NotSafe("invalid TLS client"))?,
            credentials,
            region,
        ))
    }
}

impl ReaderConfiguration<Vec<u8>> for S3Connector {
    // Functioning:
    // 1. Partitionate per total number of cores available; ideally this should maximize throughput.
    // 2. Inside each partition func open a connection.
    // 3. In parallel fetch objects from the given logical dir path assigned to the partitioner
    //    from S3 as per the config.
    // 4. Decode the files using the provided decoder func.

    fn make_reader<F, U>(
        self,
        context: Arc<Context>,
        decoder: F,
    ) -> Result<SerArc<dyn Rdd<Item = U>>>
    where
        F: SerFunc(Vec<u8>) -> U,
        U: Data,
    {
        let parallelism = context.num_threads();
        let reader = S3Reader {
            id: context.new_rdd_id(),
            context,
            splits: parallelism,
        };
        todo!()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct S3Reader {
    id: usize,
    #[serde(skip_serializing, skip_deserializing)]
    context: Arc<Context>,
    splits: usize,
}

#[derive(Clone, Serialize, Deserialize)]
struct S3ReaderSplit(usize);

impl Split for S3ReaderSplit {
    fn get_index(&self) -> usize {
        self.0
    }
}

impl RddBase for S3Reader {
    fn get_rdd_id(&self) -> usize {
        self.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.context.clone()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        vec![]
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        (0..self.splits)
            .map(|idx| Box::new(S3ReaderSplit(idx)) as Box<dyn Split>)
            .collect()
    }

    default fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        ))
    }
}

impl Rdd for S3Reader {
    type Item = Vec<u8>;

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
    {
        todo!()
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        todo!()
    }

    fn compute(&self, _: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let client = S3Connector::get_client()?;
        // All this has to be blocking because our executors are blocking right now
        // Rusoto has not moved to futures >=0.3 and/or tokio 0.2 yet; they are on the process.
        let result = client
            .get_object(GetObjectRequest::default())
            .sync()
            .map_err(|_| IOError::ConnectivityError)?;

        let mut buf = Vec::new();
        let byte_stream = result
            .body
            .ok_or_else(|| IOError::FileNotFound)?
            .into_blocking_read()
            // check whether this is the most optimal,
            // how does Rusoto ByteStream work under the hood? avoid extra syscalls
            .read_to_end(&mut buf)
            .map_err(|_| IOError::ConnectivityError)?;

        todo!()
    }
}
