use std::io::Read;
use std::sync::Arc;

use super::{IOError, ReaderConfiguration};
use crate::error;
use crate::rdd::Rdd;
use crate::serializable_traits::{Data, SerFunc};
use crate::Context;
use rusoto_core::Region;
use rusoto_credential::DefaultCredentialsProvider;
use rusoto_s3::{GetObjectRequest, S3Client, S3};
use serde_traitobject::Arc as SerArc;

struct S3Connector {}

impl S3Connector {
    fn get_object(&self, region: Region) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // Naive first implementation; would go along this lines
        // Keep all necessary config but instantiate S3Client in each partition as necessary

        // All this has to be blocking because our executors are blocking right now
        // Rusoto has not moved to futures >=0.3 and/or tokio 0.2 yet; they are on the process.
        let client = S3Client::new(region);
        let result = client.get_object(GetObjectRequest::default()).sync()?;
        let mut buf = Vec::new();
        let byte_stream = result
            .body
            .ok_or_else(|| IOError::FileNotFound)?
            .into_blocking_read()
            // check whether this is the most optimal,
            // how does Rusoto ByteStream work under the hood? avoid extra syscalls
            .read_to_end(&mut buf)?;
        Ok(buf)
    }
}

impl ReaderConfiguration<Vec<u8>> for S3Connector {
    fn make_reader<F, U>(
        self,
        context: Arc<Context>,
        decoder: F,
    ) -> error::Result<SerArc<dyn Rdd<Item = U>>>
    where
        F: SerFunc(Vec<u8>) -> U,
        U: Data,
    {
        let client = DefaultCredentialsProvider::new().map_err(|_| IOError::CredentialsNotFound)?;

        // 1. Partitionate per total number of cores available; ideally this should maximize throughput.
        let parallelism = context.num_threads();

        // 2. Inside each partition func open a blocking connection
        // 3. In parallel fetch files from the given logical dir path from S3; see get_object fn above.
        //    A similar aproach to that of the local reader when finding/filtering files.
        // 4. Decode the files using the provided decoder func.

        todo!()
    }
}
