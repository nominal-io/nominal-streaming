use std::io::Read;
use std::io::Seek;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use conjure_error::Error;
use conjure_http::client::AsyncWriteBody;
use conjure_http::private::Stream;
use conjure_object::BearerToken;
use conjure_object::ResourceIdentifier;
use conjure_object::SafeLong;
use conjure_runtime_rustls_platform_verifier::conjure_runtime::BodyWriter;
use conjure_runtime_rustls_platform_verifier::PlatformVerifierClient;
use futures::StreamExt;
use nominal_api::api::rids::WorkspaceRid;
use nominal_api::ingest::api::AvroStreamOpts;
use nominal_api::ingest::api::CompleteMultipartUploadResponse;
use nominal_api::ingest::api::DatasetIngestTarget;
use nominal_api::ingest::api::ExistingDatasetIngestDestination;
use nominal_api::ingest::api::IngestOptions;
use nominal_api::ingest::api::IngestRequest;
use nominal_api::ingest::api::IngestResponse;
use nominal_api::ingest::api::IngestServiceAsyncClient;
use nominal_api::ingest::api::IngestSource;
use nominal_api::ingest::api::InitiateMultipartUploadRequest;
use nominal_api::ingest::api::InitiateMultipartUploadResponse;
use nominal_api::ingest::api::Part;
use nominal_api::ingest::api::S3IngestSource;
use nominal_api::upload::api::UploadServiceAsyncClient;
use tokio::sync::Semaphore;
use tracing::error;
use tracing::info;

use crate::client::NominalApiClients;
use crate::types::AuthProvider;

const SMALL_FILE_SIZE_LIMIT: u64 = 512 * 1024 * 1024; // 512 MB

#[derive(Clone)]
pub struct AvroIngestManager {
    pub upload_queue: async_channel::Receiver<PathBuf>,
}

impl AvroIngestManager {
    pub fn new(
        clients: NominalApiClients,
        http_client: reqwest::Client,
        handle: tokio::runtime::Handle,
        opts: UploaderOpts,
        upload_queue: async_channel::Receiver<PathBuf>,
        auth_provider: impl AuthProvider + 'static,
        data_source_rid: ResourceIdentifier,
    ) -> Self {
        let uploader = FileObjectStoreUploader::new(
            clients.upload,
            clients.ingest,
            http_client,
            handle.clone(),
            opts,
        );

        let upload_queue_clone = upload_queue.clone();

        handle.spawn(async move {
            Self::run(upload_queue_clone, uploader, auth_provider, data_source_rid).await;
        });

        AvroIngestManager { upload_queue }
    }

    pub async fn run(
        upload_queue: async_channel::Receiver<PathBuf>,
        uploader: FileObjectStoreUploader,
        auth_provider: impl AuthProvider + 'static,
        data_source_rid: ResourceIdentifier,
    ) {
        while let Ok(file_path) = upload_queue.recv().await {
            let file_name = file_path.to_str().unwrap_or("nmstream_file");
            let file = std::fs::File::open(&file_path);
            let Some(token) = auth_provider.token() else {
                error!("Missing token for upload");
                continue;
            };
            match file {
                Ok(f) => {
                    match upload_and_ingest_file(
                        uploader.clone(),
                        &token,
                        auth_provider.workspace_rid().map(WorkspaceRid::from),
                        f,
                        file_name,
                        &file_path,
                        data_source_rid.clone(),
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(e) => {
                            error!(
                                "Error uploading and ingesting file {}: {}",
                                file_path.display(),
                                e
                            );
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to open file {}: {:?}", file_path.display(), e);
                }
            }
        }
    }
}

async fn upload_and_ingest_file(
    uploader: FileObjectStoreUploader,
    token: &BearerToken,
    workspace_rid: Option<WorkspaceRid>,
    file: std::fs::File,
    file_name: &str,
    file_path: &PathBuf,
    data_source_rid: ResourceIdentifier,
) -> Result<(), String> {
    match uploader.upload(token, file, file_name, workspace_rid).await {
        Ok(response) => {
            match uploader
                .ingest_avro(token, &response, data_source_rid)
                .await
            {
                Ok(ingest_response) => {
                    info!(
                        "Successfully uploaded and ingested file {}: {:?}",
                        file_name, ingest_response
                    );
                    if let Err(e) = std::fs::remove_file(file_path) {
                        Err(format!(
                            "Failed to remove file {}: {:?}",
                            file_path.display(),
                            e
                        ))
                    } else {
                        info!("Removed file {}", file_path.display());
                        Ok(())
                    }
                }
                Err(e) => Err(format!("Failed to ingest file {file_name}: {e:?}")),
            }
        }
        Err(e) => Err(format!("Failed to upload file {file_name}: {e:?}")),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UploaderError {
    #[error("Conjure error: {0}")]
    Conjure(String),
    #[error("Failed to initiate multipart upload: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Failed to upload part: {0}")]
    HTTPError(#[from] reqwest::Error),
    #[error("Error executing upload tasks: {0}")]
    TokioError(#[from] tokio::task::JoinError),
    #[error("Error: {0}")]
    Other(String),
}

#[derive(Debug, Clone)]
pub struct UploaderOpts {
    pub chunk_size: usize,
    pub max_retries: usize,
    pub max_concurrent_uploads: usize,
}

impl Default for UploaderOpts {
    fn default() -> Self {
        UploaderOpts {
            chunk_size: 512 * 1024 * 1024, // 512 MB
            max_retries: 3,
            max_concurrent_uploads: 1,
        }
    }
}

pub struct FileWriteBody {
    file: std::fs::File,
}

impl FileWriteBody {
    pub fn new(file: std::fs::File) -> Self {
        FileWriteBody { file }
    }
}

impl AsyncWriteBody<BodyWriter> for FileWriteBody {
    async fn write_body(self: Pin<&mut Self>, w: Pin<&mut BodyWriter>) -> Result<(), Error> {
        let mut file = self
            .file
            .try_clone()
            .map_err(|e| Error::internal_safe(format!("Failed to clone file for upload: {e}")))?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .map_err(|e| Error::internal_safe(format!("Failed to read bytes from file: {e}")))?;

        w.write_bytes(buffer.into())
            .await
            .map_err(|e| Error::internal_safe(format!("Failed to write bytes to body: {e}")))?;

        Ok(())
    }

    async fn reset(self: Pin<&mut Self>) -> bool {
        let Ok(mut file) = self.file.try_clone() else {
            return false;
        };

        use std::io::SeekFrom;

        file.seek(SeekFrom::Start(0)).is_ok()
    }
}

#[derive(Clone)]
pub struct FileObjectStoreUploader {
    upload_client: UploadServiceAsyncClient<PlatformVerifierClient>,
    ingest_client: IngestServiceAsyncClient<PlatformVerifierClient>,
    http_client: reqwest::Client,
    handle: tokio::runtime::Handle,
    opts: UploaderOpts,
}

impl FileObjectStoreUploader {
    pub fn new(
        upload_client: UploadServiceAsyncClient<PlatformVerifierClient>,
        ingest_client: IngestServiceAsyncClient<PlatformVerifierClient>,
        http_client: reqwest::Client,
        handle: tokio::runtime::Handle,
        opts: UploaderOpts,
    ) -> Self {
        FileObjectStoreUploader {
            upload_client,
            ingest_client,
            http_client,
            handle,
            opts,
        }
    }

    pub async fn initiate_upload(
        &self,
        token: &BearerToken,
        file_name: &str,
        workspace_rid: Option<WorkspaceRid>,
    ) -> Result<InitiateMultipartUploadResponse, UploaderError> {
        let request = InitiateMultipartUploadRequest::builder()
            .filename(file_name)
            .filetype("application/octet-stream")
            .workspace(workspace_rid)
            .build();
        let response = self
            .upload_client
            .initiate_multipart_upload(token, &request)
            .await
            .map_err(|e| UploaderError::Conjure(format!("{e:?}")))?;

        info!("Initiated multipart upload for file: {}", file_name);
        Ok(response)
    }

    #[expect(clippy::too_many_arguments)]
    async fn upload_part(
        client: UploadServiceAsyncClient<PlatformVerifierClient>,
        http_client: reqwest::Client,
        token: BearerToken,
        upload_id: String,
        key: String,
        part_number: i32,
        chunk: Vec<u8>,
        max_retries: usize,
    ) -> Result<Part, UploaderError> {
        let mut attempts = 0;

        loop {
            attempts += 1;
            match Self::try_upload_part(
                client.clone(),
                http_client.clone(),
                &token,
                &upload_id,
                &key,
                part_number,
                chunk.clone(),
            )
            .await
            {
                Ok(part) => return Ok(part),
                Err(e) if attempts < max_retries => {
                    error!("Upload attempt {} failed, retrying: {}", attempts, e);
                    continue;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    async fn try_upload_part(
        client: UploadServiceAsyncClient<PlatformVerifierClient>,
        http_client: reqwest::Client,
        token: &BearerToken,
        upload_id: &str,
        key: &str,
        part_number: i32,
        chunk: Vec<u8>,
    ) -> Result<Part, UploaderError> {
        let response = client
            .sign_part(token, upload_id, key, part_number)
            .await
            .map_err(|e| UploaderError::Conjure(format!("{e:?}")))?;

        let mut request_builder = http_client.put(response.url()).body(chunk);

        for (header_name, header_value) in response.headers() {
            request_builder = request_builder.header(header_name, header_value);
        }

        let http_response = request_builder.send().await?;
        let headers = http_response.headers().clone();
        let status = http_response.status();

        if !status.is_success() {
            error!("Failed to upload body");
            return Err(UploaderError::Other(format!(
                "Failed to upload part {part_number}: HTTP status {status}"
            )));
        }

        let etag = headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("ignored-etag");

        Ok(Part::new(part_number, etag))
    }

    pub async fn upload_parts<R>(
        &self,
        token: &BearerToken,
        reader: R,
        key: &str,
        upload_id: &str,
    ) -> Result<CompleteMultipartUploadResponse, UploaderError>
    where
        R: Read + Send + 'static,
    {
        let chunks = ChunkedStreamReader::new(reader, self.opts.chunk_size);

        let parallel_part_uploads = Arc::new(Semaphore::new(self.opts.max_concurrent_uploads));
        let mut upload_futures = Vec::new();

        futures::pin_mut!(chunks);

        while let Some(entry) = chunks.next().await {
            let (index, chunk) = entry?;
            let part_number = (index + 1) as i32;

            let token = token.clone();
            let key = key.to_string();
            let upload_id = upload_id.to_string();
            let parallel_part_uploads = Arc::clone(&parallel_part_uploads);
            let client = self.upload_client.clone();
            let http_client = self.http_client.clone();
            let max_retries = self.opts.max_retries;

            upload_futures.push(self.handle.spawn(async move {
                let _permit = parallel_part_uploads.acquire().await;
                Self::upload_part(
                    client,
                    http_client,
                    token,
                    upload_id,
                    key,
                    part_number,
                    chunk,
                    max_retries,
                )
                .await
            }));
        }

        let mut part_responses = futures::future::join_all(upload_futures)
            .await
            .into_iter()
            .map(|result| result.map_err(UploaderError::TokioError)?)
            .collect::<Result<Vec<_>, _>>()?;

        part_responses.sort_by_key(|part| part.part_number());

        let response = self
            .upload_client
            .complete_multipart_upload(token, upload_id, key, &part_responses)
            .await
            .map_err(|e| UploaderError::Conjure(format!("{e:?}")))?;

        Ok(response)
    }

    pub async fn upload_small_file(
        &self,
        token: &BearerToken,
        file_name: &str,
        size_bytes: i64,
        workspace_rid: Option<WorkspaceRid>,
        file: std::fs::File,
    ) -> Result<String, UploaderError> {
        let s3_path = self
            .upload_client
            .upload_file(
                token,
                file_name,
                SafeLong::new(size_bytes).ok(),
                workspace_rid.as_ref(),
                FileWriteBody::new(file),
            )
            .await
            .map_err(|e| UploaderError::Conjure(format!("{e:?}")))?;

        Ok(s3_path.as_str().to_string())
    }

    pub async fn upload<R>(
        &self,
        token: &BearerToken,
        reader: R,
        file_name: impl Into<&str>,
        workspace_rid: Option<WorkspaceRid>,
    ) -> Result<String, UploaderError>
    where
        R: Read + Send + 'static,
    {
        let file_name = file_name.into();
        let path = Path::new(file_name);
        let file_size = std::fs::metadata(path)?.len();
        if file_size < SMALL_FILE_SIZE_LIMIT {
            return self
                .upload_small_file(
                    token,
                    file_name,
                    file_size as i64,
                    workspace_rid,
                    std::fs::File::open(path)?,
                )
                .await;
        }

        let initiate_response = self
            .initiate_upload(token, file_name, workspace_rid.map(WorkspaceRid::from))
            .await?;
        let upload_id = initiate_response.upload_id();
        let key = initiate_response.key();

        let response = self.upload_parts(token, reader, key, upload_id).await?;

        let s3_path = response.location().ok_or_else(|| {
            UploaderError::Other("Upload response did not contain a location".to_string())
        })?;

        Ok(s3_path.to_string())
    }

    pub async fn ingest_avro(
        &self,
        token: &BearerToken,
        s3_path: &str,
        data_source_rid: ResourceIdentifier,
    ) -> Result<IngestResponse, UploaderError> {
        let opts = IngestOptions::AvroStream(
            AvroStreamOpts::builder()
                .source(IngestSource::S3(S3IngestSource::new(s3_path)))
                .target(DatasetIngestTarget::Existing(
                    ExistingDatasetIngestDestination::new(data_source_rid),
                ))
                .build(),
        );

        let request = IngestRequest::new(opts);

        self.ingest_client
            .ingest(token, &request)
            .await
            .map_err(|e| UploaderError::Conjure(format!("{e:?}")))
    }
}

pub struct ChunkedStreamReader {
    reader: Box<dyn Read + Send>,
    chunk_size: usize,
    current_index: usize,
}

impl ChunkedStreamReader {
    pub fn new<R>(reader: R, chunk_size: usize) -> Self
    where
        R: Read + Send + 'static,
    {
        Self {
            reader: Box::new(reader),
            chunk_size,
            current_index: 0,
        }
    }
}

impl Stream for ChunkedStreamReader {
    type Item = Result<(usize, Vec<u8>), std::io::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut buffer = vec![0u8; self.chunk_size];

        match self.reader.read(&mut buffer) {
            Ok(0) => std::task::Poll::Ready(None),
            Ok(n) => {
                buffer.truncate(n);
                let index = self.current_index;
                self.current_index += 1;
                std::task::Poll::Ready(Some(Ok((index, buffer))))
            }
            Err(e) => std::task::Poll::Ready(Some(Err(e))),
        }
    }
}
