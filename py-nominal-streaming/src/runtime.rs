//! Helpers for managing the tokio runtime that backs a stream's uploader for the lifespan of a
//! stream used by python.
//!
//! Note that data does *not* travel through this runtime. `NominalDatasetStream::enqueue` is
//! synchronous -- a buffer lock and a `Vec` push -- so python threads call it directly and the
//! runtime is only ever used by the uploader to issue requests.

use std::sync::Arc;
use std::thread::JoinHandle;
use std::thread::{self};

use anyhow::anyhow;
use anyhow::Result;
use nominal_streaming::prelude::NominalDatasetStream;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tracing::error;
use tracing::info;

use crate::lazy_dataset_stream_builder::LazyDatasetStreamBuilder;

/// The parts of a running stream that the python-facing layer needs to interact with.
pub struct StreamRuntime {
    /// The underlying stream. Points are pushed straight into this from whichever python thread
    /// called `enqueue`.
    pub stream: Arc<NominalDatasetStream>,
    /// Fire to let the runtime thread drop its tokio runtime and exit.
    ///
    /// Must not be fired until `stream` has been dropped: the uploader issues its requests on that
    /// runtime, including the ones that drain the final buffers.
    pub shutdown_tx: oneshot::Sender<()>,
}

/// Start a background thread owning a tokio runtime for the stream's uploader, build the stream on
/// that runtime, and hand the stream back to the caller.
pub fn spawn_runtime_worker(
    builder: LazyDatasetStreamBuilder,
) -> Result<(JoinHandle<()>, StreamRuntime)> {
    let (rt_info_tx, rt_info_rx) = crossbeam_channel::bounded::<Result<StreamRuntime>>(1);

    let num_workers = builder
        .opts
        .as_ref()
        .map(|o| o.num_runtime_workers)
        .unwrap_or_else(|| thread::available_parallelism().unwrap().get());

    let join = thread::spawn(move || {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .thread_name("nominal-stream-runtime")
            .worker_threads(num_workers)
            .build()
            .expect("tokio runtime failed to initialize");

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // Build the stream on this runtime's handle and hand it to the caller. This thread keeps no
        // reference of its own, so the caller's drop is what drains the stream.
        match builder.build(runtime.handle().clone()) {
            Ok(stream) => {
                let _ = rt_info_tx.send(Ok(StreamRuntime {
                    stream: Arc::new(stream),
                    shutdown_tx,
                }));
            }
            Err(e) => {
                error!("Failed to start underlying stream: {e}");
                let _ = rt_info_tx.send(Err(e));
                return;
            }
        }

        // Hold the runtime open until close() says the uploader is finished with it.
        info!("Runtime thread parked awaiting shutdown");
        runtime.block_on(async move {
            let _ = shutdown_rx.await;
        });
        info!("Runtime thread shutting down");
    });

    let rt_info = rt_info_rx
        .recv()
        .map_err(|_| anyhow!("Failed to init runtime"))??;

    info!("Runtime worker successfully started");
    Ok((join, rt_info))
}
