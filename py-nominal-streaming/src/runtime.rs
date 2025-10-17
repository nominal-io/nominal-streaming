//! Runtime machinery: dedicated Tokio runtime thread + async worker,
//! and a thin std-thread bridge from crossbeam (sync) → tokio mpsc (async).

use std::thread::JoinHandle;
use std::thread::{self};

use anyhow::anyhow;
use anyhow::Result;
use crossbeam_channel;
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::builder_state::build_stream;
use crate::builder_state::BuilderState;
use crate::point::EnqueueItem;

/// Struct encompassing all of the components of the runtime that the python-facing layer needs to interact with
pub struct StreamRuntime {
    /// Tokio runtime handle to spawn / drive async work
    pub runtime_handle: tokio::runtime::Handle,
    /// Cancellation token to signal immediate teardown within workers
    // TODO(drake): flush through to underlying NominalDatasetStream for instantaneous exit
    pub cancel_token: CancellationToken,
    /// Async queue for ingesting data into the runtime.
    /// Data from Python enters this queue, where the runtime worker then forwards the data into the underlying NominalDatasetStream
    pub ingest_tx: mpsc::Sender<EnqueueItem>,
    /// Async receiver for when the async worker has fully drained and exited
    pub runtime_exited_rx: oneshot::Receiver<()>,
}

/// Create a background thread that manages a tokio runtime and in a tight loop pulls
/// incoming data from a queue and forwards into the underlying rust streaming client
pub fn spawn_runtime_worker(state: BuilderState) -> Result<(JoinHandle<()>, StreamRuntime)> {
    let (rt_info_tx, rt_info_rx) = crossbeam_channel::bounded::<StreamRuntime>(1);

    let join = thread::spawn(move || {
        let num_workers = state
            .opts
            .as_ref()
            .map(|o| o.runtime_workers)
            .unwrap_or(thread::available_parallelism().unwrap().get());
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .thread_name("nominal-stream-runtime")
            .worker_threads(num_workers)
            .build()
            .expect("tokio runtime failed to initialize");

        // TODO(drake): flush configuration through
        // let async_cap = state
        //     .opts
        //     .as_ref()
        //     .map(|o| o.async_buffer_cap())
        //     .unwrap_or(4);
        let async_cap = 4;

        // IMPORTANT: clone the handle *before* entering block_on
        let runtime_handle = runtime.handle().clone();
        let cancel_token = CancellationToken::new();

        // Channel to communicate items to be enqueued into Rust from Python
        let (ingest_tx, mut ingest_rx) = mpsc::channel::<EnqueueItem>(async_cap);

        // Channel to communicate that the async worker has completed draining the queue & shutdown
        let (runtime_exited_tx, runtime_exited_rx) = oneshot::channel::<()>();

        // Main runtime loop
        runtime.block_on(async move {
            // Token that allows for cancelling the bridge process
            // If the parent token gets cancelled, this worker will cancel too.
            let cancel_child = cancel_token.child_token();

            // Attempt to build the stream using the *cloned* handle.
            let maybe_stream = build_stream(&state, runtime_handle.clone());

            // Spawn the ingest/forwarder task on this runtime.
            let worker = tokio::spawn(async move {
                if let Ok(stream) = maybe_stream {
                    loop {
                        tokio::select! {
                            _ = cancel_child.cancelled() => {
                                info!("Cancellation token received! Stopping runtime...");
                                break
                            },
                            maybe_item = ingest_rx.recv() => {
                                match maybe_item {
                                    Some(EnqueueItem::Doubles { ch, points }) => { stream.enqueue(&ch, points); }
                                    Some(EnqueueItem::Ints    { ch, points }) => { stream.enqueue(&ch, points); }
                                    Some(EnqueueItem::Strings { ch, points }) => { stream.enqueue(&ch, points); }
                                    None => {
                                        info!("Empty enqueue item received! Stopping runtime...");
                                        break;
                                    },
                                }
                            }
                        }
                    }
                    info!("Done forwarding points from python! Awaiting underlying stream teardown...");
                }

                // TODO (drake): consider adding error handling around send() call
                info!("Signalling runtime exit");
                let _ = runtime_exited_tx.send(());
                Ok::<(), anyhow::Error>(())
            });

            // Send runtime parts back to the creator. Use `handle.clone()` here,
            // NOT `rt.handle()`, so the future doesn't capture `rt`.
            rt_info_tx
                .send(StreamRuntime {
                    runtime_handle: runtime_handle,
                    cancel_token: cancel_token,
                    ingest_tx: ingest_tx,
                    runtime_exited_rx: runtime_exited_rx,
                })
                .ok();

            let _ = worker.await;
        });
    });

    // Receive information about runtime once initialization has finished
    let rt_info = rt_info_rx
        .recv()
        .map_err(|_| anyhow!("Failed to init runtime"))?;

    info!("Runtime worker successfully started");
    Ok((join, rt_info))
}
