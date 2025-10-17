//! Builder state + constraint validation + stream construction.
//!
//! Constraints enforced here (before starting the runtime):
//!   - Exactly one primary target must be set: Core or File
////!   - with_file_fallback() is only valid when the primary target is Core

use crate::nominal_stream_opts::NominalStreamOptsWrapper;
use anyhow::{anyhow, Result};
use nominal_streaming::prelude::*;
use nominal_streaming::stream::NominalDatasetStreamBuilder;
use std::path::PathBuf;

/// Primary streaming target (mutually exclusive)
#[derive(Clone)]
pub enum Target {
    Core {
        token: BearerToken,
        rid: ResourceIdentifier,
        file_fallback: Option<PathBuf>,
    },
    File {
        path: PathBuf,
    },
}

#[derive(Clone, Default)]
pub struct BuilderState {
    pub logging: bool,
    pub opts: Option<NominalStreamOptsWrapper>,
    pub target: Option<Target>,
}

impl BuilderState {
    pub fn validate(&self) -> Result<()> {
        match &self.target {
            None => Err(anyhow!(
                "no streaming target configured; call with_core_consumer(...) or to_file(...)"
            )),
            _ => Ok(()),
        }
    }
}

/// Apply state to `NominalDatasetStreamBuilder` and build.
///
/// NOTE: This returns a `NominalDatasetStream`.
/// If your `to_file(...)` target compiles to a *different* consumer type in your
/// crate, you can:
///   - move this building logic into the runtime worker and match on `Target` there,
///     or
///   - introduce a small enum wrapper around both concrete stream types with a
///     common `enqueue` method. (The current project has worked with a unified
///     type in earlier iterations; keep this as-is if that holds.)
pub fn build_stream(
    state: &BuilderState,
    handle: tokio::runtime::Handle,
) -> Result<NominalDatasetStream> {
    let mut builder = match &state.target {
        Some(Target::Core {
            token,
            rid,
            file_fallback,
        }) => {
            let builder = NominalDatasetStreamBuilder::new().stream_to_core(
                token.clone(),
                rid.clone(),
                handle,
            );
            match file_fallback.as_ref() {
                Some(file_path) => builder.with_file_fallback(file_path),
                None => builder,
            }
        }
        Some(Target::File { path }) => {
            NominalDatasetStreamBuilder::new().stream_to_file(path.clone())
        }
        _ => {
            return Err(anyhow!(
                "No target defined for streaming! Can't build stream"
            ))
        }
    };

    if state.logging {
        builder = builder.enable_logging();
    }

    if let Some(ref opts) = state.opts {
        builder = builder.with_options(opts.inner.clone());
    }

    Ok(builder.build())
}
