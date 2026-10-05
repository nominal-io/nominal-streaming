use std::path::PathBuf;

use anyhow::anyhow;
use anyhow::Result;
use nominal_streaming::prelude::*;
use nominal_streaming::stream::NominalDatasetStreamBuilder;

use crate::nominal_stream_opts::PyNominalStreamOpts;

#[derive(Clone)]
pub struct CoreTarget {
    pub token: BearerToken,
    pub rid: ResourceIdentifier,
}

#[derive(Clone)]
pub struct FileTarget {
    pub path: PathBuf,
    pub overwrite: bool,
}

#[derive(Clone, Default)]
pub struct StreamTargets {
    pub core_target: Option<CoreTarget>,
    pub file_target: Option<FileTarget>,
    pub file_fallback: Option<FileTarget>,
}

/// Simple wrapper around a NominalDatasetStreamBuilder that allows for lazily configuring
/// streaming to core vs. files without a tokio runtime readily available.
#[derive(Clone, Default)]
pub struct LazyDatasetStreamBuilder {
    pub log_level: Option<String>,
    pub opts: Option<PyNominalStreamOpts>,
    pub targets: StreamTargets,
}

impl LazyDatasetStreamBuilder {
    pub fn validate(&self) -> Result<()> {
        if self.targets.core_target.is_none() && self.targets.file_target.is_none() {
            return Err(anyhow!(
                "no streaming target configured; call with_core_consumer(...) or to_file(...)"
            ));
        } else if self.targets.file_target.is_some() && self.targets.file_fallback.is_some() {
            return Err(anyhow!("file output and file fallback cannot be combined"));
        } else if let Some(opts) = self.opts.clone() {
            if opts.num_runtime_workers < opts.inner.request_dispatcher_tasks {
                return Err(
                    anyhow!(
                        "Number of runtime workers must be at least as large as the number of request dispatcher tasks ({} < {})", 
                        opts.num_runtime_workers,
                        opts.inner.request_dispatcher_tasks,
                    )
                );
            }
        }

        Ok(())
    }

    pub fn build(&self, handle: tokio::runtime::Handle) -> Result<NominalDatasetStream> {
        let mut builder = NominalDatasetStreamBuilder::new();

        if let Some(core_target) = &self.targets.core_target {
            builder =
                builder.stream_to_core(core_target.token.clone(), core_target.rid.clone(), handle);
        }

        if let Some(file_target) = &self.targets.file_target {
            builder =
                builder.stream_to_file_overwrite(file_target.path.clone(), file_target.overwrite);
        }

        if let Some(file_fallback) = &self.targets.file_fallback {
            builder =
                builder.with_file_fallback_overwrite(&file_fallback.path, file_fallback.overwrite);
        }

        if let Some(ref log_level) = &self.log_level {
            builder = builder.enable_logging_with_directive(log_level);
        }

        if let Some(ref opts) = &self.opts {
            builder = builder.with_options(opts.inner.clone());
        }

        builder.try_build().map_err(Into::into)
    }
}
