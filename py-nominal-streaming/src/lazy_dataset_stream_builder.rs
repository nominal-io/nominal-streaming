use std::path::PathBuf;

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
}

#[derive(Clone, Default)]
pub struct StreamTargets {
    pub core_target: Option<CoreTarget>,
    pub file_target: Option<FileTarget>,
    pub file_fallback: Option<PathBuf>,
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
    pub fn build(
        &self,
        handle: tokio::runtime::Handle,
    ) -> Result<NominalDatasetStream, StreamBuildError> {
        let mut builder = NominalDatasetStreamBuilder::new();

        if let Some(core_target) = &self.targets.core_target {
            builder =
                builder.stream_to_core(core_target.token.clone(), core_target.rid.clone(), handle);
        }

        if let Some(file_target) = &self.targets.file_target {
            builder = builder.stream_to_file(file_target.path.clone());
        }

        if let Some(file_fallback) = &self.targets.file_fallback {
            builder = builder.with_file_fallback(file_fallback);
        }

        if let Some(log_level) = &self.log_level {
            builder = builder.enable_logging_with_directive(log_level);
        }

        if let Some(opts) = &self.opts {
            builder = builder.with_options(opts.inner.clone());
        }

        builder.try_build()
    }
}
