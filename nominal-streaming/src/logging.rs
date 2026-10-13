//! Shared tracing setup for stream builders.

pub(crate) fn init(directive: Option<&str>) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    // Build the filter, either from an explicit directive or the environment.
    let base = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::DEBUG.into());
    let env_filter = match directive {
        Some(d) => base.parse_lossy(d),
        None => base.from_env_lossy(),
    };

    let subscriber = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_line_number(true),
        )
        .with(env_filter);

    if let Err(error) = subscriber.try_init() {
        eprintln!("nominal streaming failed to enable logging: {error}");
    }
}
