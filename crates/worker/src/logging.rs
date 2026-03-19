use garage_queue_lib::{LogFormat, LogLevel};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

pub fn init_logging(level: LogLevel, format: LogFormat) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(level.to_string()));

    match format {
        LogFormat::Text => tracing_subscriber::registry()
            .with(fmt::layer().with_filter(filter))
            .init(),
        LogFormat::Json => tracing_subscriber::registry()
            .with(fmt::layer().json().with_filter(filter))
            .init(),
    }
}
