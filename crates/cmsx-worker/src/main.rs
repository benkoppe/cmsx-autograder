mod auth;
mod capacity;
mod client;
mod config;
mod worker;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = config::WorkerConfig::load()?;
    worker::run(config).await
}
