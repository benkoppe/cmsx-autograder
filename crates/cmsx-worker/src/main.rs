mod auth;
mod capacity;
mod client;
mod config;
mod executor;
mod job_runner;
mod worker;
mod workspace;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = config::WorkerConfig::load()?;
    worker::run(config).await
}
