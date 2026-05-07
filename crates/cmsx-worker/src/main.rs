#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = cmsx_worker::config::WorkerConfig::load()?;
    cmsx_worker::worker::run(config).await
}
