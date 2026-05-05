mod app;
mod config;
mod db;
mod error;
mod routes;
mod storage;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = config::Config::load()?;
    let db = db::connect(&config.database_url).await?;
    let storage = storage::Storage::from_config(&config.storage)?;

    let state = app::AppState {
        db,
        storage,
        cmsx: config.cmsx.clone(),
    };
    let app = app::router(state);

    tracing::info!("listening on http://{}", config.bind_addr);

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
