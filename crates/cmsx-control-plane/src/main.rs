mod app;
mod config;
mod db;
mod error;
mod routes;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = config::Config::from_env()?;
    let db = db::connect(&config.database_url).await?;
    let state = app::AppState { db };
    let app = app::router(state);

    tracing::info!("listening on http://{}", config.bind_addr);

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
