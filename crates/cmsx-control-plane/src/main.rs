mod app;
mod cli;
mod config;
mod db;
mod error;
mod routes;
mod storage;
mod workers;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    match cli::handle_cli()? {
        cli::CliAction::Serve => serve().await,
        cli::CliAction::Exit => Ok(()),
    }
}

async fn serve() -> anyhow::Result<()> {
    let config = config::Config::load()?;
    let db = db::connect(&config.database_url).await?;
    let storage = storage::Storage::from_config(&config.storage)?;

    let state = app::AppState {
        db,
        storage,
        cmsx: config.cmsx.clone(),
        admin: config.admin.clone(),
    };

    let app = app::router(state);

    tracing::info!("listening on http://{}", config.bind_addr);

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
