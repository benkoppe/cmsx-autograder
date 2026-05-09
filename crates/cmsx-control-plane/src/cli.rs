use anyhow::{Result, anyhow};
use argon2::{
    Argon2, PasswordHasher,
    password_hash::{SaltString, rand_core::OsRng},
};
use clap::{Parser, Subcommand};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "cmsx-control-plane")]
#[command(about = "CMSX autograder control plane")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Start the HTTP control-plane server.
    Serve,

    /// Run admin/bootstrap utilities.
    Admin {
        #[command(subcommand)]
        command: AdminCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum AdminCommand {
    /// Generate a new random bootstrap admin token and config hash.
    GenerateToken,
    /// Hash an existing bootstrap admin token for config.
    HashToken { token: String },
}

pub enum CliAction {
    Serve,
    Exit,
}

pub fn handle_cli() -> Result<CliAction> {
    let cli = Cli::parse();

    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => Ok(CliAction::Serve),
        Command::Admin { command } => {
            handle_admin_command(command)?;
            Ok(CliAction::Exit)
        }
    }
}

fn handle_admin_command(command: AdminCommand) -> Result<()> {
    match command {
        AdminCommand::GenerateToken => generate_admin_token(),
        AdminCommand::HashToken { token } => hash_admin_token(&token),
    }
}

fn generate_admin_token() -> Result<()> {
    let token = format!(
        "cmsx_admin_{}_{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    );

    let hash = hash_token(&token)?;

    println!("Admin token:");
    println!("{token}");
    println!();
    println!("Config hash:");
    println!("{hash}");
    println!();
    println!("Add this to cmsx.toml:");
    println!("[admin]");
    println!("public_url = \"https://autograder.example.com\"");
    println!("bootstrap_token_hashes = [");
    println!("  \"{hash}\",");
    println!("]");

    Ok(())
}

fn hash_admin_token(token: &str) -> Result<()> {
    let hash = hash_token(token)?;
    println!("{hash}");
    Ok(())
}

fn hash_token(token: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);

    let hash = Argon2::default()
        .hash_password(token.as_bytes(), &salt)
        .map_err(|error| anyhow!("failed to hash token: {error}"))?;

    Ok(hash.to_string())
}
