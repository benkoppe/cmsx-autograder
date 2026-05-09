use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn admin_generate_token_does_not_require_config() {
    let mut command = Command::cargo_bin("cmsx-control-plane")
        .expect("cmsx-control-plane binary should be available");

    command
        .env("CMSX_CONFIG", "/definitely/not/a/real/cmsx.toml")
        .args(["admin", "generate-token"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Admin token:"))
        .stdout(predicate::str::contains("cmsx_admin_"))
        .stdout(predicate::str::contains("Config hash:"))
        .stdout(predicate::str::contains("$argon2"))
        .stdout(predicate::str::contains("bootstrap_token_hashes"));
}

#[test]
fn admin_hash_token_does_not_require_config() {
    let mut command = Command::cargo_bin("cmsx-control-plane")
        .expect("cmsx-control-plane binary should be available");

    command
        .env("CMSX_CONFIG", "/definitely/not/a/real/cmsx.toml")
        .args(["admin", "hash-token", "cmsx_admin_known_test_token"])
        .assert()
        .success()
        .stdout(predicate::str::contains("$argon2"));
}
