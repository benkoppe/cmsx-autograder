{ lib, flake-parts-lib, ... }:
let
  inherit (flake-parts-lib) mkPerSystemOption;
  inherit (lib) mkOption types;
in
{
  options.perSystem = mkPerSystemOption (_: {
    options.cmsx-db = {
      installationScript = mkOption {
        type = types.lines;
        readOnly = true;
        description = "Shell code that configures the CMSX autograder DB development environment.";
      };
      devShellPackages = mkOption {
        type = types.listOf types.package;
        readOnly = true;
        description = "Helper commands for working with the CMSX autograder DB.";
      };
    };
  });
  config.perSystem =
    { pkgs, ... }:
    let
      cmsxDbInstallationScript = ''
        repo_root="$(${lib.getExe pkgs.git} rev-parse --show-toplevel 2>/dev/null || pwd)"
        export REPO_ROOT="$repo_root"
        export CMSX_DB_DIR="$REPO_ROOT/dev"
        export CMSX_DB_PATH="$CMSX_DB_DIR/cmsx-autograder.db"
        export CMSX_DB_MIGRATIONS_DIR="$REPO_ROOT/migrations"
        export DATABASE_URL="sqlite://$CMSX_DB_PATH"
        mkdir -p "$CMSX_DB_DIR"
      '';
      mkCmsxDbCommand =
        {
          name,
          body,
        }:
        pkgs.writeShellApplication {
          inherit name;
          runtimeInputs = [
            pkgs.coreutils
            pkgs.git
            pkgs.sqlx-cli
          ];
          text = ''
            set -euo pipefail
            ${cmsxDbInstallationScript}
            ${body}
          '';
        };
      cmsxDbCreate = mkCmsxDbCommand {
        name = "cmsx-db-create";
        body = ''
          if [ -f "$CMSX_DB_PATH" ]; then
            exit 0
          fi
          exec sqlx database create "$@"
        '';
      };
      cmsxDbMigrate = mkCmsxDbCommand {
        name = "cmsx-db-migrate";
        body = ''
          if [ ! -f "$CMSX_DB_PATH" ]; then
            sqlx database create
          fi
          exec sqlx migrate run --source "$CMSX_DB_MIGRATIONS_DIR" "$@"
        '';
      };
      cmsxDbInfo = mkCmsxDbCommand {
        name = "cmsx-db-info";
        body = ''
          exec sqlx migrate info --source "$CMSX_DB_MIGRATIONS_DIR" "$@"
        '';
      };
      cmsxDbAdd = mkCmsxDbCommand {
        name = "cmsx-db-add";
        body = ''
          if [ "$#" -eq 0 ]; then
            printf '%s\n' "usage: cmsx-db-add <migration-name>" >&2
            exit 1
          fi
          exec sqlx migrate add --source "$CMSX_DB_MIGRATIONS_DIR" "$@"
        '';
      };
      cmsxDbReset = mkCmsxDbCommand {
        name = "cmsx-db-reset";
        body = ''
          rm -f "$CMSX_DB_PATH" "$CMSX_DB_PATH-wal" "$CMSX_DB_PATH-shm"
          sqlx database create
          exec sqlx migrate run --source "$CMSX_DB_MIGRATIONS_DIR" "$@"
        '';
      };
      cmsxSqlxPrepare = mkCmsxDbCommand {
        name = "cmsx-sqlx-prepare";
        body = ''
          exec cargo sqlx prepare --workspace -- --all-targets "$@"
        '';
      };
    in
    {
      packages = {
        cmsx-db-create = cmsxDbCreate;
        cmsx-db-migrate = cmsxDbMigrate;
        cmsx-db-info = cmsxDbInfo;
        cmsx-db-add = cmsxDbAdd;
        cmsx-db-reset = cmsxDbReset;
        cmsx-sqlx-prepare = cmsxSqlxPrepare;
      };
      cmsx-db = {
        installationScript = cmsxDbInstallationScript;
        devShellPackages = [
          cmsxDbCreate
          cmsxDbMigrate
          cmsxDbInfo
          cmsxDbAdd
          cmsxDbReset
          cmsxSqlxPrepare
        ];
      };
    };
}
