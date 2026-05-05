{ inputs, lib, ... }:
{
  perSystem =
    {
      config,
      pkgs,
      self',
      ...
    }:
    let
      craneLib = inputs.crane.mkLib pkgs;

      rustDevPackages = [
        pkgs.bacon
        pkgs.cargo-audit
        pkgs.cargo-nextest
        pkgs.cargo-udeps
        pkgs.clippy
        pkgs.cmake
        pkgs.rust-analyzer
        pkgs.rustc
        pkgs.rustfmt
      ];

      pythonDevPackages = [
        pkgs.python314
        pkgs.uv
      ];

      dataDevPackages = [
        pkgs.postgresql
        pkgs.sqlx-cli
      ];

      nixDevPackages = [
        pkgs.nixfmt
      ];

      platformPackages = lib.optionals pkgs.stdenv.isDarwin [
        pkgs.libiconv
      ];

      devPackages =
        rustDevPackages
        ++ pythonDevPackages
        ++ dataDevPackages
        ++ nixDevPackages
        ++ platformPackages
        ++ [ self'.packages.services ];
    in
    {
      devShells.default = craneLib.devShell {
        inherit (config) checks;
        packages = devPackages;

        shellHook = ''
          ${config.pre-commit.installationScript}

          export CMSX_DATABASE_URL="${
            config.process-compose.services.services.postgres.cmsx.connectionURI { dbName = "cmsx"; }
          }"
          # sqlx compile-time
          export DATABASE_URL="$CMSX_DATABASE_URL"
        '';
      };
    };
}
