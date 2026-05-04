{ inputs, lib, ... }:
{
  perSystem =
    { config, pkgs, ... }:
    let
      craneLib = inputs.crane.mkLib pkgs;

      rustDevPackages = [
        pkgs.bacon
        pkgs.cargo-audit
        pkgs.cargo-nextest
        pkgs.cargo-udeps
        pkgs.clippy
        pkgs.rust-analyzer
        pkgs.rustc
        pkgs.rustfmt
      ];

      pythonDevPackages = [
        pkgs.python314
        pkgs.uv
      ];

      dataDevPackages = [
        pkgs.sqlite
        pkgs.sqlx-cli
      ];

      nixDevPackages = [
        pkgs.nixfmt
      ];

      platformPackages = lib.optionals pkgs.stdenv.isDarwin [
        pkgs.libiconv
      ];

      devPackages =
        rustDevPackages ++ pythonDevPackages ++ dataDevPackages ++ nixDevPackages ++ platformPackages;
    in
    {
      devShells.default = craneLib.devShell {
        inherit (config) checks;
        packages = devPackages;

        shellHook = ''
          ${config.pre-commit.installationScript}
        '';
      };
    };
}
