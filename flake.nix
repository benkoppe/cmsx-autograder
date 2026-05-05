{
  description = "CMSX autograder";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";

    devenv.url = "github:cachix/devenv";
    devenv-root = {
      url = "file+file:///dev/null";
      flake = false;
    };

    pre-commit-hooks = {
      url = "github:cachix/pre-commit-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];

      imports = [
        inputs.pre-commit-hooks.flakeModule
        inputs.devenv.flakeModule
      ];

      perSystem = {
        devenv.shells.default =
          { config, ... }:
          {
            languages = {
              rust.enable = true;
              python.enable = true;
            };

            outputs = {
              cmsx-control-plane = config.languages.rust.import ./crates/cmsx-control-plane { };
            };

            services = {
              postgres = {
                enable = true;
                initialDatabases = [ { name = "cmsx"; } ];
              };
            };

            enterShell = ''
              export DATABASE_URL="postgresql:///cmsx?host=$PGHOST"
            '';
          };
      };
    };
}
