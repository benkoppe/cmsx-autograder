{ inputs, lib, ... }:
{
  perSystem =
    { pkgs, self', ... }:
    let
      craneLib = inputs.crane.mkLib pkgs;
      unfilteredRoot = ../.;

      cargoSrc = lib.fileset.toSource {
        root = unfilteredRoot;
        fileset = craneLib.fileset.commonCargoSources unfilteredRoot;
      };

      workspaceSrc = lib.fileset.toSource {
        root = unfilteredRoot;
        fileset = lib.fileset.unions [
          (craneLib.fileset.commonCargoSources unfilteredRoot)
          (lib.fileset.maybeMissing ../migrations)
          (lib.fileset.maybeMissing ../.sqlx)
        ];
      };

      commonBuildArgs = {
        strictDeps = true;

        nativeBuildInputs = [
          pkgs.pkg-config
          pkgs.postgresql
        ]
        ++ lib.optionals pkgs.stdenv.isDarwin [
          pkgs.libiconv
        ];
      };

      cargoArtifacts = craneLib.buildDepsOnly (commonBuildArgs // { src = cargoSrc; });

      workspaceBuildArgs = commonBuildArgs // {
        src = workspaceSrc;
        inherit cargoArtifacts;
        doCheck = false;
      };

      mkWorkspacePackage =
        pname:
        craneLib.buildPackage (
          workspaceBuildArgs
          // {
            inherit pname;
            cargoExtraArgs = "-p ${pname}";
            meta.mainProgram = pname;
          }
        );

      packages = rec {
        default = cmsx-control-plane;
        cmsx-control-plane = mkWorkspacePackage "cmsx-control-plane";
      };

      checks = {
        clippy = craneLib.cargoClippy (
          commonBuildArgs
          // {
            src = workspaceSrc;
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- --deny warnings";
          }
        );

        rust-fmt = craneLib.cargoFmt {
          src = workspaceSrc;
        };

        rust-tests = craneLib.cargoNextest (
          commonBuildArgs
          // {
            src = workspaceSrc;
            inherit cargoArtifacts;
            partitions = 1;
            partitionType = "count";
            cargoNextestPartitionsExtraArgs = "--no-tests=pass";
          }
        );

        cargo-doc = craneLib.cargoDoc (
          commonBuildArgs
          // {
            src = workspaceSrc;
            inherit cargoArtifacts;
            env.RUSTDOCFLAGS = "--deny warnings";
          }
        );
      };
    in
    {
      inherit packages checks;

      apps = {
        cmsx-control-plane = {
          type = "app";
          program = lib.getExe self'.packages.cmsx-control-plane;
        };

        default = self'.apps.cmsx-control-plane;
      };
    };
}
