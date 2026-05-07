{ inputs, lib, ... }:
{
  perSystem =
    { pkgs, self', ... }:
    let
      craneLib = inputs.crane.mkLib pkgs;
      unfilteredRoot = ../.;

      cargoFileset = craneLib.fileset.commonCargoSources unfilteredRoot;

      workspaceFileset = lib.fileset.unions [
        cargoFileset
        (lib.fileset.maybeMissing ../migrations)
        (lib.fileset.maybeMissing ../.sqlx)
      ];

      workspaceTestFileset = lib.fileset.unions [
        workspaceFileset
        (lib.fileset.maybeMissing ../python/sdk/src)
      ];

      cargoSrc = lib.fileset.toSource {
        root = unfilteredRoot;
        fileset = cargoFileset;
      };

      workspaceSrc = lib.fileset.toSource {
        root = unfilteredRoot;
        fileset = workspaceFileset;
      };

      workspaceTestSrc = lib.fileset.toSource {
        root = unfilteredRoot;
        fileset = workspaceTestFileset;
      };

      commonBuildArgs = {
        strictDeps = true;

        env.SQLX_OFFLINE = "true";

        nativeBuildInputs = [
          pkgs.cmake
          pkgs.git
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
            src = workspaceTestSrc;
            inherit cargoArtifacts;
            nativeBuildInputs = commonBuildArgs.nativeBuildInputs ++ [
              pkgs.python314
              pkgs.postgresql
            ];

            preCheck = ''
              export PGDATA="$TMPDIR/postgres"
              export PGHOST="$TMPDIR/postgres-socket"
              export PGPORT="5432"
              mkdir -p "$PGHOST"

              initdb "$PGDATA" --auth=trust --no-locale
              pg_ctl \
                -D "$PGDATA" \
                -o "-k $PGHOST -p $PGPORT" \
                -l "$TMPDIR/postgres.log" \
                start

              export CMSX_DATABASE_URL="postgresql://localhost/cmsx?host=$PGHOST&port=$PGPORT"
            '';
            postCheck = ''
              pg_ctl -D "$PGDATA" stop || true
            '';

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
