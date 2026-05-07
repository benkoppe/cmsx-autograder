{ lib, inputs, ... }:
{
  perSystem =
    {
      pkgs,
      system,
      inputs',
      ...
    }:
    let
      targetSystem =
        {
          x86_64-darwin = "x86_64-linux";
          aarch64-darwin = "aarch64-linux";
        }
        .${system} or system;

      imagePkgs = import inputs.nixpkgs {
        system = targetSystem;
      };

      inherit (inputs.nix2container.packages.${targetSystem}) nix2container;

      python = imagePkgs.python314;

      sdkSrc = lib.fileset.toSource {
        root = ../python/sdk;
        fileset = lib.fileset.unions [
          ../python/sdk/pyproject.toml
          ../python/sdk/src
        ];
      };

      cmsx-autograder-python = python.pkgs.buildPythonPackage {
        pname = "cmsx-autograder";
        version = "0.1.0";
        pyproject = true;

        src = sdkSrc;

        build-system = [
          python.pkgs.hatchling
        ];

        dependencies = [ ];

        pythonImportsCheck = [
          "cmsx_autograder"
        ];

        doCheck = false;
      };

      mkRunnerPython =
        extraPythonPackages:
        python.withPackages (
          ps:
          [
            cmsx-autograder-python
          ]
          ++ extraPythonPackages ps
        );

      mkRunnerImage =
        {
          name,
          tag ? "latest",
          runnerPython,
          extraContents ? [ ],
          extraEnv ? [ ],
          extraConfig ? { },
        }:
        let
          contents = [
            runnerPython

            imagePkgs.bashInteractive
            imagePkgs.coreutils
            imagePkgs.findutils
            imagePkgs.gawk
            imagePkgs.gnugrep
            imagePkgs.gnused
            imagePkgs.gnutar
            imagePkgs.gzip
            imagePkgs.which

            imagePkgs.cacert
            imagePkgs.dockerTools.binSh
            imagePkgs.dockerTools.usrBinEnv
          ]
          ++ extraContents;

          root = imagePkgs.buildEnv {
            name = "${name}-root";
            paths = contents;
            pathsToLink = [
              "/bin"
              "/etc"
              "/usr/bin"
            ];
          };

          baseConfig = {
            env = [
              "PATH=/bin:/usr/bin"
              "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
              "CMSX_INPUT_DIR=/input"
              "CMSX_WORK_DIR=/work"
              "CMSX_OUTPUT_DIR=/output"
            ]
            ++ extraEnv;

            workingdir = "/work";

            cmd = [
              "python"
              "-m"
              "cmsx_autograder"
              "/grader/grade.py"
            ];
          };

          dependencyLayers = [
            (nix2container.buildLayer {
              deps = [ runnerPython ];
              reproducible = false;
            })

            (nix2container.buildLayer {
              deps = [
                imagePkgs.bashInteractive
                imagePkgs.coreutils
              ];
              reproducible = false;
            })

            (nix2container.buildLayer {
              deps = [
                imagePkgs.findutils
                imagePkgs.gawk
                imagePkgs.gnugrep
                imagePkgs.gnused
                imagePkgs.gnutar
                imagePkgs.gzip
                imagePkgs.which
                imagePkgs.cacert
              ];
              reproducible = false;
            })
          ];

          rootLayer = nix2container.buildLayer {
            copyToRoot = root;
            layers = dependencyLayers;
            reproducible = false;
          };
        in
        nix2container.buildImage {
          inherit name tag;

          layers = [ rootLayer ];
          created = "1970-01-01T00:00:01Z";

          config = baseConfig // extraConfig;
        };

      cmsx-runner-python = mkRunnerPython (_: [ ]);

      cmsx-runner-python-image = mkRunnerImage {
        name = "cmsx-runner-python";
        runnerPython = cmsx-runner-python;
      };
    in
    {
      packages = {
        inherit cmsx-autograder-python;
        inherit cmsx-runner-python;
        inherit cmsx-runner-python-image;
      };

      apps = {
        load-cmsx-runner-python = {
          type = "app";
          program = lib.getExe (
            pkgs.writeShellApplication {
              name = "load-cmsx-runner-python";
              runtimeInputs = [
                inputs'.nix2container.packages.skopeo-nix2container
                pkgs.docker-client
              ];
              text = ''
                tmpdir=$(mktemp -d)
                trap 'rm -rf "$tmpdir"' EXIT
                image_ref="${cmsx-runner-python-image.imageName}:${cmsx-runner-python-image.imageTag}"
                archive="$tmpdir/image.tar"
                echo "Copy to Docker archive image $image_ref"
                skopeo --insecure-policy copy \
                  nix:${cmsx-runner-python-image} \
                  docker-archive:"$archive":$image_ref \
                  "$@"
                echo "Load Docker image $image_ref"
                docker load -i "$archive"
              '';
            }
          );
        };
      };
    };
}
