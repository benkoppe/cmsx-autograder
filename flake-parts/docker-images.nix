{ lib, ... }:
{
  perSystem =
    { pkgs, ... }:
    let
      python = pkgs.python314;

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

            pkgs.bashInteractive
            pkgs.coreutils
            pkgs.findutils
            pkgs.gawk
            pkgs.gnugrep
            pkgs.gnused
            pkgs.gnutar
            pkgs.gzip
            pkgs.which

            pkgs.cacert
            pkgs.dockerTools.binSh
            pkgs.dockerTools.usrBinEnv
          ]
          ++ extraContents;

          path = lib.makeBinPath contents;

          baseConfig = {
            Env = [
              "PATH=${path}"
              "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
              "CMSX_INPUT_DIR=/input"
              "CMSX_WORK_DIR=/work"
              "CMSX_OUTPUT_DIR=/output"
            ]
            ++ extraEnv;

            WorkingDir = "/work";

            Cmd = [
              "python"
              "-m"
              "cmsx_autograder"
              "/grader/grade.py"
            ];
          };
        in
        pkgs.dockerTools.buildLayeredImage {
          inherit name tag contents;

          maxLayers = 120;

          config = baseConfig // extraConfig;
        };

      cmsx-runner-python = mkRunnerPython (_: [ ]);

      cmsx-runner-python-image = mkRunnerImage {
        name = "cmsx-runner-python";
        runnerPython = cmsx-runner-python;
      };

      load-cmsx-runner-python = pkgs.writeShellApplication {
        name = "load-cmsx-runner-python";
        runtimeInputs = [
          pkgs.docker
        ];
        text = ''
          docker load < ${cmsx-runner-python-image}
        '';
      };

      linuxPackages = lib.optionalAttrs pkgs.stdenv.isLinux {
        inherit cmsx-autograder-python;
        inherit cmsx-runner-python;
        inherit cmsx-runner-python-image;
      };

      linuxApps = lib.optionalAttrs pkgs.stdenv.isLinux {
        load-cmsx-runner-python = {
          type = "app";
          program = lib.getExe load-cmsx-runner-python;
        };
      };

      linuxChecks = lib.optionalAttrs pkgs.stdenv.isLinux {
        inherit cmsx-runner-python-image;
      };
    in
    {
      packages = linuxPackages;
      apps = linuxApps;
      checks = linuxChecks;
    };
}
