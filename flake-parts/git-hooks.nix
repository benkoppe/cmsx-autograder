_: {
  perSystem = _: {
    pre-commit = {
      check.enable = false;

      settings = {
        src = ../.;
        default_stages = [ "pre-push" ];

        hooks = {
          nixfmt.enable = true;
          rustfmt.enable = true;

          sqlx-prepare = {
            enable = false;
            entry = "cargo sqlx prepare --workspace --check -- --all-targets";
            pass_filenames = false;
          };
        };
      };
    };
  };
}
