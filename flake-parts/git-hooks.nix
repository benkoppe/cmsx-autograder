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
        };
      };
    };
  };
}
