{ inputs, ... }:
{
  perSystem = _: {
    process-compose.services = {
      imports = [ inputs.services-flake.processComposeModules.default ];

      services.postgres.cmsx = {
        enable = true;
        initialDatabases = [
          { name = "cmsx"; }
        ];
      };
    };
  };
}
