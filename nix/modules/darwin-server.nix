{ self }:
{ config, lib, pkgs, system, ... }:
let
  cfg = config.services.garage-queue-server;
  settingsFormat = pkgs.formats.toml { };
  integrations = import ./integrations.nix { inherit lib; };
  # Derive TOML queue config from the declarative queues option.  A single
  # integration can expand into multiple TOML queues (e.g. ollama produces
  # both {name}-generate and {name}-tags).
  queueSettings = integrations.expandQueues cfg.queues;
  mergedSettings = lib.recursiveUpdate cfg.settings (
    lib.optionalAttrs (queueSettings != { }) { queues = queueSettings; }
  );
  configFile = settingsFormat.generate "garage-queue-server.toml" mergedSettings;
in
{
  options.services.garage-queue-server = {
    enable = lib.mkEnableOption "garage-queue server";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${system}.server;
      defaultText = lib.literalExpression "garage-queue.packages.\${system}.server";
      description = "The garage-queue-server package to use.";
    };

    user = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = ''
        User account under which the daemon runs.  Null means the daemon runs
        as root (the launchd default for system daemons).  The user must
        already exist; this module does not create it.
      '';
    };

    logFile = lib.mkOption {
      type = lib.types.str;
      default = "/var/log/garage-queue-server.log";
      description = "Path for combined stdout/stderr log output.";
    };

    # The settings attrset is converted to TOML and passed to the server via
    # --config.  Its structure mirrors the server config.toml directly, so any
    # field accepted by the server can be set here.  Queue definitions should
    # use the queues option rather than settings.queues.
    settings = lib.mkOption {
      type = settingsFormat.type;
      default = { };
      description = ''
        Configuration written verbatim to the server's TOML config file.
        Mirrors the config.toml structure.  See config.example.toml in the
        source tree for the full reference.
      '';
      example = lib.literalExpression ''
        {
          server = {
            listen = "0.0.0.0:9090";
            nats_url = "nats://localhost:4222";
          };
        }
      '';
    };

    queues = lib.mkOption {
      type = lib.types.attrsOf (lib.types.submodule {
        options.integrations.ollama.enable =
          lib.mkEnableOption "standard Ollama queue configuration";
      });
      default = { };
      description = ''
        Named queue definitions.  Each queue can enable integrations that
        inject well-known configuration fragments.  The resulting queue
        config is merged on top of settings.

        The Ollama integration produces two queues from each entry:
        {name}-generate (exclusive, /api/generate) and {name}-tags
        (broadcast, /api/tags with a deduplicating combiner).
      '';
      example = lib.literalExpression ''
        { ollama.integrations.ollama.enable = true; }
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    launchd.daemons.garage-queue-server = {
      serviceConfig = {
        ProgramArguments = [
          "${cfg.package}/bin/garage-queue-server"
          "--config" "${configFile}"
        ];
        RunAtLoad = true;
        KeepAlive = true;
        StandardOutPath = cfg.logFile;
        StandardErrorPath = cfg.logFile;
      } // lib.optionalAttrs (cfg.user != null) {
        UserName = cfg.user;
      };
    };
  };
}
