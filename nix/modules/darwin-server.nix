{ self }:
{ config, lib, pkgs, system, ... }:
let
  cfg = config.services.garage-queue-server;
  settingsFormat = pkgs.formats.toml { };
  configFile = settingsFormat.generate "garage-queue-server.toml" cfg.settings;
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
    # field accepted by the server can be set here.
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
          queues.ollama.extractors.model = {
            kind = "tag";
            capability = "model";
            jq_exp = ".model";
          };
        }
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
