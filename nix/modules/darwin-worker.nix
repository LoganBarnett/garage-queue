{ self }:
{ config, lib, pkgs, ... }:
let
  cfg = config.services.garage-queue-worker;
  settingsFormat = pkgs.formats.toml { };
  configFile = settingsFormat.generate "garage-queue-worker.toml" cfg.settings;
in
{
  options.services.garage-queue-worker = {
    enable = lib.mkEnableOption "garage-queue worker";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.system}.worker;
      defaultText = lib.literalExpression "garage-queue.packages.\${system}.worker";
      description = "The garage-queue-worker package to use.";
    };

    logFile = lib.mkOption {
      type = lib.types.str;
      default = "/tmp/garage-queue-worker.log";
      description = "Path for combined stdout/stderr log output.";
    };

    # The settings attrset is converted to TOML and passed to the worker via
    # --config.  Its structure mirrors the worker config.toml directly.
    #
    # The worker runs as a user-level launchd agent, so it shares the user's
    # session and can reach Ollama or other delegators running as the same
    # user without extra configuration.
    settings = lib.mkOption {
      type = settingsFormat.type;
      default = { };
      description = ''
        Configuration written verbatim to the worker's TOML config file.
        Mirrors the config.toml structure.  The delegator section is required.
      '';
      example = lib.literalExpression ''
        {
          worker = {
            server_url = "http://192.168.1.10:9090";
            poll_interval_ms = 1000;
          };
          control = {
            host = "127.0.0.1";
            port = 9091;
          };
          capabilities = {
            tags = [ "llama3.2:8b" "llama3.2:3b" ];
            scalars.vram_mb = 8192;
          };
          delegator = {
            kind = "http";
            url = "http://localhost:11434";
          };
        }
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    # User-level agent: starts when the user logs in and can reach services
    # running in their session (e.g. Ollama on localhost).
    launchd.agents.garage-queue-worker = {
      serviceConfig = {
        ProgramArguments = [
          "${cfg.package}/bin/garage-queue-worker"
          "--config" "${configFile}"
        ];
        RunAtLoad = true;
        KeepAlive = true;
        StandardOutPath = cfg.logFile;
        StandardErrorPath = cfg.logFile;
      };
    };
  };
}
