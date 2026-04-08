# Darwin launchd module for the garage-queue-worker service.
# Exported from the flake as darwinModules.worker.
#
# Each named worker in services.garage-queue-worker.workers becomes a
# separate launchd user agent (garage-queue-worker-<name>).  This lets
# a single host run workers with different capabilities — for example one
# worker backed by GPU inference and another for CPU-only tasks.
#
# Workers run as user-level agents so they share the logged-in user's
# session and can reach Ollama or other per-user delegators without extra
# privilege configuration.
#
# Minimal usage:
#
#   services.garage-queue-worker.workers.gpu = {
#     enable = true;
#     integrations.ollama.enable = true;
#     settings.worker.server_url = "https://ollama.example.com";
#   };
#
# The Ollama integration requires services.ollama to be configured on
# the same host; it reads services.ollama.loadModels for capability tags
# and derives the delegator URL from services.ollama.{host,port}.
{ self }:
{ config, lib, pkgs, system, ... }:
let
  cfg = config.services.garage-queue-worker;
  settingsFormat = pkgs.formats.toml { };

  # Resolve the final settings for a named worker by merging any
  # integration-generated fragments on top of the raw settings.
  resolvedSettings = name: wCfg:
    let
      ollamaCfg = config.services.ollama;
      # Every worker needs an id for SSE-based dispatch.
      baseSettings = { worker.id = name; };
      ollamaSettings = lib.optionalAttrs wCfg.integrations.ollama.enable {
        capabilities.tags = ollamaCfg.loadModels;
        delegator = {
          kind = "http";
          url = "http://${ollamaCfg.host}:${toString ollamaCfg.port}/api/generate";
        };
      };
    in
    lib.recursiveUpdate (lib.recursiveUpdate baseSettings wCfg.settings) ollamaSettings;

  enabledWorkers = lib.filterAttrs (_: wCfg: wCfg.enable) cfg.workers;
in
{
  options.services.garage-queue-worker = {
    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${system}.worker;
      defaultText = lib.literalExpression "garage-queue.packages.\${system}.worker";
      description = "The garage-queue-worker package to use.";
    };

    workers = lib.mkOption {
      type = lib.types.attrsOf (lib.types.submodule (
        { name, ... }: {
          options = {
            enable = lib.mkEnableOption "this garage-queue worker instance";

            # The settings attrset is converted to TOML and passed to
            # the worker via --config.  Its structure mirrors the worker
            # config.toml directly.
            settings = lib.mkOption {
              type = settingsFormat.type;
              default = { };
              description = ''
                Configuration written verbatim to this worker's TOML
                config file.  Mirrors the config.toml structure.  The
                delegator section is required unless
                integrations.ollama.enable provides it.
              '';
              example = lib.literalExpression ''
                {
                  worker = {
                    server_url = "http://192.168.1.10:9090";
                    reconnect_interval_ms = 1000;
                  };
                  control = {
                    host = "127.0.0.1";
                    port = 9091;
                  };
                  capabilities.scalars.vram_mb = 16384;
                }
              '';
            };

            integrations.ollama.enable = lib.mkEnableOption ''
              Ollama integration for this worker.  When enabled,
              capabilities.tags is populated from
              services.ollama.loadModels and the delegator is set to
              the local Ollama HTTP endpoint
            '';

            logFile = lib.mkOption {
              type = lib.types.str;
              default = "/tmp/garage-queue-worker-${name}.log";
              description = "Path for combined stdout/stderr log output.";
            };
          };
        }
      ));
      default = { };
      description = ''
        Named worker instances.  Each entry produces a separate launchd
        user agent named garage-queue-worker-<name>.
      '';
      example = lib.literalExpression ''
        {
          gpu = {
            enable = true;
            integrations.ollama.enable = true;
            settings.worker.server_url = "https://ollama.example.com";
          };
        }
      '';
    };
  };

  config = lib.mkIf (enabledWorkers != { }) {
    # User-level agents: start when the user logs in and can reach
    # services running in their session (e.g. Ollama on localhost).
    launchd.agents = lib.mapAttrs' (name: wCfg:
      lib.nameValuePair "garage-queue-worker-${name}" {
        serviceConfig = {
          ProgramArguments =
            let
              configFile = settingsFormat.generate
                "garage-queue-worker-${name}.toml"
                (resolvedSettings name wCfg);
            in
            [
              "${cfg.package}/bin/garage-queue-worker"
              "--config" "${configFile}"
            ];
          RunAtLoad = true;
          KeepAlive = true;
          StandardOutPath = wCfg.logFile;
          StandardErrorPath = wCfg.logFile;
        };
      }
    ) enabledWorkers;
  };
}
