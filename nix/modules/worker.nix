# NixOS module for the garage-queue-worker service.
# Exported from the flake as nixosModules.worker.
#
# Each named worker in services.garage-queue-worker.workers becomes a
# separate systemd service (garage-queue-worker-<name>).  This lets a
# single host run workers with different capabilities — for example one
# worker backed by GPU inference and another for CPU-only tasks.
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

  # Build the listen address string for a server endpoint from its
  # socket/host/port options.
  listenValue = opts:
    if opts.socket != null then opts.socket
    else "${opts.host}:${toString opts.port}";

  # Per-instance runtime directory for Unix domain sockets.
  runtimeDir = name: "/run/garage-queue-worker-${name}";

  # Resolve the final settings for a named worker by merging any
  # integration-generated fragments on top of the raw settings, then
  # inject per-instance listen addresses for the control and observe
  # servers.
  resolvedSettings = name: wCfg:
    let
      ollamaCfg = config.services.ollama;
      baseSettings = { };
      ollamaSettings = lib.optionalAttrs wCfg.integrations.ollama.enable {
        capabilities.tags = ollamaCfg.loadModels;
        delegator = {
          kind = "http";
          url = "http://${ollamaCfg.host}:${toString ollamaCfg.port}/api/generate";
        };
      };
      listenSettings = {
        control.listen_address = listenValue wCfg.control;
        observe.listen_address = listenValue wCfg.observe;
      };
    in
    lib.recursiveUpdate
      (lib.recursiveUpdate
        (lib.recursiveUpdate baseSettings wCfg.settings)
        ollamaSettings)
      listenSettings;

  enabledWorkers = lib.filterAttrs (_: wCfg: wCfg.enable) cfg.workers;

  # Shared option definitions for a listen endpoint (socket or TCP).
  listenOptions = { defaultSocket, defaultPort }: {
    socket = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = defaultSocket;
      description = ''
        Path for the Unix domain socket.  When set, the host and port
        options are ignored.  Set to null to use TCP instead.
      '';
    };

    host = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1";
      description = "IP address to bind to.  Ignored when socket is set.";
    };

    port = lib.mkOption {
      type = lib.types.port;
      default = defaultPort;
      description = "TCP port to listen on.  Ignored when socket is set.";
    };
  };
in
{
  options.services.garage-queue-worker = {
    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${system}.worker;
      defaultText = lib.literalExpression "garage-queue.packages.\${system}.worker";
      description = "The garage-queue-worker package to use.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "garage-queue";
      description = "User account under which all worker instances run.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "garage-queue";
      description = "Group under which all worker instances run.";
    };

    workers = lib.mkOption {
      type = lib.types.attrsOf (lib.types.submodule (
        { name, ... }: {
          options = {
            enable = lib.mkEnableOption "this garage-queue worker instance";

            # The settings attrset is converted to TOML and passed to the
            # worker via --config.  Its structure mirrors the worker
            # config.toml directly.
            settings = lib.mkOption {
              type = settingsFormat.type;
              default = { };
              description = ''
                Configuration written verbatim to this worker's TOML config
                file.  Mirrors the config.toml structure.  The delegator
                section is required unless integrations.ollama.enable
                provides it.

                The control and observe listen addresses are managed by
                their respective options below and should not be set here.
              '';
              example = lib.literalExpression ''
                {
                  worker = {
                    server_url = "http://192.168.1.10:9090";
                    reconnect_interval_ms = 1000;
                  };
                  capabilities.scalars.vram_mb = 16384;
                  concurrency = {
                    default = 1;
                    ollama-tags = 4;
                  };
                }
              '';
            };

            control = listenOptions {
              defaultSocket = "${runtimeDir name}/control.sock";
              defaultPort = 9091;
            };

            observe = listenOptions {
              defaultSocket = "${runtimeDir name}/observe.sock";
              defaultPort = 9092;
            };

            integrations.ollama.enable = lib.mkEnableOption ''
              Ollama integration for this worker.  When enabled,
              capabilities.tags is populated from
              services.ollama.loadModels and the delegator is set to the
              local Ollama HTTP endpoint
            '';
          };
        }
      ));
      default = { };
      description = ''
        Named worker instances.  Each entry produces a separate systemd
        service named garage-queue-worker-<name>.
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
    assertions = lib.mapAttrsToList (name: wCfg:
      let resolved = resolvedSettings name wCfg;
      in {
        assertion = (resolved ? worker) && (resolved.worker ? id);
        message = ''
          garage-queue worker '${name}' is missing worker.id.
          Set services.garage-queue-worker.workers.${name}.settings.worker.id
          to a value unique across all hosts (e.g. the hostname).
        '';
      }
    ) enabledWorkers;

    users.users.${cfg.user} = lib.mkDefault {
      isSystemUser = true;
      group = cfg.group;
      description = "garage-queue worker service user";
    };

    users.groups.${cfg.group} = lib.mkDefault { };

    systemd.services = lib.mapAttrs' (name: wCfg:
      lib.nameValuePair "garage-queue-worker-${name}" {
        description = "garage-queue worker (${name})";
        wantedBy = [ "multi-user.target" ];
        after = [ "network-online.target" ];
        wants = [ "network-online.target" ];

        serviceConfig = {
          Type = "simple";
          User = cfg.user;
          Group = cfg.group;
          ExecStart =
            let
              configFile = settingsFormat.generate
                "garage-queue-worker-${name}.toml"
                (resolvedSettings name wCfg);
            in
            "${cfg.package}/bin/garage-queue-worker --config ${configFile}";
          Restart = "on-failure";
          RestartSec = "5s";
          NoNewPrivileges = true;
          ProtectSystem = "strict";
          ProtectHome = true;
          PrivateTmp = true;
          # RuntimeDirectory creates /run/garage-queue-worker-<name> owned by
          # the service user and exempts it from ProtectSystem=strict so the
          # worker can bind Unix domain sockets there.  The directory persists
          # for the lifetime of the service.
          RuntimeDirectory = lib.mkIf
            (wCfg.control.socket != null || wCfg.observe.socket != null)
            "garage-queue-worker-${name}";
          RuntimeDirectoryMode = lib.mkIf
            (wCfg.control.socket != null || wCfg.observe.socket != null)
            "0750";
        };
      }
    ) enabledWorkers;
  };
}
