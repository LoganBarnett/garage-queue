{ self }:
{ config, lib, pkgs, ... }:
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
      default = self.packages.${pkgs.system}.server;
      defaultText = lib.literalExpression "garage-queue.packages.\${system}.server";
      description = "The garage-queue-server package to use.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "garage-queue";
      description = "User account under which the server runs.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "garage-queue";
      description = "Group under which the server runs.";
    };

    nats = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = ''
          Start a local NATS server with JetStream for the queue.  When true,
          services.nats is configured with JetStream enabled.  All tuning
          (storage limits, clustering, etc.) is left to the operator via
          services.nats.
        '';
      };
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
            host = "0.0.0.0";
            port = 9090;
            nats_url = "nats://localhost:4222";
            generate_queue = "ollama";
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
    users.users.${cfg.user} = lib.mkDefault {
      isSystemUser = true;
      group = cfg.group;
      description = "garage-queue server service user";
    };

    users.groups.${cfg.group} = lib.mkDefault { };

    # Only JetStream is required; all tuning is left to the operator.
    services.nats = lib.mkIf cfg.nats.enable {
      enable = true;
      jetstream = true;
    };

    systemd.services.garage-queue-server = {
      description = "garage-queue server";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ]
        ++ lib.optional cfg.nats.enable "nats.service";
      wants = [ "network-online.target" ]
        ++ lib.optional cfg.nats.enable "nats.service";

      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        ExecStart = "${cfg.package}/bin/garage-queue-server --config ${configFile}";
        Restart = "on-failure";
        RestartSec = "5s";
        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
      };
    };
  };
}
