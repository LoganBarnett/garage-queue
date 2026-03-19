{ self }:
{ config, lib, pkgs, system, ... }:
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
      default = self.packages.${system}.worker;
      defaultText = lib.literalExpression "garage-queue.packages.\${system}.worker";
      description = "The garage-queue-worker package to use.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "garage-queue";
      description = "User account under which the worker runs.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "garage-queue";
      description = "Group under which the worker runs.";
    };

    # The settings attrset is converted to TOML and passed to the worker via
    # --config.  Its structure mirrors the worker config.toml directly.
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
    users.users.${cfg.user} = lib.mkDefault {
      isSystemUser = true;
      group = cfg.group;
      description = "garage-queue worker service user";
    };

    users.groups.${cfg.group} = lib.mkDefault { };

    systemd.services.garage-queue-worker = {
      description = "garage-queue worker";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];

      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        ExecStart = "${cfg.package}/bin/garage-queue-worker --config ${configFile}";
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
