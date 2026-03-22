# NixOS module for the garage-queue-server service.
# Exported from the flake as nixosModules.server.
#
# Minimal usage (defaults to Unix domain socket):
#
#   services.garage-queue-server = {
#     enable = true;
#     settings.server.nats_url = "nats://localhost:4222";
#   };
#
# To use TCP instead:
#
#   services.garage-queue-server = {
#     enable = true;
#     socket = null;
#     host   = "0.0.0.0";
#     port   = 9090;
#     settings.server.nats_url = "nats://localhost:4222";
#   };
#
# To reference the socket from a reverse proxy (e.g. nginx):
#
#   locations."/".proxyPass =
#     "http://unix:${config.services.garage-queue-server.socket}";
#
# Note: when using socket mode the reverse proxy user must be a member of
# the service group (cfg.group) so it can connect to the socket.
{ self }:
{ config, lib, pkgs, system, ... }:
let
  cfg = config.services.garage-queue-server;
  settingsFormat = pkgs.formats.toml { };
  # Inject the listen address into the server section so the operator does
  # not need to set it manually in settings.
  listenValue =
    if cfg.socket != null then "sd-listen"
    else "${cfg.host}:${toString cfg.port}";
  mergedSettings = lib.recursiveUpdate cfg.settings {
    server.listen = listenValue;
  };
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
      type = lib.types.str;
      default = "garage-queue";
      description = "User account under which the server runs.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "garage-queue";
      description = "Group under which the server runs.";
    };

    socket = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = "/run/garage-queue-server/garage-queue-server.sock";
      description = ''
        Path for the Unix domain socket used by the service.  When set,
        systemd socket activation is used and the host/port options are
        ignored.  Set to null to use TCP instead.

        Other services (e.g. nginx) that proxy to this socket must be
        members of the service group to connect.
      '';
    };

    host = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1";
      description = "IP address to bind to.  Ignored when socket is set.";
    };

    port = lib.mkOption {
      type = lib.types.port;
      default = 9090;
      description = "TCP port to listen on.  Ignored when socket is set.";
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
    # field accepted by the server can be set here.  The server.listen key is
    # managed by the socket/host/port options above and should not be set here.
    settings = lib.mkOption {
      type = settingsFormat.type;
      default = { };
      description = ''
        Configuration written verbatim to the server's TOML config file.
        Mirrors the config.toml structure.  See config.example.toml in the
        source tree for the full reference.  Do not set server.listen here;
        use the socket, host, and port options instead.
      '';
      example = lib.literalExpression ''
        {
          server.nats_url = "nats://localhost:4222";
          queues.ollama = {
            route = "/api/generate";
            extractors.model = {
              kind = "tag";
              capability = "model";
              jq_exp = ".model";
            };
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

    # Create the socket directory before the socket unit tries to bind.
    systemd.tmpfiles.rules = lib.mkIf (cfg.socket != null) [
      "d ${dirOf cfg.socket} 0750 ${cfg.user} ${cfg.group} -"
    ];

    # Socket unit: systemd creates and holds the Unix domain socket, then
    # passes the open file descriptor to the service on first activation.
    systemd.sockets.garage-queue-server = lib.mkIf (cfg.socket != null) {
      description = "garage-queue-server Unix domain socket";
      wantedBy = [ "sockets.target" ];
      socketConfig = {
        ListenStream = cfg.socket;
        SocketUser = cfg.user;
        SocketGroup = cfg.group;
        # 0660: accessible to the service user and group only.  Add the
        # reverse proxy user to cfg.group to grant it access.
        SocketMode = "0660";
        Accept = false;
      };
    };

    systemd.services.garage-queue-server = {
      description = "garage-queue server";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ]
        ++ lib.optional cfg.nats.enable "nats.service"
        ++ lib.optional (cfg.socket != null) "garage-queue-server.socket";
      wants = [ "network-online.target" ]
        ++ lib.optional cfg.nats.enable "nats.service";
      requires =
        lib.optional (cfg.socket != null) "garage-queue-server.socket";

      serviceConfig = {
        # Type = notify causes systemd to wait for the binary to call
        # sd_notify(READY=1) before marking the unit active.  The binary
        # does this via the sd-notify crate immediately after the listener
        # is bound.  NotifyAccess = main restricts who may send
        # notifications to the main process only.
        Type = "notify";
        NotifyAccess = "main";
        WatchdogSec = lib.mkDefault "30s";
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
