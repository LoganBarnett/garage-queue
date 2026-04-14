{
  description = "Heterogeneous capability-aware work queue";
  inputs = {
    nixpkgs.url = github:NixOS/nixpkgs/25.11;
    rust-overlay.url = "github:oxalica/rust-overlay";
    crane.url = "github:ipetkov/crane";
  };

  outputs = { self, nixpkgs, rust-overlay, crane }@inputs: let
    forAllSystems = nixpkgs.lib.genAttrs nixpkgs.lib.systems.flakeExposed;
    overlays = [
      (import rust-overlay)
    ];
    pkgsFor = system: import nixpkgs {
      inherit system;
      overlays = overlays;
    };

    # ============================================================================
    # WORKSPACE CRATES CONFIGURATION
    # ============================================================================
    # Define all workspace crates here. This makes it easy to:
    # - Generate packages
    # - Generate apps
    # - Generate overlays
    # - Keep package lists consistent across the flake
    #
    # When customizing this template for your project:
    # 1. Update the names below to match your project
    # 2. Add/remove crates as needed
    # 3. The package and app generation will automatically update
    # ============================================================================
    workspaceCrates = {
      # CRATE:cli:begin
      # CLI tool for inspecting and managing queues
      cli = {
        name = "garage-queue-cli";
        binary = "garage-queue-cli";
        description = "Queue inspection and management CLI";
      };
      # CRATE:cli:end

      # CRATE:server:begin
      # Queue server — accepts producer requests, dispatches to workers
      server = {
        name = "garage-queue-server";
        binary = "garage-queue-server";
        description = "Queue server";
      };
      # CRATE:server:end

      # CRATE:worker:begin
      # Queue worker — polls for items, executes via configured delegator
      worker = {
        name = "garage-queue-worker";
        binary = "garage-queue-worker";
        description = "Queue worker";
      };
      # CRATE:worker:end

      # Note: The 'lib' crate is not included here as it doesn't produce a binary
    };

    # Development shell packages.
    devPackages = pkgs: let
      rust = pkgs.rust-bin.stable.latest.default.override {
        extensions = [
          # For rust-analyzer and others.  See
          # https://nixos.wiki/wiki/Rust#Shell.nix_example for some details.
          "rust-src"
          "rust-analyzer"
          "rustfmt"
        ];
      };
    in [
      rust
      pkgs.cargo-sweep
      pkgs.pkg-config
      pkgs.openssl
      pkgs.jq
    ];
  in {

    devShells = forAllSystems (system: {
      default = (pkgsFor system).mkShell {
        buildInputs = devPackages (pkgsFor system);
        shellHook = ''
          echo "Rust Template development environment"
          echo ""
          echo "Available Cargo packages (use 'cargo build -p <name>'):"
          cargo metadata --no-deps --format-version 1 2>/dev/null | \
            jq -r '.packages[].name' | \
            sort | \
            sed 's/^/  • /' || echo "  Run 'cargo init' to get started"
        '';
      };
    });

    # ============================================================================
    # PACKAGES
    # ============================================================================
    packages = forAllSystems (system: let
      pkgs = pkgsFor system;
      craneLib = (crane.mkLib pkgs).overrideToolchain (p: p.rust-bin.stable.latest.default);

      # Common build arguments shared by all crates.
      # Tests run via 'cargo test' in the dev shell; the Nix sandbox typically
      # lacks access to external services and integration test binaries.
      # Do NOT use darwin.apple_sdk.frameworks here — removed in nixpkgs 25.11;
      # macOS SDK frameworks are part of the default Darwin stdenv.
      commonArgs = {
        src = craneLib.cleanCargoSource ./.;
        doCheck = false;
        # reqwest uses native-tls which needs OpenSSL on Linux.
        buildInputs = pkgs.lib.optionals pkgs.stdenv.isLinux [ pkgs.openssl ];
        nativeBuildInputs = pkgs.lib.optionals pkgs.stdenv.isLinux [ pkgs.pkg-config ];
      };

      # Build individual crate packages from workspaceCrates.
      cratePackages = pkgs.lib.mapAttrs (key: crate:
        craneLib.buildPackage (commonArgs // {
          pname = crate.name;
          cargoExtraArgs = "-p ${crate.name}";
        })
      ) workspaceCrates;

    in cratePackages // {
      # Build all workspace binaries together.
      # Update pname to match your project name.
      default = craneLib.buildPackage (commonArgs // { pname = "garage-queue"; });
    });

    # ============================================================================
    # APPS
    # ============================================================================
    apps = forAllSystems (system: let
      pkgs = pkgsFor system;
    in pkgs.lib.mapAttrs (key: crate: {
      type = "app";
      program = "${self.packages.${system}.${key}}/bin/${crate.binary}";
    }) workspaceCrates);

    # ============================================================================
    # OVERLAYS
    # ============================================================================
    overlays.default = final: prev:
      prev.lib.mapAttrs' (key: crate:
        prev.lib.nameValuePair crate.name self.packages.${final.stdenv.hostPlatform.system}.${key}
      ) workspaceCrates;

    # ============================================================================
    # NIXOS MODULES
    # ============================================================================
    nixosModules = {
      server = import ./nix/modules/server.nix { inherit self; };
      worker = import ./nix/modules/worker.nix { inherit self; };

      # Convenience module that imports both server and worker.
      default = {
        imports = [
          (import ./nix/modules/server.nix { inherit self; })
          (import ./nix/modules/worker.nix { inherit self; })
        ];
      };
    };

    # ============================================================================
    # DARWIN MODULES
    # ============================================================================
    darwinModules = {
      server = import ./nix/modules/darwin-server.nix { inherit self; };
      worker = import ./nix/modules/darwin-worker.nix { inherit self; };

      # Convenience module that imports both server and worker.
      default = {
        imports = [
          (import ./nix/modules/darwin-server.nix { inherit self; })
          (import ./nix/modules/darwin-worker.nix { inherit self; })
        ];
      };
    };

  };

}
