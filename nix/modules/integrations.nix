# Shared integration layer for the garage-queue server modules.
#
# Both the NixOS and darwin server modules call `expandQueues` to turn the
# declarative queues option into the flat TOML queue settings the server binary
# expects.
{ lib }:
let
  ollamaIntegration = import ./integration-ollama.nix;
in
{
  # expandQueues : attrsOf queueSubmodule -> attrsOf tomlQueueSettings
  #
  # Iterates over the queues attrset and expands each enabled integration
  # into the concrete TOML queue definitions.
  expandQueues = queues:
    lib.foldlAttrs (acc: name: qCfg:
      acc // lib.optionalAttrs qCfg.integrations.ollama.enable
        (ollamaIntegration name)
    ) { } queues;
}
