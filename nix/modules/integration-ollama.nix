# Ollama queue integration.
#
# Takes a queue name and returns an attrset of TOML queue definitions that the
# server config expects.  The Ollama integration produces two queues:
#   {name}-generate  — exclusive, POST /api/generate, extracts the model tag
#   {name}-tags      — broadcast, GET /api/tags, deduplicating combiner
name: {
  "${name}-generate" = {
    route = "/api/generate";
    method = "post";
    extractors.model_tag = {
      kind = "tag";
      capability = "model";
      jq_exp = ".model";
    };
  };
  "${name}-tags" = {
    route = "/api/tags";
    method = "get";
    mode = "broadcast";
    combiner_jq_exp = "{ models: [.[] | .response.models] | add | unique_by(.name) }";
  };
}
