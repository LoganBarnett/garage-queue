# Estimate VRAM requirement from the model name.
#
# Parses parameter count (e.g. "8b" -> 8) and quantisation level
# (e.g. "q4" -> 600 MB/B, default q4 if absent), then adds overhead.
# Known overrides bypass the heuristic entirely.

.model as $m |
({ "q4": 600, "q5": 700, "q8": 1000, "f16": 2000 }) as $mult |
({"mixtral:8x7b": 26000}) as $overrides |

$overrides[$m] //
(
  ($m | capture("(?<n>[0-9]+\\.?[0-9]*)b").n | tonumber) as $params |
  ($m | (match("(q[0-9]+|f[0-9]+)").captures[0].string) // "q4") as $quant |
  ($mult[$quant] // 600) as $mbPerB |
  ($params * $mbPerB + 1500)
)
