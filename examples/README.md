# examples/

Runnable reference material beyond the hand-written fixtures.

## `real_gemini_chat.json`

Real Gemini 2.5 Flash chat, captured through
[`tests/capture_real_traces.py`](../tests/capture_real_traces.py) with
`--provider google`. Conversation id and timestamps are sanitised; the
token counts (8 / 136), model name, and response preview are authentic.

Drop it through the mapper + sink the same way you'd load any fixture:

```bash
# with the Quickstart's Neo4j container running
python -m otel_genai_graph.load examples/real_gemini_chat.json
# then in the browser:
#   MATCH (s:Session {id:"example-conv-gemini-1"})-[:CONTAINS]->(o)-[:EXECUTED]->(m) RETURN *
```

Expected graph: `Session` + `Operation` + `Model (google/gemini-2.5-flash)`
= 3 nodes, `CONTAINS` + `EXECUTED` = 2 edges. No violations from the
invariant suite.

## Capturing your own real traces

To produce more examples with your own API keys / local Ollama:

```bash
# Free tier — no credit card: https://aistudio.google.com → API keys
export GEMINI_API_KEY=...
python tests/capture_real_traces.py \
    --provider google --model gemini-2.5-flash \
    --prompt "Explain quicksort in two sentences." \
    --budget-usd 0.10 \
    --output-dir tests/fixtures/real

# Local / offline: Ollama exposes an OpenAI-compatible endpoint
ollama pull qwen2.5:7b
export OPENAI_BASE_URL=http://localhost:11434/v1/
export OPENAI_API_KEY=ollama
python tests/capture_real_traces.py \
    --provider openai --model qwen2.5:7b \
    --shape tool_call --agent-id researcher \
    --prompt "What time is it?" \
    --ignore-unknown-pricing --budget-usd 0 \
    --output-dir tests/fixtures/real/ollama
```

`tests/fixtures/real/` is gitignored — it's your local scratch space. Copy
anything you want to commit to `examples/` and sanitise it first.
