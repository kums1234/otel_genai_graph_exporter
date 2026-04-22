# demo/

Source material for the animated demo you see in the project README.

## What's here

| File | Role |
|---|---|
| `demo.tape` | VHS script — deterministic shell recording |
| `../images/demo.svg` | hand-crafted animated SVG shown in the README; renders natively on GitHub, no tooling required |
| `../images/demo.gif` (generated) | recorded from `demo.tape` via VHS; optional, higher-fidelity alternative to the SVG |
| `../images/demo.mp4` (generated) | same, as MP4 |

## Regenerating `demo.gif`

The animated SVG ships with the repo and covers the ≥90% case. Only
bother producing the `.gif` / `.mp4` if you specifically need a raster
format (Slack / Twitter embeds, non-SVG renderers, accessibility tools
that prefer video).

**Install VHS:**
```bash
brew install vhs          # macOS
apt install vhs           # Debian / Ubuntu
# or see https://github.com/charmbracelet/vhs for other platforms
```

**Prereqs when recording:**
```bash
docker run -d --name otel-neo4j \
    -p 17474:7474 -p 17687:7687 \
    -e NEO4J_AUTH=neo4j/testtest neo4j:5

cp .env.example .env   # uncomment NEO4J_URI/USER/PASSWORD

# prime the graph so cost_by_model has rows
.venv/bin/python -m otel_genai_graph.load \
    tests/fixtures/*.json \
    tests/fixtures/real/ollama/*.json 2>/dev/null
```

**Record:**
```bash
vhs docs/demo/demo.tape
# writes docs/images/demo.gif and docs/images/demo.mp4
```

The tape is intentionally opinionated — `Set Theme "Tokyo Night"`, fixed
font size, fixed typing speed — so any re-recording produces a visually
identical asset. If you want to tweak, edit `demo.tape` and commit it
alongside the regenerated images.
