# VHS tape — deterministic recording of the headline "cost_by_model" demo.
#
# Produces docs/images/demo.gif and docs/images/demo.mp4.
#
# Install once:  brew install vhs       # macOS
#                apt install vhs        # Debian/Ubuntu
#                or see https://github.com/charmbracelet/vhs
#
# Record:
#   vhs docs/demo/demo.tape
#
# Prereqs while recording:
#   * local Neo4j running on bolt://localhost:17687 (our dev container)
#   * .env populated with NEO4J_URI/USER/PASSWORD
#   * fixtures already loaded:
#       .venv/bin/python -m otel_genai_graph.load \
#           tests/fixtures/*.json tests/fixtures/real/ollama/*.json
#
# The tape is checked-in so anyone can re-record the demo identically.

Output docs/images/demo.gif
Output docs/images/demo.mp4

Set Shell       bash
Set FontSize    16
Set Width       1000
Set Height      580
Set Theme       "Tokyo Night"
Set PlaybackSpeed 1.2
Set TypingSpeed 40ms

# Land in the project root on load
Type "cd ~/code/otel-genai-graph"
Enter
Sleep 600ms

# Activate venv — quiet
Type "source .venv/bin/activate"
Enter
Sleep 400ms
Ctrl+L

# Headline query
Type "python tools/render_graph.py --from-neo4j --query cost_by_model --format table"
Sleep 400ms
Enter
Sleep 2500ms
Sleep 1500ms

# Quick pivot to the visual
Type "python tools/render_graph.py --from-neo4j --query session_tree \\"
Sleep 200ms
Enter
Type "    --param session_id=ollama-multi-demo --output /tmp/multi --format html"
Enter
Sleep 2000ms

# Reveal that it's a single self-contained file
Type "ls -lh /tmp/multi.html && echo '-> open /tmp/multi.html'"
Enter
Sleep 2500ms

# Soft outro
Type "# mixed-vendor, one command, \$0, zero Cypher."
Sleep 2000ms
Enter
Sleep 1500ms
