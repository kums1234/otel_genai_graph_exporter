# Contributors

Thanks to everyone who has shaped this project. Additions are welcome —
see the [How to get listed](#how-to-get-listed) section below.

## Maintainers

| Name | Role | Contact |
|---|---|---|
| **Kumaran Ravichandran** ([@kums1234](https://github.com/kums1234)) | Creator, maintainer | `kumaran.pec [at] gmail.com` |

## Contributors

<!--
The format below mirrors the `all-contributors` bot spec so it's easy to
automate later. For now, add yourself in a PR.

Emoji legend (subset we use):
  💻  code              📖  docs
  🐛  bug reports       💡  examples / ideas
  ⚠️  tests             🔌  provider adapter
  🎨  design / visuals  🔍  research
  🚇  infrastructure    🤔  design feedback
-->

<!-- contributors-start -->

_No external contributors yet — **be the first.** Open a PR that adds
your name below and a one-line description of what you shipped._

<!-- contributors-end -->

## How to get listed

Any non-trivial contribution — code, docs, bug report, test fixture,
real-trace capture, provider adapter, design feedback — qualifies.

1. Open a PR that addresses something in the issue tracker (or the
   [outstanding items in the README](README.md#status)).
2. In the same PR, edit `CONTRIBUTORS.md` and add a row to the
   **Contributors** section with:
   - your name and GitHub handle (required)
   - one-line description of your contribution
   - one or more of the emoji from the legend above
3. The maintainers will merge both together.

Example row once someone contributes:

```markdown
| **Jane Doe** ([@janedoe](https://github.com/janedoe)) | 🔌 added the Cohere provider adapter |
```

## Acknowledgements

This project stands on the shoulders of:

- The **OpenTelemetry** project and the GenAI Semantic Conventions
  working group, whose v1.37 spec is the foundation this exporter maps
  from.
- **Neo4j**, for the graph database and the MERGE semantics that make
  idempotent ingest trivial.
- **Python Contrib** authors of `opentelemetry-instrumentation-openai-v2`
  and `opentelemetry-instrumentation-google-genai` — their test
  cassettes and live instrumentor output surface the real-world
  divergence our legacy-compat table handles.
- The **cytoscape.js** team, whose single-file interactive graph
  viewer makes the HTML export format possible.

## See also

- [`CONTRIBUTING.md`](CONTRIBUTING.md) — dev loop, test pipeline, how
  to add fixtures / provider adapters / legacy aliases / invariants.
- [Issue tracker](https://github.com/kums1234/otel_genai_graph_exporter/issues)
  — things we'd love help with are labelled `good first issue`.
