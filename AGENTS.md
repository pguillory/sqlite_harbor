# SqliteHarbor

This is a standalone Elixir library. Keep the public API plain data and the library
independent of its hosts. Read README.md and docs/architecture.md before changing behavior.

Use real SQLite and faithful storage fakes. Cluster promises require real BEAM peers.
Run `mix format --check-formatted` and `mix test --warnings-as-errors` before committing.
The real S3/R2 probe is manual; report explicitly when it has not run.
Review meaningful changes through independent Claude and Codex reviewers before commit.
Never commit or print object-store credentials. Do not add AI attribution to commits.
