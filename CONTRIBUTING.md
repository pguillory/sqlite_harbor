# Contributing

Open a clear issue or pull request describing the behavior and validation. Keep changes
within the small-library model documented in README.md and docs/architecture.md.
The default test suite includes real BEAM peers and intentional lifecycle failures;
it needs an Elixir/OTP installation, a C compiler and epmd, with no cloud credentials.

Run `mix format --check-formatted` and `mix test --warnings-as-errors`. Use real SQLite
and faithful storage fakes; failure tests should control object-store responses rather
than substitute SQL behavior. Exercise cluster promises through real nodes. Never print
or commit credentials. Report unperformed real-storage checks plainly.

Contributions are licensed under this repository's MIT license.
