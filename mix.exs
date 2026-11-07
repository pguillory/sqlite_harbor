defmodule SqliteHarbor.MixProject do
  use Mix.Project

  def project do
    [
      app: :sqlite_harbor,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: if(Mix.env() == :test, do: ["lib", "test/support"], else: ["lib"]),
      deps: deps(),
      name: "SqliteHarbor",
      description: "Many small SQLite databases across a BEAM cluster, backed by S3",
      source_url: "https://github.com/pguillory/sqlite_harbor",
      package: [
        files:
          ~w(lib docs scripts mix.exs README.md LICENSE CONTRIBUTING.md SECURITY.md AGENTS.md .formatter.exs),
        licenses: ["MIT"],
        links: %{"GitHub" => "https://github.com/pguillory/sqlite_harbor"}
      ],
      docs: [
        main: "readme",
        extras: [
          "README.md",
          "CONTRIBUTING.md",
          "SECURITY.md",
          "docs/architecture.md",
          "docs/operations.md"
        ]
      ]
    ]
  end

  def application, do: [extra_applications: [:logger, :crypto]]

  defp deps do
    [
      {:exqlite,
       git: "https://github.com/pguillory/exqlite.git",
       ref: "b35a8b01f62e0087d32a7a6ae6be74c2bd51276d",
       override: true},
      {:req_s3, "~> 0.2.5"},
      {:req, "~> 0.7"},
      {:telemetry, "~> 1.3"},
      {:jason, "~> 1.4"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end
end
