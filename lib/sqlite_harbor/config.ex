defmodule SqliteHarbor.Config do
  @moduledoc false
  @enforce_keys [:name, :storage, :cache_dir]
  @derive {Inspect, except: [:storage]}
  defstruct name: nil,
            storage: nil,
            cache_dir: nil,
            prefix: "",
            cache_budget_bytes: 1_000_000_000,
            max_db_bytes: 100_000_000,
            save_interval: 120_000,
            idle_timeout: 300_000,
            max_statements: 100,
            max_request_bytes: 1_000_000,
            max_rows: 10_000,
            max_result_bytes: 10_000_000,
            max_value_bytes: 10_000_000,
            wall_timeout: 10_000,
            hard_heap_limit: 1_000_000_000,
            shutdown_timeout: 120_000,
            recheck_interval: 30_000,
            retry_max: 30_000

  def new(opts) do
    config = struct!(__MODULE__, opts)

    unless is_atom(config.name) and config.name != nil,
      do: raise(ArgumentError, "name must be an atom")

    unless match?({module, _} when is_atom(module), config.storage),
      do: raise(ArgumentError, "storage must be {module, options}")

    unless is_binary(config.cache_dir), do: raise(ArgumentError, "cache_dir is required")

    unless Regex.match?(~r/\A[A-Za-z0-9._~\/-]*\z/, config.prefix) and
             (config.prefix == "" or String.ends_with?(config.prefix, "/")),
           do: raise(ArgumentError, "prefix must be a safe path ending with /")

    for {key, default} <- Map.from_struct(%__MODULE__{name: nil, storage: nil, cache_dir: nil}),
        is_integer(default) do
      value = Map.fetch!(config, key)

      unless is_integer(value) and value > 0,
        do: raise(ArgumentError, "#{key} must be a positive integer")
    end

    if config.max_value_bytes > 2_147_483_647,
      do: raise(ArgumentError, "max_value_bytes must fit a SQLite limit integer")

    %{config | cache_dir: Path.expand(config.cache_dir)}
  end

  def local(module, name), do: {:global, {module, node(), name}}
  def key(c, id), do: c.prefix <> "db/" <> id

  def event(name, measures \\ %{}, metadata \\ %{}),
    do: :telemetry.execute([:sqlite_harbor, name], measures, metadata)
end
