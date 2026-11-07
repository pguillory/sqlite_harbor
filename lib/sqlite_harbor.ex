defmodule SqliteHarbor do
  @moduledoc """
  Many small SQLite databases, each with one cluster-wide owner and a whole-file
  object-store copy. Add `{SqliteHarbor, name: MyStore, storage: {backend, opts},
  cache_dir: "/data/sqlite"}` to your supervision tree.

  An execute statement list is one transaction. Success acknowledges a **local**
  commit; remote saves happen on a timer and at idle. See README for loss windows.
  """
  alias SqliteHarbor.{Cache, Config, Files, Owner, SQL, Storage}

  def child_spec(opts) do
    c = Config.new(opts)

    %{
      id: {__MODULE__, c.name},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      shutdown: :infinity
    }
  end

  def start_link(opts), do: opts |> Config.new() |> SqliteHarbor.Supervisor.start_link()

  @doc "Creates and durably uploads an initialized, empty database."
  def create(service) do
    c = Cache.config(service)
    path = Files.temp(c)

    try do
      with :ok <- SQL.initialize(path, c.max_value_bytes),
           :ok <- if(Files.size(path) <= c.max_db_bytes, do: :ok, else: {:error, :full}),
           do: upload_new(c, path)
    after
      Files.scratch_remove(path)
    end
  end

  @doc "Deletes a database through its owner. Unknown IDs are idempotent successes."
  def delete(service, id) do
    with :ok <- check_id(id), do: call(Cache.config(service), id, :delete, :delete, 30_000)
  end

  @doc "Lists database IDs, with an opaque continuation cursor and positive page limit."
  def list(service, cursor \\ nil, limit)

  def list(service, cursor, limit) when is_integer(limit) and limit > 0 do
    c = Cache.config(service)

    with {:ok, ids, next} <- Storage.call(c.storage, :list, [c.prefix <> "db/", cursor, limit]) do
      {:ok, Enum.filter(ids, &valid_id?/1), next}
    end
  end

  def list(_, _, _), do: {:error, :invalid_limit}

  @doc """
  Executes `[sql | {sql, list_or_map_params}]` as one transaction. Errors carry a
  zero-based statement index, code and message. timeout defaults to 30 seconds;
  the configured wall_timeout (10 seconds) also limits the total call.
  """
  def execute(service, id, statements, opts \\ []) do
    c = Cache.config(service)
    timeout = Keyword.get(opts, :timeout, 30_000)

    with :ok <- check_id(id),
         :ok <- inputs(statements, c),
         true <- is_integer(timeout) and timeout > 0 do
      budget = min(timeout, c.wall_timeout)
      expiry = System.system_time(:millisecond) + budget

      case call(c, id, {:execute, statements, expiry}, :normal, budget) do
        {:error, %{code: _}} = result -> result
        {:error, reason} -> {:error, SQL.error(nil, reason)}
        other -> other
      end
    else
      false -> {:error, SQL.error(nil, :invalid_timeout)}
      {:error, reason} -> {:error, SQL.error(nil, reason)}
    end
  end

  @doc """
  Forces an owner save, then downloads and verifies one whole object version.
  Returns a stream whose temporary file is deleted when enumeration ends.
  Enumerate once. Unenumerated/abandoned exports are removed at the next node boot.
  """
  def export(service, id) do
    c = Cache.config(service)

    with :ok <- check_id(id),
         :ok <- call(c, id, :save, :normal, 30_000),
         {:ok, path} <- export_file(c, id, 2) do
      stream =
        Stream.resource(
          fn -> File.open!(path, [:read, :binary]) end,
          fn io ->
            case IO.binread(io, 64 * 1024) do
              :eof ->
                {:halt, io}

              {:error, reason} ->
                raise File.Error, reason: reason, action: "read export", path: path

              bytes ->
                {[bytes], io}
            end
          end,
          fn io ->
            File.close(io)
            Files.remove(path)
          end
        )

      {:ok, stream}
    end
  end

  @doc "Imports a path or enumerable of binary chunks, validates SQLite, and assigns a new ID."
  def import(service, source) do
    c = Cache.config(service)
    path = Files.temp(c)

    try do
      with :ok <- import_file(source, path, c.max_db_bytes),
           :ok <- SQL.validate(path, c.max_db_bytes, c.max_value_bytes) do
        upload_new(c, path)
      end
    after
      Files.scratch_remove(path)
    end
  end

  @doc false
  def valid_id?(id) when is_binary(id),
    do:
      Regex.match?(
        ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/,
        id
      )

  def valid_id?(_), do: false
  defp check_id(id), do: if(valid_id?(id), do: :ok, else: {:error, :invalid_id})

  defp uuid do
    <<a::48, _::4, b::12, _::2, c::62>> = :crypto.strong_rand_bytes(16)

    <<p::binary-size(8), q::binary-size(4), r::binary-size(4), s::binary-size(4),
      t::binary-size(12)>> =
      Base.encode16(<<a::48, 4::4, b::12, 2::2, c::62>>, case: :lower)

    Enum.join([p, q, r, s, t], "-")
  end

  defp upload_new(c, path) do
    id = uuid()

    with {:ok, _etag} <-
           Storage.call(c.storage, :put, [Config.key(c, id), path, :absent, Files.hash(path)]),
         do: {:ok, id}
  end

  defp export_file(_c, _id, 0), do: {:error, :integrity_failure}

  defp export_file(c, id, attempts) do
    path = Files.temp(c)

    case Storage.call(c.storage, :get, [Config.key(c, id), path, nil]) do
      {:ok, etag} ->
        if Files.hash(path) == etag do
          {:ok, path}
        else
          Files.remove(path)
          export_file(c, id, attempts - 1)
        end

      {:error, _} = error ->
        Files.remove(path)
        error
    end
  end

  defp import_file(source, path, cap) do
    source = if is_binary(source), do: File.stream!(source, [], 64 * 1024), else: source

    File.open!(path, [:write, :binary], fn io ->
      Enum.reduce_while(source, 0, fn chunk, size ->
        cond do
          not is_binary(chunk) ->
            {:halt, {:error, :invalid_source}}

          size + byte_size(chunk) > cap ->
            {:halt, {:error, :full}}

          true ->
            :ok = IO.binwrite(io, chunk)
            {:cont, size + byte_size(chunk)}
        end
      end)
    end)
    |> case do
      {:error, _} = error -> error
      _bytes -> :ok
    end
  rescue
    _ in [File.Error, Protocol.UndefinedError] -> {:error, :invalid_source}
  end

  defp inputs(statements, c) when is_list(statements) do
    cond do
      length(statements) > c.max_statements ->
        {:error, :too_many_statements}

      true ->
        Enum.reduce_while(statements, 0, fn item, bytes ->
          result =
            case item do
              sql when is_binary(sql) ->
                {:ok, byte_size(sql)}

              {sql, params} when is_binary(sql) and (is_list(params) or is_map(params)) ->
                values = if is_map(params), do: Map.values(params), else: params

                if Enum.all?(values, &value?/1) and
                     (not is_map(params) or Enum.all?(Map.keys(params), &is_binary/1)) do
                  keys =
                    if is_map(params),
                      do: Enum.reduce(Map.keys(params), 0, &(byte_size(&1) + &2)),
                      else: 0

                  {:ok, byte_size(sql) + keys + Enum.reduce(values, 0, &(input_size(&1) + &2))}
                else
                  {:error, :invalid_params}
                end

              _ ->
                {:error, :invalid_statement}
            end

          case result do
            {:ok, n} when bytes + n <= c.max_request_bytes -> {:cont, bytes + n}
            {:ok, _} -> {:halt, {:error, :request_too_large}}
            error -> {:halt, error}
          end
        end)
        |> case do
          {:error, _} = error -> error
          _ -> :ok
        end
    end
  end

  defp inputs(_, _), do: {:error, :invalid_statements}
  defp value?(nil), do: true
  defp value?(v) when is_number(v) or is_binary(v), do: true
  defp value?({:blob, v}) when is_binary(v), do: true
  defp value?(_), do: false
  defp input_size({:blob, v}), do: byte_size(v)
  defp input_size(v), do: SQL.value_bytes(v)

  defp call(c, id, request, mode, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    dispatch(c, id, request, mode, deadline, nil)
  end

  defp dispatch(c, id, request, mode, deadline, previous) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:error, :timeout}
    else
      owner =
        case :global.whereis_name(Owner.name(c.name, id)) do
          :undefined ->
            case Owner.start(c, id, mode) do
              {:ok, pid} -> pid
              {:error, {:already_started, pid}} -> pid
              {:error, reason} -> {:error, reason}
            end

          pid ->
            pid
        end

      case owner do
        {:error, reason} ->
          {:error, reason}

        pid when pid == previous or pid == :undefined ->
          Process.sleep(min(10, remaining))
          dispatch(c, id, request, mode, deadline, previous)

        pid ->
          try do
            GenServer.call(pid, request, remaining + 100)
          catch
            :exit, {:normal, _} -> dispatch(c, id, request, mode, deadline, pid)
            :exit, {:shutdown, _} -> dispatch(c, id, request, mode, deadline, pid)
            :exit, {:noproc, _} -> dispatch(c, id, request, mode, deadline, pid)
            :exit, {:timeout, _} -> {:error, :timeout}
            :exit, _reason -> {:error, :unknown_outcome}
          end
      end
    end
  end
end
