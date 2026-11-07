defmodule SqliteHarbor.Test.Helpers do
  @moduledoc false
  alias SqliteHarbor.{Cache, Owner}

  def setup_service(context, overrides \\ []) do
    root =
      Path.join(System.tmp_dir!(), "sqlite-harbor-test-#{System.unique_integer([:positive])}")

    File.mkdir_p!(root)
    name = String.to_atom("harbor_test_#{System.unique_integer([:positive])}")

    store =
      ExUnit.Callbacks.start_supervised!(
        {SqliteHarbor.Test.Store, root: Path.join(root, "objects")}
      )

    opts =
      [
        name: name,
        storage: {SqliteHarbor.Test.Store, store},
        cache_dir: Path.join(root, "cache"),
        prefix: "test/",
        idle_timeout: 60_000,
        save_interval: 60_000,
        recheck_interval: 60_000
      ]
      |> Keyword.merge(overrides)

    supervisor = ExUnit.Callbacks.start_supervised!({SqliteHarbor, opts})
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf!(root) end)

    Map.merge(context, %{
      service: name,
      store: store,
      root: root,
      opts: opts,
      supervisor: supervisor
    })
  end

  def owner(service, id), do: :global.whereis_name(Owner.name(service, id))
  def cache(service), do: :global.whereis_name({Cache, node(), service})
  def config(service), do: Cache.config(service)
  def save(service, id), do: GenServer.call(owner(service, id), :save, :infinity)

  def eventually(fun, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    poll(fun, deadline)
  end

  defp poll(fun, deadline) do
    case fun.() do
      false ->
        if System.monotonic_time(:millisecond) >= deadline,
          do: raise("condition did not become true")

        receive do
        after
          5 -> poll(fun, deadline)
        end

      nil ->
        poll(fn -> fun.() || false end, deadline)

      result ->
        result
    end
  end

  def stop_owner(service, id) do
    pid = owner(service, id)
    ref = Process.monitor(pid)
    send(pid, :idle)

    receive do
      {:DOWN, ^ref, :process, ^pid, :normal} -> :ok
    after
      5_000 -> raise "owner did not idle"
    end
  end

  def assert_rows(service, id, sql, rows) do
    case SqliteHarbor.execute(service, id, [sql]) do
      {:ok, [%{rows: ^rows}]} -> :ok
      result -> raise "unexpected query result: #{inspect(result)}"
    end
  end

  def relay(parent) do
    receive do
      :stop ->
        :ok

      message ->
        send(parent, {self(), message})
        relay(parent)
    end
  end
end
