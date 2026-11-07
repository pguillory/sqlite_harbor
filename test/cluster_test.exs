defmodule SqliteHarbor.ClusterTest do
  # Peer connections and the distribution node are VM-wide.
  use ExUnit.Case, async: false
  import SqliteHarbor.Test.Helpers
  alias SqliteHarbor, as: Harbor
  alias SqliteHarbor.{Owner, Test.Store}

  setup_all do
    unless Node.alive?() do
      System.cmd("epmd", ["-daemon"])
      {:ok, _} = Node.start(:"harbor_tests_#{System.unique_integer([:positive])}", :shortnames)
    end

    :ok
  end

  setup context do
    c = setup_service(context)

    peers =
      for index <- 1..2 do
        {:ok, peer, node} =
          :peer.start_link(%{
            name: :"harbor_peer_#{System.unique_integer([:positive])}",
            connection: :standard_io,
            args: [~c"+S", ~c"2", ~c"-pa" | :code.get_path()]
          })

        Process.unlink(peer)
        on_exit(fn -> if Process.alive?(peer), do: :peer.stop(peer) end)
        {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:sqlite_harbor])
        opts = c.opts |> Keyword.put(:cache_dir, Path.join(c.root, "peer-cache-#{index}"))
        {:ok, _} = :erpc.call(node, Supervisor, :start_child, [:kernel_sup, {Harbor, opts}])
        node
      end

    [first, second] = peers
    true = :erpc.call(first, Node, :connect, [second])
    for n <- peers, do: :erpc.call(n, :global, :sync, [])
    Map.put(c, :peers, peers)
  end

  test "calls from two BEAM nodes share one owner and survive an owner crash", c do
    [first, second] = c.peers
    {:ok, id} = :erpc.call(first, Harbor, :create, [c.service])

    assert {:ok, _} =
             :erpc.call(first, Harbor, :execute, [
               c.service,
               id,
               ["CREATE TABLE t(x)", "INSERT INTO t VALUES(1)"]
             ])

    original = :erpc.call(first, :global, :whereis_name, [Owner.name(c.service, id)])
    assert node(original) == first

    assert {:ok, [%{rows: [[1]]}]} =
             :erpc.call(second, Harbor, :execute, [c.service, id, ["SELECT x FROM t"]])

    assert :erpc.call(second, :global, :whereis_name, [Owner.name(c.service, id)]) == original
    :ok = :erpc.call(first, GenServer, :call, [original, :save, :infinity])
    ref = Process.monitor(original)
    :erpc.call(first, Process, :exit, [original, :kill])
    assert_receive {:DOWN, ^ref, :process, ^original, :killed}, 5_000

    assert {:ok, [%{rows: [[1]]}]} =
             :erpc.call(second, Harbor, :execute, [c.service, id, ["SELECT x FROM t"]])

    replacement = :erpc.call(second, :global, :whereis_name, [Owner.name(c.service, id)])
    assert is_pid(replacement) and replacement != original
  end

  test "a request expired while queued at a remote owner never commits", c do
    [first, second] = c.peers
    {:ok, id} = :erpc.call(first, Harbor, :create, [c.service])
    {:ok, _} = :erpc.call(first, Harbor, :execute, [c.service, id, ["CREATE TABLE t(x)"]])
    ref = make_ref()
    Store.fault(c.store, :put, {:block, self(), ref})

    saving =
      Task.async(fn ->
        :erpc.call(first, GenServer, :call, [owner(c.service, id), :save, :infinity])
      end)

    assert_receive {:blocked, ^ref, uploader, _}, 5_000

    queued =
      Task.async(fn ->
        :erpc.call(second, Harbor, :execute, [
          c.service,
          id,
          ["INSERT INTO t VALUES(1)"],
          [timeout: 50]
        ])
      end)

    assert {:error, %{code: :timeout}} = Task.await(queued)
    send(uploader, {:release, ref})
    assert :ok = Task.await(saving)

    assert {:ok, [%{rows: [[0]]}]} =
             :erpc.call(first, Harbor, :execute, [c.service, id, ["SELECT count(*) FROM t"]])
  end

  test "external registry resolver keeps one pid and notifies both across nodes", c do
    [first, second] = c.peers
    a = :erpc.call(first, :erlang, :spawn, [SqliteHarbor.Test.Helpers, :relay, [self()]])
    b = :erpc.call(second, :erlang, :spawn, [SqliteHarbor.Test.Helpers, :relay, [self()]])
    name = {:resolver_test, c.service}
    kept = Owner.resolve(name, a, b)
    assert kept in [a, b]
    assert_receive {^a, {:name_conflict, ^name, ^kept}}
    assert_receive {^b, {:name_conflict, ^name, ^kept}}
    send(a, :stop)
    send(b, :stop)
  end

  test "delete purges connected caches and truncation options reach remote owners", c do
    [first, second] = c.peers
    {:ok, id} = :erpc.call(first, Harbor, :create, [c.service])

    assert {:ok, _} = :erpc.call(first, Harbor, :execute, [c.service, id, ["CREATE TABLE t(x)"]])

    assert {:ok, [%{changes: 10001, truncated: true, rows: rows}]} =
             :erpc.call(second, Harbor, :execute, [
               c.service,
               id,
               [
                 "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10001) INSERT INTO t SELECT x FROM n RETURNING x"
               ],
               [row_limit: :truncate]
             ])

    assert length(rows) == 10000

    paths =
      for n <- [node() | c.peers] do
        conf = :erpc.call(n, SqliteHarbor.Cache, :config, [c.service])
        snapshot = SqliteHarbor.Files.snapshot(conf, id, "old")
        conflict = Path.join([conf.cache_dir, "conflicts", id <> "--old"])
        scratch = SqliteHarbor.Files.temp(conf, id)

        for path <- [snapshot, conflict, scratch],
            do: :ok = :erpc.call(n, File, :write, [path, "private"])

        {n, [snapshot, conflict, scratch]}
      end

    assert :ok = :erpc.call(second, Harbor, :delete, [c.service, id])

    for {n, local_paths} <- paths,
        path <- local_paths,
        do: refute(:erpc.call(n, File, :exists?, [path]))

    assert {:error, %{code: :not_found}} =
             :erpc.call(first, Harbor, :execute, [c.service, id, ["SELECT 1"]])
  end
end
