defmodule SqliteHarbor.LifecycleTest do
  use ExUnit.Case, async: true
  import SqliteHarbor.Test.Helpers
  alias SqliteHarbor, as: Harbor
  alias SqliteHarbor.{Files, SQL, Storage.Local}
  alias SqliteHarbor.Test.Store

  setup context, do: setup_service(context)

  defp seeded(c) do
    {:ok, id} = Harbor.create(c.service)
    {:ok, _} = Harbor.execute(c.service, id, ["CREATE TABLE t(x)", "INSERT INTO t VALUES(1)"])
    id
  end

  defp file_rows(path) do
    {:ok, db} = Exqlite.Sqlite3.open(path, mode: :readonly)

    try do
      :ok = Exqlite.Sqlite3.execute(db, "PRAGMA trusted_schema=OFF")
      {:ok, rows} = SQL.query(db, "SELECT x FROM t ORDER BY x")
      rows
    after
      Exqlite.Sqlite3.close(db)
    end
  end

  test "immutable save snapshots allow newer commits and follow up with a fresh save", c do
    id = seeded(c)
    ref = make_ref()
    Store.fault(c.store, :put, {:block, self(), ref})
    send(owner(c.service, id), :save)
    assert_receive {:blocked, ^ref, uploader, [_key, snapshot, _, _]}, 5_000
    assert file_rows(snapshot) == [[1]]
    {:ok, _} = Harbor.execute(c.service, id, ["INSERT INTO t VALUES(2)"])
    assert file_rows(snapshot) == [[1]]
    send(uploader, {:release, ref})
    assert :ok = save(c.service, id)
    {:ok, stream} = Harbor.export(c.service, id)
    {:ok, imported} = Harbor.import(c.service, stream)
    assert_rows(c.service, imported, "SELECT x FROM t ORDER BY x", [[1], [2]])
  end

  test "unknown successful PUT is retried with the same snapshot and adopted by MD5", c do
    id = seeded(c)
    Store.fault(c.store, :put, {:land_then_error, :unavailable})
    assert {:error, :unavailable} = save(c.service, id)
    meta = Files.meta(config(c.service), id)
    assert is_binary(meta.pending)
    assert :ok = save(c.service, id)
    puts = Store.calls(c.store) |> Enum.filter(fn {op, _} -> op == :put end) |> Enum.drop(1)
    assert [{:put, [_, same, condition, digest]}, {:put, [_, same, condition, digest]}] = puts
    assert Path.wildcard(Path.join([c.root, "cache", "conflicts", "*"])) == []
    assert Files.meta(config(c.service), id).pending == nil
  end

  test "owner crash recovers pending snapshot and post-snapshot acknowledged writes", c do
    id = seeded(c)
    ref = make_ref()
    Store.fault(c.store, :put, {:block, self(), ref})
    original = owner(c.service, id)
    send(original, :save)
    assert_receive {:blocked, ^ref, _uploader, _}, 5_000
    {:ok, _} = Harbor.execute(c.service, id, ["INSERT INTO t VALUES(2)"])
    monitor = Process.monitor(original)
    Process.exit(original, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^original, :killed}

    eventually(fn ->
      p = owner(c.service, id)
      is_pid(p) and p != original
    end)

    assert_rows(c.service, id, "SELECT x FROM t ORDER BY x", [[1], [2]])
    assert :ok = save(c.service, id)
    assert file_rows(Path.join([c.root, "objects", "test", "db", id])) == [[1], [2]]
  end

  test "conflict rescue includes writes committed after the losing snapshot", c do
    id = seeded(c)
    assert :ok = save(c.service, id)
    {:ok, _} = Harbor.execute(c.service, id, ["INSERT INTO t VALUES(2)"])
    ref = make_ref()
    Store.fault(c.store, :put, {:block, self(), ref})
    original = owner(c.service, id)
    monitor = Process.monitor(original)
    send(original, :save)
    assert_receive {:blocked, ^ref, uploader, _}, 5_000
    {:ok, _} = Harbor.execute(c.service, id, ["INSERT INTO t VALUES(3)"])
    foreign = Path.join(c.root, "foreign.sqlite")
    :ok = SQL.initialize(foreign)
    {:ok, db} = Exqlite.Sqlite3.open(foreign)
    :ok = Exqlite.Sqlite3.execute(db, "CREATE TABLE t(x); INSERT INTO t VALUES(99)")
    :ok = Exqlite.Sqlite3.close(db)

    assert {:ok, _} =
             Local.put(
               [root: Path.join(c.root, "objects")],
               "test/db/" <> id,
               foreign,
               :none,
               Files.hash(foreign)
             )

    send(uploader, {:release, ref})
    assert_receive {:DOWN, ^monitor, :process, ^original, :normal}, 5_000

    rescued =
      eventually(fn ->
        {:ok, keys, _} =
          Local.list([root: Path.join(c.root, "objects")], "test/conflicts/", nil, 100)

        List.first(keys)
      end)

    assert file_rows(Path.join([c.root, "objects", "test", "conflicts", rescued])) == [
             [1],
             [2],
             [3]
           ]

    assert_rows(c.service, id, "SELECT x FROM t", [[99]])
  end

  test "failed idle save resumes serving without marking its file clean", c do
    id = seeded(c)
    Store.fault(c.store, :put, {:error, :unavailable})
    send(owner(c.service, id), :idle)
    eventually(fn -> Files.meta(config(c.service), id).pending != nil end)
    assert_rows(c.service, id, "SELECT x FROM t", [[1]])
    assert Files.meta(config(c.service), id).status == :dirty
    assert :ok = save(c.service, id)
  end

  test "failed forced save retries the pending snapshot before the normal save interval", c do
    id = seeded(c)
    Store.fault(c.store, :put, {:error, :unavailable})
    ref = make_ref()
    Store.fault(c.store, :put, {:block, self(), ref})
    assert {:error, :unavailable} = save(c.service, id)
    pending = Files.meta(config(c.service), id).pending
    assert_receive {:blocked, ^ref, uploader, [_, path, _, ^pending]}, 4_000
    assert Path.basename(path) == id <> "." <> pending
    assert_rows(c.service, id, "SELECT x FROM t", [[1]])
    send(uploader, {:release, ref})
    assert :ok = save(c.service, id)
  end

  test "delete holds ownership until remote deletion succeeds", c do
    id = seeded(c)
    ref = make_ref()
    Store.fault(c.store, :delete, {:block, self(), ref})
    deletion = Task.async(fn -> Harbor.delete(c.service, id) end)
    assert_receive {:blocked, ^ref, deleter, _}, 5_000
    read = Task.async(fn -> Harbor.execute(c.service, id, ["SELECT x FROM t"]) end)
    send(deleter, {:release, ref})
    assert :ok = Task.await(deletion)
    assert {:error, %{code: :not_found}} = Task.await(read)
    assert {:ok, [], nil} = Harbor.list(c.service, nil, 100)
  end

  test "delete failure preserves both remote and local committed contents", c do
    id = seeded(c)
    Store.fault(c.store, :delete, {:error, :unavailable})
    assert {:error, :unavailable} = Harbor.delete(c.service, id)
    assert_rows(c.service, id, "SELECT x FROM t", [[1]])
    assert :ok = save(c.service, id)
  end

  test "a DELETE that landed before its error never resurrects the object", c do
    id = seeded(c)
    Store.fault(c.store, :delete, {:land_then_error, :unavailable})
    assert {:error, :unavailable} = Harbor.delete(c.service, id)
    assert {:ok, [], nil} = Harbor.list(c.service, nil, 100)
    assert {:error, :not_found} = save(c.service, id)
    assert {:error, %{code: :not_found}} = Harbor.execute(c.service, id, ["SELECT x FROM t"])
    assert :ok = Harbor.delete(c.service, id)
  end

  test "delete-mode removes a crashed predecessor's pending snapshot", c do
    {:ok, id} = Harbor.create(c.service)
    conf = config(c.service)
    path = Files.path(conf, id)
    :ok = SQL.initialize(path)
    digest = Files.hash(path)
    Files.copy(path, Files.snapshot(conf, id, digest))
    Files.put_meta(conf, id, digest, digest, :dirty)
    assert :ok = Harbor.delete(c.service, id)
    refute File.exists?(Files.snapshot(conf, id, digest))
    refute File.exists?(path)
    refute File.exists?(Files.meta_path(conf, id))
  end

  test "normal supervisor shutdown flushes acknowledged local writes", c do
    id = seeded(c)
    assert :ok = stop_supervised({Harbor, c.service})
    assert file_rows(Path.join([c.root, "objects", "test", "db", id])) == [[1]]
    start_supervised!({Harbor, c.opts})
    assert_rows(c.service, id, "SELECT x FROM t", [[1]])
  end

  test "a live export stream survives Cache restarting in the same VM", c do
    id = seeded(c)
    {:ok, stream} = Harbor.export(c.service, id)
    old = cache(c.service)
    Process.exit(old, :kill)

    eventually(fn ->
      pid = cache(c.service)
      is_pid(pid) and pid != old
    end)

    assert <<"SQLite format 3", 0, _::binary>> = Enum.join(stream)
    assert_rows(c.service, id, "SELECT x FROM t", [[1]])
  end

  test "an abandoned delete-mode activation releases the name at idle", c do
    {:ok, id} = Harbor.create(c.service)
    {:ok, pid} = SqliteHarbor.Owner.start(config(c.service), id, :delete)
    ref = Process.monitor(pid)
    send(pid, :idle)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    assert_rows(c.service, id, "SELECT 1", [[1]])
  end

  test "Cache crash kills owners without graceful save and recovers leftovers", c do
    id = seeded(c)
    old_cache = cache(c.service)
    original = owner(c.service, id)
    owner_ref = Process.monitor(original)
    ref = make_ref()
    Store.fault(c.store, :head, {:block, self(), ref})
    initial_puts = Enum.count(Store.calls(c.store), fn {op, _} -> op == :put end)
    Process.exit(old_cache, :kill)
    assert_receive {:DOWN, ^owner_ref, :process, ^original, :killed}, 5_000
    assert_receive {:blocked, ^ref, recovery, _}, 5_000
    assert Enum.count(Store.calls(c.store), fn {op, _} -> op == :put end) == initial_puts
    assert Files.meta(config(c.service), id).status == :dirty
    send(recovery, {:release, ref})
    assert_rows(c.service, id, "SELECT x FROM t", [[1]])
    assert :ok = save(c.service, id)
    assert file_rows(Path.join([c.root, "objects", "test", "db", id])) == [[1]]
  end

  test "watchdog cancels a running statement and leaves the connection usable", c do
    id = seeded(c)

    assert {:error, %{code: :timeout}} =
             Harbor.execute(
               c.service,
               id,
               [
                 "WITH RECURSIVE nums(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM nums) SELECT sum(x) FROM nums"
               ],
               timeout: 100
             )

    assert_rows(c.service, id, "SELECT x FROM t", [[1]])
  end

  test "boot adoption recognizes a landed pending upload and saves newer WAL writes", c do
    id = seeded(c)
    Store.fault(c.store, :put, {:land_then_error, :unavailable})
    assert {:error, :unavailable} = save(c.service, id)
    {:ok, _} = Harbor.execute(c.service, id, ["INSERT INTO t VALUES(2)"])
    :ok = SqliteHarbor.Cache.drain(cache(c.service))
    pid = owner(c.service, id)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
    :ok = stop_supervised({Harbor, c.service})
    start_supervised!({Harbor, c.opts})
    eventually(fn -> is_pid(owner(c.service, id)) end)
    assert_rows(c.service, id, "SELECT x FROM t ORDER BY x", [[1], [2]])
    assert :ok = save(c.service, id)
    assert file_rows(Path.join([c.root, "objects", "test", "db", id])) == [[1], [2]]
  end

  test "missing pending snapshot is quarantined before primary data is downloaded", c do
    id = seeded(c)
    Store.fault(c.store, :put, {:error, :unavailable})
    assert {:error, :unavailable} = save(c.service, id)
    conf = config(c.service)
    md5 = Files.meta(conf, id).pending
    :ok = SqliteHarbor.Cache.drain(cache(c.service))
    pid = owner(c.service, id)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
    :ok = stop_supervised({Harbor, c.service})
    Files.remove(Files.snapshot(conf, id, md5))
    start_supervised!({Harbor, c.opts})
    assert_rows(c.service, id, "SELECT count(*) FROM sqlite_schema", [[0]])

    rescued =
      eventually(fn ->
        {:ok, keys, _} =
          Local.list([root: Path.join(c.root, "objects")], "test/conflicts/", nil, 100)

        List.first(keys)
      end)

    assert file_rows(Path.join([c.root, "objects", "test", "conflicts", rescued])) == [[1]]
  end

  test "cache eviction removes only clean unheld files", c do
    id = seeded(c)
    opts = Keyword.put(c.opts, :cache_budget_bytes, 1)
    :ok = stop_supervised({Harbor, c.service})
    start_supervised!({Harbor, opts})
    conf = config(c.service)
    eventually(fn -> not File.exists?(Files.path(conf, id)) end)
    assert_rows(c.service, id, "SELECT x FROM t", [[1]])
    send(cache(c.service), :sweep)
    # The active checkout protects its database even with a one-byte clean budget.
    :sys.get_state(cache(c.service))
    assert File.exists?(Files.path(conf, id))
  end

  test "expired queued request never runs after an earlier blocking save", c do
    id = seeded(c)
    ref = make_ref()
    Store.fault(c.store, :put, {:block, self(), ref})
    exporting = Task.async(fn -> Harbor.export(c.service, id) end)
    assert_receive {:blocked, ^ref, uploader, _}, 5_000

    queued =
      Task.async(fn ->
        Harbor.execute(c.service, id, ["INSERT INTO t VALUES(2)"], timeout: 50)
      end)

    assert {:error, %{code: :timeout}} = Task.await(queued)
    send(uploader, {:release, ref})
    {:ok, stream} = Task.await(exporting)
    Enum.to_list(stream)
    assert_rows(c.service, id, "SELECT x FROM t", [[1]])
  end
end
