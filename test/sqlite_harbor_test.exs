defmodule SqliteHarborTest do
  use ExUnit.Case, async: true
  import SqliteHarbor.Test.Helpers
  alias SqliteHarbor, as: Harbor
  alias SqliteHarbor.Test.Store

  setup context, do: setup_service(context)

  test "create, transactional execution, parameter types, pagination and deletion", c do
    {:ok, id} = Harbor.create(c.service)
    {:ok, other} = Harbor.create(c.service)
    assert {:ok, [first], cursor} = Harbor.list(c.service, nil, 1)
    assert cursor
    assert {:ok, [second], nil} = Harbor.list(c.service, cursor, 1)
    assert Enum.sort([first, second]) == Enum.sort([id, other])

    assert {:ok, [_, _, %{rows: [[1, "a", <<0, 255>>, nil, 1.5]]}]} =
             Harbor.execute(c.service, id, [
               "CREATE TABLE t (n INTEGER PRIMARY KEY, text, bytes, empty, f)",
               {"INSERT INTO t VALUES (:n, :t, :b, :e, :f)",
                %{":n" => 1, ":t" => "a", ":b" => {:blob, <<0, 255>>}, ":e" => nil, ":f" => 1.5}},
               "SELECT * FROM t"
             ])

    assert {:error, %{statement: 1}} =
             Harbor.execute(c.service, id, [
               "INSERT INTO t VALUES (2,'b',NULL,NULL,0)",
               "INSERT INTO missing VALUES (1)"
             ])

    assert_rows(c.service, id, "SELECT count(*) FROM t", [[1]])
    assert :ok = Harbor.delete(c.service, id)
    assert :ok = Harbor.delete(c.service, id)
    assert {:error, %{code: :not_found}} = Harbor.execute(c.service, id, ["SELECT 1"])
    assert {:ok, [^other], nil} = Harbor.list(c.service, nil, 100)
  end

  test "SQL transaction controls, temp objects, pragmas, attachments and tails are refused", c do
    {:ok, id} = Harbor.create(c.service)

    for sql <- [
          "BEGIN",
          "COMMIT",
          "ROLLBACK",
          "SAVEPOINT x",
          "RELEASE x",
          "PRAGMA user_version",
          "SELECT * FROM pragma_table_info('t')",
          "ATTACH DATABASE ':memory:' AS aux",
          "CREATE TEMP TABLE temp_table(x)",
          "SELECT 1; SELECT 2",
          "SELECT 1; PRAGMA user_version=3",
          "SELECT 1; nonsense",
          "SELECT 1\0"
        ] do
      assert {:error, %{statement: 0}} = Harbor.execute(c.service, id, [sql]), sql
    end

    assert {:ok, _} =
             Harbor.execute(c.service, id, [
               "CREATE TABLE t(x)",
               "CREATE TRIGGER tr AFTER INSERT ON t BEGIN UPDATE t SET x=2; UPDATE t SET x=3; END; -- okay",
               "INSERT INTO t VALUES(1)"
             ])

    assert_rows(c.service, id, "SELECT x FROM t", [[3]])

    assert {:error, _} =
             Harbor.execute(c.service, id, ["VACUUM INTO '/tmp/harbor-unwanted.sqlite'"])

    assert_rows(c.service, id, "SELECT name FROM sqlite_schema WHERE type='table'", [["t"]])
  end

  test "export/import round trips both new files and saved WAL-header files", c do
    {:ok, fresh} = Harbor.create(c.service)
    {:ok, fresh_stream} = Harbor.export(c.service, fresh)
    fresh_bytes = Enum.join(fresh_stream)
    assert <<"SQLite format 3", 0, _::binary>> = fresh_bytes
    assert {:ok, imported_fresh} = Harbor.import(c.service, [fresh_bytes])
    assert_rows(c.service, imported_fresh, "SELECT count(*) FROM sqlite_schema", [[0]])

    {:ok, _} =
      Harbor.execute(c.service, fresh, ["CREATE TABLE t(x)", "INSERT INTO t VALUES ('hello')"])

    {:ok, stream} = Harbor.export(c.service, fresh)
    bytes = Enum.join(stream)
    assert :binary.at(bytes, 18) == 2
    assert {:ok, imported} = Harbor.import(c.service, [bytes])
    assert_rows(c.service, imported, "SELECT x FROM t", [["hello"]])
    assert Path.wildcard(Path.join([c.root, "cache", "tmp", "*"])) == []
    assert {:error, :invalid_database} = Harbor.import(c.service, ["garbage"])
    assert {:error, :invalid_source} = Harbor.import(c.service, [:invalid])
  end

  test "export retries a corrupted transfer and checks integrity", c do
    {:ok, id} = Harbor.create(c.service)
    assert_rows(c.service, id, "SELECT 1", [[1]])
    Store.fault(c.store, :get, {:corrupt_get, "wrong"})
    assert {:ok, stream} = Harbor.export(c.service, id)
    assert <<"SQLite format 3", 0, _::binary>> = Enum.join(stream)
    Store.fault(c.store, :get, {:corrupt_get, "wrong"})
    Store.fault(c.store, :get, {:corrupt_get, "wrong again"})
    assert {:error, :integrity_failure} = Harbor.export(c.service, id)
  end

  test "clean cached files revalidate with a conditional GET", c do
    {:ok, id} = Harbor.create(c.service)
    {:ok, _} = Harbor.execute(c.service, id, ["CREATE TABLE t(x)", "INSERT INTO t VALUES(7)"])
    stop_owner(c.service, id)
    assert_rows(c.service, id, "SELECT x FROM t", [[7]])

    assert Enum.any?(Store.calls(c.store), fn
             {:get, [key, _, etag]} -> String.ends_with?(key, id) and is_binary(etag)
             _ -> false
           end)
  end

  test "input and output limits reject the batch and roll back writes", c do
    # Distinct instance for deliberately low per-call limits.
    opts =
      c.opts
      |> Keyword.put(:name, :harbor_limit_test)
      |> Keyword.put(:cache_dir, Path.join(c.root, "limited"))
      |> Keyword.merge(
        max_statements: 3,
        max_request_bytes: 500,
        max_rows: 2,
        max_result_bytes: 100
      )

    start_supervised!({Harbor, opts})
    service = :harbor_limit_test
    {:ok, id} = Harbor.create(service)

    assert {:error, %{code: :too_many_statements}} =
             Harbor.execute(service, id, List.duplicate("SELECT 1", 4))

    assert {:error, %{code: :request_too_large}} =
             Harbor.execute(service, id, [{"SELECT ?", [String.duplicate("x", 501)]}])

    assert {:ok, _} = Harbor.execute(service, id, ["CREATE TABLE t(x)"])

    assert {:error, %{code: :result_too_large, statement: 1}} =
             Harbor.execute(service, id, [
               "INSERT INTO t VALUES(1)",
               "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3"
             ])

    assert_rows(service, id, "SELECT count(*) FROM t", [[0]])

    assert {:error, %{code: :result_too_large}} =
             Harbor.execute(service, id, ["SELECT hex(zeroblob(100))"])

    assert {:error, %{code: :invalid_params}} =
             Harbor.execute(service, id, [{"SELECT ?", [self()]}])

    assert {:error, %{code: :invalid_id}} = Harbor.execute(service, "../escape", ["SELECT 1"])
  end

  test "page cap rejects growth and import size is bounded", c do
    opts =
      c.opts
      |> Keyword.put(:name, :harbor_cap_test)
      |> Keyword.put(:cache_dir, Path.join(c.root, "capped"))
      |> Keyword.put(:max_db_bytes, 12_288)

    start_supervised!({Harbor, opts})
    {:ok, id} = Harbor.create(:harbor_cap_test)
    assert {:ok, _} = Harbor.execute(:harbor_cap_test, id, ["CREATE TABLE t(x)"])

    assert {:error, %{code: :full}} =
             Harbor.execute(:harbor_cap_test, id, ["INSERT INTO t VALUES(zeroblob(20000))"])

    assert_rows(:harbor_cap_test, id, "SELECT count(*) FROM t", [[0]])
    assert {:error, :full} = Harbor.import(:harbor_cap_test, [String.duplicate("x", 12_289)])
  end

  test "result cap stops before the following SQLite row is evaluated", c do
    opts =
      c.opts
      |> Keyword.put(:name, :harbor_one_row_test)
      |> Keyword.put(:cache_dir, Path.join(c.root, "one-row"))
      |> Keyword.put(:max_result_bytes, 100)

    start_supervised!({Harbor, opts})
    {:ok, id} = Harbor.create(:harbor_one_row_test)
    # With multi_step(..., 50), SQLite evaluates row two and returns malformed JSON
    # before the library sees the first 200-byte row. One-row stepping must reject
    # that first row instead, without evaluating the error-producing next row.
    sql =
      "WITH n(x) AS (VALUES(1),(2)) SELECT CASE x WHEN 1 THEN zeroblob(200) ELSE json('invalid') END AS value FROM n"

    assert {:error, %{code: :result_too_large, statement: 0}} =
             Harbor.execute(:harbor_one_row_test, id, [sql])

    assert_rows(:harbor_one_row_test, id, "SELECT 1", [[1]])
  end

  test "an impossible fresh-file size cap is rejected before uploading", c do
    opts =
      c.opts
      |> Keyword.put(:name, :harbor_small_cap_test)
      |> Keyword.put(:cache_dir, Path.join(c.root, "small-cap"))
      |> Keyword.put(:max_db_bytes, 1)

    start_supervised!({Harbor, opts})
    assert {:error, :full} = Harbor.create(:harbor_small_cap_test)
    assert {:ok, [], nil} = Harbor.list(:harbor_small_cap_test, nil, 100)
  end

  test "SQLite length limit refuses oversized generated values before copying results", c do
    {:ok, id} = Harbor.create(c.service)
    # length(zeroblob) is cheap even if the limit regresses: SQLite represents an
    # unmaterialized zero BLOB. The configured length cap must reject it anyway.
    assert {:error, %{code: :too_big, message: message}} =
             Harbor.execute(c.service, id, ["SELECT length(zeroblob(900000000))"])

    assert message =~ "too big"

    assert {:error, %{code: :too_big}} =
             Harbor.execute(c.service, id, ["SELECT zeroblob(10000001)"])

    assert_rows(c.service, id, "SELECT 1", [[1]])
  end

  test "statement change counts do not carry over to reads or DDL", c do
    {:ok, id} = Harbor.create(c.service)

    assert {:ok, [create, insert, select, ddl]} =
             Harbor.execute(c.service, id, [
               "CREATE TABLE t(x)",
               "INSERT INTO t VALUES(1)",
               "SELECT x FROM t",
               "CREATE TABLE other(x)"
             ])

    assert create.changes == 0
    assert insert.changes == 1
    assert select.changes == 0
    assert ddl.changes == 0
  end

  test "a wide SELECT row cannot amplify a small repeated parameter past its byte cap", c do
    opts =
      c.opts
      |> Keyword.put(:name, :harbor_wide_row_test)
      |> Keyword.put(:cache_dir, Path.join(c.root, "wide-row"))
      |> Keyword.put(:max_result_bytes, 10_000)

    start_supervised!({Harbor, opts})
    {:ok, id} = Harbor.create(:harbor_wide_row_test)
    sql = "SELECT " <> Enum.join(List.duplicate("?1", 2000), ",")
    # Safe fixture: 2MB if the guard regresses, with the same shared-value shape
    # as the 900KB × 2000 case. The native driver test checks its pre-copy error.
    assert {:error, %{code: :result_too_large, statement: 0}} =
             Harbor.execute(:harbor_wide_row_test, id, [{sql, [String.duplicate("x", 1000)]}])

    assert_rows(:harbor_wide_row_test, id, "SELECT 1", [[1]])
  end
end
