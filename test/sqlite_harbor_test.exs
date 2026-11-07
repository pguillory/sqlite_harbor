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

  test "only the five parenthesized schema inspection pragmas are allowed", c do
    {:ok, id} = Harbor.create(c.service)

    assert {:ok, _} =
             Harbor.execute(c.service, id, [
               "CREATE TABLE parent(id INTEGER PRIMARY KEY)",
               "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id REFERENCES parent(id), doubled AS (id * 2))",
               "CREATE INDEX child_parent ON child(parent_id)",
               "CREATE TABLE \"odd'name\"(x)",
               "CREATE TABLE \"a; PRAGMA user_version=7\"(x)"
             ])

    assert {:ok, [info, extended, indexes, index, foreign]} =
             Harbor.execute(c.service, id, [
               "PrAgMa MAIN.table_info('child'); ",
               "PRAGMA table_xinfo(child)",
               "PRAGMA index_list(\"child\")",
               "PRAGMA index_info(`child_parent`)",
               "PRAGMA foreign_key_list([child])"
             ])

    assert Enum.map(info.rows, &Enum.at(&1, 1)) == ["id", "parent_id"]
    assert Enum.map(extended.rows, &Enum.at(&1, 1)) == ["id", "parent_id", "doubled"]
    assert Enum.any?(indexes.rows, &(Enum.at(&1, 1) == "child_parent"))
    assert index.rows == [[0, 1, "parent_id"]]
    assert Enum.any?(foreign.rows, &(Enum.at(&1, 2) == "parent"))
    assert_rows(c.service, id, "PRAGMA table_info('odd''name')", [[0, "x", "", 0, nil, 0]])
    assert_rows(c.service, id, "PRAGMA main.table_info(`odd'name`)", [[0, "x", "", 0, nil, 0]])

    assert {:ok, _} = Harbor.execute(c.service, id, ["CREATE TABLE \"a`b\"(x)"])
    assert_rows(c.service, id, "PRAGMA table_info(`a``b`)", [[0, "x", "", 0, nil, 0]])

    assert_rows(c.service, id, "PRAGMA table_info('a; PRAGMA user_version=7')", [
      [0, "x", "", 0, nil, 0]
    ])

    for sql <- [
          "PRAGMA user_version=7",
          "PRAGMA journal_mode(DELETE)",
          "PRAGMA trusted_schema=ON",
          "PRAGMA max_page_count=999999",
          "PRAGMA writable_schema=ON",
          "PRAGMA foreign_keys=OFF",
          "PRAGMA table_info = child",
          "PRAGMA table_info('child'); PRAGMA user_version=7",
          "PRAGMA table_info('child'); SELECT 1",
          "PRAGMA table_info('child') /* outside the accepted grammar */",
          "PRAGMA temp.table_info('child')",
          "SELECT * FROM pragma_table_info('child')"
        ] do
      assert {:error, %{statement: 0}} = Harbor.execute(c.service, id, [sql]), sql
    end

    assert {:error, %{code: :invalid_params}} =
             Harbor.execute(c.service, id, [{"PRAGMA table_info('child')", ["ignored"]}])

    # The relaxed authorizer must end with the introspection statement.
    assert {:error, %{statement: 1, code: :unauthorized}} =
             Harbor.execute(c.service, id, ["PRAGMA table_info('child')", "PRAGMA user_version=7"])

    assert_rows(c.service, id, "SELECT 1", [[1]])
  end

  test "opt-in row truncation finishes RETURNING writes and preserves batch semantics", c do
    opts =
      c.opts
      |> Keyword.put(:name, :harbor_truncate_test)
      |> Keyword.put(:cache_dir, Path.join(c.root, "truncated"))
      |> Keyword.merge(max_rows: 2, max_result_bytes: 100)

    start_supervised!({Harbor, opts})
    service = :harbor_truncate_test
    {:ok, id} = Harbor.create(service)
    assert {:ok, _} = Harbor.execute(service, id, ["CREATE TABLE t(x)"])

    assert {:error, %{code: :result_too_large}} =
             Harbor.execute(service, id, ["INSERT INTO t VALUES(1),(2),(3) RETURNING x"])

    assert_rows(service, id, "SELECT count(*) FROM t", [[0]])

    assert {:ok, [insert, read, write]} =
             Harbor.execute(
               service,
               id,
               [
                 "INSERT INTO t VALUES(1),(2),(3) RETURNING x",
                 "SELECT x FROM t ORDER BY x",
                 "INSERT INTO t VALUES(4)"
               ],
               row_limit: :truncate
             )

    assert length(insert.rows) == 2
    assert insert.truncated
    assert insert.changes == 3
    assert insert.last_insert_rowid == 3
    assert read.rows == [] and read.truncated and read.changes == 0
    assert write.rows == [] and not write.truncated and write.changes == 1
    assert_rows(service, id, "SELECT count(*) FROM t", [[4]])

    assert {:error, %{statement: 1, code: :result_too_large}} =
             Harbor.execute(
               service,
               id,
               [
                 "INSERT INTO t VALUES(9) RETURNING hex(zeroblob(25)) AS x",
                 "SELECT hex(zeroblob(25)) AS x"
               ],
               row_limit: :truncate
             )

    assert_rows(service, id, "SELECT count(*) FROM t", [[4]])

    assert {:ok, [first, second, empty]} =
             Harbor.execute(service, id, ["SELECT 1", "SELECT 2", "SELECT x FROM t WHERE 0"],
               row_limit: :truncate
             )

    assert first.rows == [[1]] and not first.truncated
    assert second.rows == [[2]] and not second.truncated
    refute empty.truncated

    assert {:ok, [%{rows: [[1], [2]], truncated: false}]} =
             Harbor.execute(service, id, ["SELECT 1 UNION ALL SELECT 2"], row_limit: :truncate)

    # Discarded rows do not consume the retained byte budget; each native step
    # remains bounded even after the row cap is reached.
    assert {:ok, [%{rows: rows, truncated: true}]} =
             Harbor.execute(
               service,
               id,
               [
                 "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100) SELECT x FROM n"
               ],
               row_limit: :truncate
             )

    assert rows == [[1], [2]]

    assert {:error, %{statement: 1}} =
             Harbor.execute(
               service,
               id,
               [
                 "INSERT INTO t VALUES(5),(6),(7) RETURNING x",
                 "INSERT INTO missing VALUES(1)"
               ],
               row_limit: :truncate
             )

    assert_rows(service, id, "SELECT count(*) FROM t", [[4]])

    assert {:error, %{code: :result_too_large}} =
             Harbor.execute(service, id, ["SELECT hex(zeroblob(100))"], row_limit: :truncate)

    assert {:error, %{code: :result_too_large}} =
             Harbor.execute(
               service,
               id,
               [
                 "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT zeroblob(101)"
               ],
               row_limit: :truncate
             )

    assert {:error, %{code: :sql_error}} =
             Harbor.execute(
               service,
               id,
               [
                 "WITH n(x) AS (VALUES(1),(2),(3)) SELECT CASE x WHEN 3 THEN json('invalid') ELSE x END FROM n"
               ],
               row_limit: :truncate
             )

    assert {:error, %{code: :timeout}} =
             Harbor.execute(
               service,
               id,
               [
                 "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n) SELECT x FROM n"
               ],
               row_limit: :truncate,
               timeout: 50
             )

    assert {:error, %{code: :invalid_row_limit}} =
             Harbor.execute(service, id, ["INSERT INTO t VALUES(8)"], row_limit: :ignore)

    assert_rows(service, id, "SELECT count(*) FROM t", [[4]])
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
