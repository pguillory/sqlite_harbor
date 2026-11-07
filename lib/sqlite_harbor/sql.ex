defmodule SqliteHarbor.SQL do
  @moduledoc false
  alias Exqlite.Sqlite3, as: DB
  alias SqliteHarbor.{Files, Watchdog}

  @denied [
    :transaction,
    :savepoint,
    :attach,
    :detach,
    :pragma,
    :create_temp_index,
    :create_temp_table,
    :create_temp_trigger,
    :create_temp_view,
    :drop_temp_index,
    :drop_temp_table,
    :drop_temp_trigger,
    :drop_temp_view
  ]

  # The driver authorizer can deny actions, but cannot allow individual PRAGMA
  # names. Only this complete, side-effect-free grammar may lift that denial.
  @schema_pragma ~r/\A\s*PRAGMA\s+(?:main\s*\.\s*)?(table_info|table_xinfo|index_list|index_info|foreign_key_list)\s*\(\s*('(?:[^']|'')*'|"(?:[^"]|"")*"|`(?:[^`]|``)*`|\[[^\]]*\]|[A-Za-z_][A-Za-z0-9_$]*)\s*\)\s*;?\s*\z/i

  def initialize(path, max_value_bytes \\ 10_000_000) do
    with {:ok, db} <- DB.open(path) do
      try do
        with :ok <- DB.execute(db, "PRAGMA trusted_schema=OFF"),
             {:ok, _} <- DB.limit(db, :length, max_value_bytes),
             do: DB.execute(db, "PRAGMA user_version=0")
      after
        DB.close(db)
      end
    end
  end

  def heap_limit(limit) do
    {:ok, db} = DB.open(":memory:")

    try do
      :ok = DB.execute(db, "PRAGMA hard_heap_limit=#{limit}")
    after
      DB.close(db)
    end
  end

  def open(path, c) do
    with {:ok, db} <- DB.open(path, mode: :readwrite) do
      case configure(db, c) do
        :ok ->
          {:ok, db}

        error ->
          DB.close(db)
          error
      end
    end
  end

  defp configure(db, c) do
    with :ok <- DB.execute(db, "PRAGMA trusted_schema=OFF"),
         {:ok, _} <- DB.limit(db, :length, c.max_value_bytes),
         {:ok, [[page_size]]} <- query(db, "PRAGMA page_size"),
         :ok <-
           DB.execute(
             db,
             "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=NORMAL; PRAGMA max_page_count=#{max(1, div(c.max_db_bytes, page_size))}"
           ) do
      :ok
    end
  end

  def validate(path, cap, max_value_bytes \\ 10_000_000) do
    header = File.open!(path, [:read, :binary], &IO.binread(&1, 16))

    cond do
      Files.size(path) > cap ->
        {:error, :full}

      header != <<"SQLite format 3", 0>> ->
        {:error, :invalid_database}

      true ->
        with {:ok, db} <- DB.open(path, mode: :readonly) do
          try do
            with :ok <- DB.execute(db, "PRAGMA trusted_schema=OFF"),
                 {:ok, _} <- DB.limit(db, :length, max_value_bytes),
                 {:ok, [["ok"]]} <- query(db, "PRAGMA quick_check") do
              :ok
            else
              _ -> {:error, :invalid_database}
            end
          after
            DB.close(db)
          end
        end
    end
  end

  def checkpoint(db) do
    case query(db, "PRAGMA wal_checkpoint(TRUNCATE)") do
      {:ok, [[0, log, done]]} when log == done -> :ok
      {:ok, _} -> {:error, :busy}
      error -> error
    end
  end

  def query(db, sql) do
    with {:ok, stmt} <- DB.prepare(db, sql) do
      try do
        DB.fetch_all(db, stmt)
      after
        DB.release(db, stmt)
      end
    end
  end

  def execute(db, statements, c, deadline, watchdog, row_limit \\ :error) do
    token = make_ref()
    :ok = Watchdog.arm(watchdog, db, deadline, token)

    try do
      check_deadline(deadline)
      :ok = DB.set_authorizer(db, [])
      must(DB.execute(db, "BEGIN"))
      :ok = DB.set_authorizer(db, @denied)

      {results, _, _} =
        statements
        |> Enum.with_index()
        |> Enum.reduce({[], 0, 0}, fn {item, index}, {results, rows, bytes} ->
          try do
            {result, count, size} = statement(db, item, c, deadline, rows, bytes, row_limit)
            {[result | results], count, size}
          catch
            {:sql_error, reason} -> throw({:batch_error, error(index, reason)})
          end
        end)

      check_deadline(deadline)
      :ok = DB.set_authorizer(db, [])
      must(DB.execute(db, "COMMIT"))
      {:ok, Enum.reverse(results)}
    catch
      {:batch_error, result} ->
        rollback(db)
        {:error, result}

      {:sql_error, reason} ->
        rollback(db)
        {:error, error(nil, reason)}
    after
      DB.set_authorizer(db, [])
      Watchdog.disarm(watchdog, token)
    end
  end

  defp rollback(db) do
    DB.set_authorizer(db, [])
    # SQLITE_FULL and interrupts can already roll the entire transaction back.
    case DB.transaction_status(db) do
      {:ok, :transaction} -> DB.execute(db, "ROLLBACK")
      {:ok, :idle} -> :ok
    end
  end

  defp statement(db, item, c, deadline, row_count, byte_count, row_limit) do
    check_deadline(deadline)
    {sql, params} = if is_binary(item), do: {item, []}, else: item
    {sql, denied} = authorized_sql(sql)
    :ok = DB.set_authorizer(db, denied)

    try do
      if denied != @denied and params not in [[], %{}],
        do: throw({:sql_error, :invalid_params})

      stmt = must(DB.prepare_single(db, sql))

      try do
        must(DB.bind(stmt, params))
        {:ok, [[before_changes]]} = query(db, "SELECT total_changes()")
        columns = must(DB.columns(db, stmt))
        column_bytes = Enum.reduce(columns, 0, &(byte_size(&1) + &2))
        byte_count = byte_count + column_bytes
        if byte_count > c.max_result_bytes, do: throw({:sql_error, :result_too_large})

        {rows, count, bytes, truncated} =
          rows(db, stmt, c, deadline, [], row_count, byte_count, row_limit, false)

        {:ok, [[after_changes]]} = query(db, "SELECT total_changes()")
        changes = after_changes - before_changes
        rowid = must(DB.last_insert_rowid(db))

        {%{
           columns: columns,
           rows: rows,
           changes: changes,
           last_insert_rowid: rowid,
           truncated: truncated
         }, count, bytes}
      after
        DB.release(db, stmt)
      end
    rescue
      e in [ArgumentError, Exqlite.Error] -> throw({:sql_error, Exception.message(e)})
    after
      DB.set_authorizer(db, @denied)
    end
  end

  defp authorized_sql(sql) do
    case Regex.run(@schema_pragma, sql, capture: :all_but_first) do
      [name, argument] ->
        {"PRAGMA main.#{String.downcase(name)}(#{argument})", List.delete(@denied, :pragma)}

      nil ->
        {sql, @denied}
    end
  end

  defp rows(db, stmt, c, deadline, acc, count, bytes, row_limit, truncated) do
    check_deadline(deadline)
    discard = row_limit == :truncate and count >= c.max_rows
    budget = if discard, do: c.max_result_bytes, else: c.max_result_bytes - bytes

    # The native budget preflights every column before copying any result cells.
    # Discarded rows have the same per-row bound but consume no retained budget.
    case DB.step(db, stmt, budget) do
      :done ->
        {Enum.reverse(acc), count, bytes, truncated}

      {:row, _row} when discard ->
        rows(db, stmt, c, deadline, acc, count, bytes, row_limit, true)

      {:row, row} ->
        if count >= c.max_rows, do: throw({:sql_error, :result_too_large})
        bytes = bytes + Enum.reduce(row, 0, &(value_bytes(&1) + &2))
        rows(db, stmt, c, deadline, [row | acc], count + 1, bytes, row_limit, truncated)

      {:error, reason} ->
        throw({:sql_error, reason})

      :busy ->
        throw({:sql_error, :busy})
    end
  end

  def value_bytes(nil), do: 1
  def value_bytes(:out_of_memory), do: throw({:sql_error, :out_of_memory})
  def value_bytes(value) when is_binary(value), do: byte_size(value)
  def value_bytes(value) when is_number(value), do: 8

  def check_deadline(deadline) do
    if System.monotonic_time(:millisecond) >= deadline, do: throw({:sql_error, :timeout})
  end

  defp must(:ok), do: :ok
  defp must({:ok, value}), do: value
  defp must({:error, reason}), do: throw({:sql_error, reason})

  def error(index, reason) do
    message = if is_binary(reason), do: reason, else: inspect(reason)

    code =
      cond do
        is_atom(reason) -> reason
        String.contains?(message, "not authorized") -> :unauthorized
        String.contains?(message, "full") -> :full
        String.contains?(message, "too big") -> :too_big
        String.contains?(message, "out of memory") -> :out_of_memory
        String.contains?(message, "interrupt") -> :timeout
        String.contains?(message, "constraint") -> :constraint
        true -> :sql_error
      end

    %{statement: index, code: code, message: message}
  end
end
