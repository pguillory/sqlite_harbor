defmodule SqliteHarbor.Owner do
  @moduledoc false
  use GenServer, restart: :temporary
  alias Exqlite.Sqlite3, as: DB
  alias SqliteHarbor.{Cache, Config, Files, SQL, Storage, Watchdog}

  def name(service, id), do: {SqliteHarbor, service, id}

  def child_spec({c, id, mode}) do
    %{
      id: {__MODULE__, id},
      start: {__MODULE__, :start_link, [{c, id, mode}]},
      restart: :temporary,
      shutdown: c.shutdown_timeout
    }
  end

  def start(c, id, mode) do
    DynamicSupervisor.start_child(
      Config.local(SqliteHarbor.Owners, c.name),
      {__MODULE__, {c, id, mode}}
    )
  catch
    :exit, {:noproc, _} -> {:error, :starting}
  end

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  # External, total resolver: keep one name registered while owners reconcile.
  def resolve(name, first, second) do
    kept = if {node(first), first} <= {node(second), second}, do: first, else: second
    send(first, {:name_conflict, name, kept})
    send(second, {:name_conflict, name, kept})
    kept
  end

  def init({c, id, mode}) do
    Process.flag(:trap_exit, true)
    started = System.monotonic_time(:millisecond)

    case :global.register_name(name(c.name, id), self(), &__MODULE__.resolve/3) do
      :yes ->
        Config.event(
          :registration,
          %{duration_ms: System.monotonic_time(:millisecond) - started},
          %{service: c.name, id: id}
        )

        cache = :global.whereis_name({Cache, node(), c.name})

        s = %{
          c: c,
          id: id,
          cache: cache,
          mode: mode,
          db: nil,
          watchdog: nil,
          base: nil,
          pending: nil,
          task: nil,
          gen: 0,
          saved_gen: 0,
          error: nil,
          idle: nil,
          save_timer: nil,
          retry: 1_000,
          closed: false,
          waiters: []
        }

        {:ok, s, {:continue, :activate}}

      :no ->
        {:stop, {:already_started, :global.whereis_name(name(c.name, id))}}
    end
  end

  def handle_continue(:activate, s) do
    case Cache.checkout(s.cache, s.id) do
      :ok ->
        {:ok, watchdog} = Watchdog.start_link(self(), s.cache)
        s = %{s | watchdog: watchdog}
        result = if s.mode == :delete, do: {:ok, s}, else: activate(s)

        case result do
          {:ok, %{mode: :delete} = s} -> {:noreply, idle_timer(s)}
          {:ok, s} -> {:noreply, timers(s)}
          {:error, reason, s} -> {:noreply, %{s | error: reason}}
        end

      {:error, reason} ->
        {:noreply, %{s | error: reason}}
    end
  end

  def handle_call(_request, _from, %{error: reason} = s) when not is_nil(reason),
    do: {:stop, :normal, {:error, reason}, s}

  def handle_call({:execute, _, _} = request, from, %{mode: :delete} = s),
    do: {:noreply, %{s | waiters: [{from, request} | s.waiters]}}

  def handle_call({:execute, _, _, _} = request, from, %{mode: :delete} = s),
    do: {:noreply, %{s | waiters: [{from, request} | s.waiters]}}

  def handle_call(:save, from, %{mode: :delete} = s),
    do: {:noreply, %{s | waiters: [{from, :save} | s.waiters]}}

  def handle_call({:execute, statements, expiry}, from, s),
    do: handle_call({:execute, statements, expiry, :error}, from, s)

  def handle_call({:execute, statements, expiry, row_limit}, _from, s) do
    started = System.monotonic_time(:millisecond)
    deadline = started + max(0, expiry - System.system_time(:millisecond))
    before = Files.size(Files.path(s.c, s.id) <> "-wal")
    result = SQL.execute(s.db, statements, s.c, deadline, s.watchdog, row_limit)
    after_size = Files.size(Files.path(s.c, s.id) <> "-wal")
    s = if after_size > before, do: %{s | gen: s.gen + 1}, else: s
    if after_size >= 4 * 1024 * 1024, do: SQL.checkpoint(s.db)

    Config.event(:execute, %{duration_ms: System.monotonic_time(:millisecond) - started}, %{
      service: s.c.name,
      id: s.id,
      outcome: elem(result, 0)
    })

    {:reply, result, idle_timer(s)}
  end

  def handle_call(:save, _from, s) do
    case flush(s) do
      {:ok, s} ->
        {:reply, :ok, idle_timer(s)}

      {:error, reason, s} ->
        unsaved(s, reason)

        {:reply, {:error, reason},
         s |> idle_timer() |> save_timer(if(reason == :busy, do: 3_000, else: s.retry))}

      {:exit, reason, s} ->
        {:stop, :normal, {:error, reason}, s}
    end
  end

  def handle_call(:delete, _from, s) do
    s = kill_upload(s)
    caches = Cache.instances(s.c.name)

    try do
      with :ok <- cache_calls(caches, :begin_delete, s.id),
           :ok <- Storage.call(s.c.storage, :delete, [Config.key(s.c, s.id)]) do
        # After remote deletion, even a partial cleanup failure must never take
        # the normal termination path that saves the database again.
        s = %{close(s) | closed: true, pending: nil}

        local = cache_calls(caches, :purge, s.id)
        remote = purge_conflicts(s.c, s.id, nil)
        result = if local == :ok, do: remote, else: local
        reason = if result == :ok, do: :not_found, else: elem(result, 1)
        Enum.each(s.waiters, fn {from, _} -> GenServer.reply(from, {:error, reason}) end)
        {:stop, :normal, result, s}
      else
        {:error, reason} ->
          Enum.each(s.waiters, fn {from, _} -> GenServer.reply(from, {:error, reason}) end)

          if s.mode == :delete,
            do: {:stop, :normal, {:error, reason}, s},
            else: {:reply, {:error, reason}, timers(s)}
      end
    after
      cache_calls(caches, :end_delete, s.id)
    end
  end

  defp cache_calls(caches, operation, id) do
    # Visit every cache even if an earlier one fails, including when releasing
    # exclusions after a lost response or a node disconnect.
    Enum.reduce(caches, :ok, fn pid, result ->
      current = Cache.deletion_call(pid, operation, id)
      if result == :ok, do: current, else: result
    end)
  end

  defp purge_conflicts(c, id, cursor) do
    prefix = c.prefix <> "conflicts/" <> id <> "/"

    with {:ok, keys, next} <- Storage.call(c.storage, :list, [prefix, cursor, 1000]),
         :ok <-
           Enum.reduce_while(keys, :ok, fn key, :ok ->
             case Storage.call(c.storage, :delete, [prefix <> key]) do
               :ok -> {:cont, :ok}
               error -> {:halt, error}
             end
           end) do
      if next, do: purge_conflicts(c, id, next), else: :ok
    end
  end

  def handle_info(:save, s) do
    s = %{s | save_timer: nil}

    case start_save(s) do
      {:ok, s} ->
        {:noreply, save_timer(s, s.c.save_interval)}

      {:error, reason, s} ->
        unsaved(s, reason)
        {:noreply, save_timer(s, if(reason == :busy, do: 3_000, else: s.retry))}
    end
  end

  def handle_info({ref, result}, %{task: %Task{ref: ref}} = s) do
    Process.demonitor(ref, [:flush])
    s = %{s | task: nil}

    case settle(s, result) do
      {:ok, s} ->
        # Replay snapshots carry no persisted generation; immediately save latest.
        {:noreply, save_timer(s, if(s.gen > s.saved_gen, do: 0, else: s.c.save_interval))}

      {:error, reason, s} ->
        unsaved(s, reason)
        {:noreply, save_timer(s, s.retry)}

      {:exit, _reason, s} ->
        {:stop, :normal, s}
    end
  end

  def handle_info(:idle, %{mode: :delete} = s), do: {:stop, :normal, %{s | closed: true}}

  def handle_info(:idle, s) do
    case flush(s) do
      {:ok, s} ->
        s = close(s)
        Files.put_meta(s.c, s.id, s.base, nil, :clean)
        Cache.checkin(s.cache, s.id)
        {:stop, :normal, %{s | closed: true}}

      {:error, reason, s} ->
        unsaved(s, reason)
        {:noreply, s |> idle_timer() |> save_timer(if(reason == :busy, do: 3_000, else: s.retry))}

      {:exit, _reason, s} ->
        {:stop, :normal, s}
    end
  end

  def handle_info({:name_conflict, _name, kept}, s) do
    cond do
      is_nil(s.db) and kept == self() ->
        {:noreply, s}

      is_nil(s.db) ->
        {:stop, :normal, %{s | closed: true}}

      kept == self() ->
        {:noreply, save_timer(%{s | gen: s.gen + 1}, 0)}

      true ->
        {:exit, _, s} = conflict(kill_upload(s))
        {:stop, :normal, s}
    end
  end

  def handle_info({:EXIT, _pid, :normal}, s), do: {:noreply, s}
  def handle_info({:DOWN, _, :process, _, :normal}, s), do: {:noreply, s}
  def handle_info({:EXIT, pid, reason}, s), do: {:stop, {:linked_process_failed, pid, reason}, s}

  def terminate(reason, s) do
    if not s.closed and not is_nil(s.db) and reason in [:shutdown, :normal] and is_pid(s.cache) and
         Process.alive?(s.cache) do
      s = shutdown_flush(s)

      if not s.closed do
        s = close(s)
        Files.put_meta(s.c, s.id, s.base, nil, :clean)
        Cache.checkin(s.cache, s.id)
      end
    end

    if s.task, do: Task.shutdown(s.task, :brutal_kill)
    if s.watchdog && Process.alive?(s.watchdog), do: GenServer.stop(s.watchdog, :normal)
    :ok
  end

  defp activate(s) do
    meta = Files.meta(s.c, s.id)
    dirty = Files.dirty?(s.c, s.id)

    if dirty and meta do
      case Storage.call(s.c.storage, :head, [Config.key(s.c, s.id)]) do
        {:ok, etag} when etag == meta.base or etag == meta.pending ->
          adopt(s, meta, etag)

        {:ok, _} ->
          {:ok, db} = SQL.open(Files.path(s.c, s.id), s.c)

          {:exit, _, s} =
            conflict(%{
              s
              | db: db,
                base: meta.base,
                pending: pending(s, meta.pending, meta.base, nil)
            })

          download(%{s | closed: false, db: nil, pending: nil}, nil)

        {:error, :not_found} ->
          if meta.pending, do: Files.remove(Files.snapshot(s.c, s.id, meta.pending))
          Files.remove_db(s.c, s.id)
          {:error, :not_found, s}

        {:error, reason} ->
          {:error, reason, s}
      end
    else
      if meta && meta.pending, do: Files.remove(Files.snapshot(s.c, s.id, meta.pending))
      etag = if meta && File.exists?(Files.path(s.c, s.id)), do: meta.base, else: nil
      download(s, etag)
    end
  end

  defp adopt(s, meta, etag) do
    path = if meta.pending, do: Files.snapshot(s.c, s.id, meta.pending)

    cond do
      meta.pending && etag == meta.base && not File.exists?(path) ->
        {:ok, db} = SQL.open(Files.path(s.c, s.id), s.c)
        {:exit, _, s} = conflict(%{s | db: db, base: meta.base})
        download(%{s | closed: false, db: nil}, nil)

      true ->
        p =
          if meta.pending && etag == meta.base,
            do: pending(s, meta.pending, meta.base, nil),
            else: nil

        if meta.pending && is_nil(p), do: Files.remove(path)

        with {:ok, db} <- SQL.open(Files.path(s.c, s.id), s.c) do
          Files.put_meta(s.c, s.id, etag, if(p, do: p.md5), :dirty)
          Config.event(:adoption, %{}, %{service: s.c.name, id: s.id})
          s = %{s | db: db, base: etag, pending: p, gen: 1, saved_gen: 0}
          send(self(), :save)
          {:ok, s}
        else
          {:error, reason} -> {:error, reason, s}
        end
    end
  end

  defp download(s, etag) do
    path = Files.temp(s.c, s.id)
    started = System.monotonic_time(:millisecond)
    result = Storage.call(s.c.storage, :get, [Config.key(s.c, s.id), path, etag])

    outcome =
      case result do
        :not_modified ->
          {:ok, etag, :hit}

        {:ok, new_etag} ->
          Files.copy(path, Files.path(s.c, s.id))
          {:ok, new_etag, if(etag, do: :stale, else: :miss)}

        {:error, :not_found} ->
          Files.remove_db(s.c, s.id)
          {:error, :not_found}

        error ->
          error
      end

    Files.remove(path)

    case outcome do
      {:ok, base, hit} ->
        case SQL.open(Files.path(s.c, s.id), s.c) do
          {:ok, db} ->
            Files.put_meta(s.c, s.id, base, nil, :dirty)

            Config.event(
              :activation,
              %{
                duration_ms: System.monotonic_time(:millisecond) - started,
                bytes: Files.size(Files.path(s.c, s.id))
              },
              %{service: s.c.name, id: s.id, cache: hit}
            )

            {:ok, %{s | db: db, base: base}}

          {:error, reason} ->
            {:error, reason, s}
        end

      {:error, reason} ->
        {:error, reason, s}
    end
  end

  defp pending(s, md5, base, gen) do
    if md5, do: %{md5: md5, base: base, path: Files.snapshot(s.c, s.id, md5), gen: gen}, else: nil
  end

  defp start_save(%{task: task} = s) when not is_nil(task), do: {:ok, s}
  defp start_save(%{pending: p} = s) when not is_nil(p), do: {:ok, upload(s)}
  defp start_save(%{gen: gen, saved_gen: saved} = s) when gen <= saved, do: {:ok, s}

  defp start_save(s) do
    case SQL.checkpoint(s.db) do
      :ok ->
        tmp = Path.join([s.c.cache_dir, "snapshots", s.id <> "." <> Files.unique()])
        Files.copy(Files.path(s.c, s.id), tmp)
        md5 = Files.hash(tmp)
        path = Files.snapshot(s.c, s.id, md5)
        File.rename!(tmp, path)
        Files.sync_dir(Path.dirname(path))
        Files.put_meta(s.c, s.id, s.base, md5, :dirty)
        {:ok, upload(%{s | pending: pending(s, md5, s.base, s.gen)})}

      {:error, reason} ->
        {:error, reason, s}
    end
  end

  defp upload(s) do
    c = s.c
    id = s.id
    p = s.pending

    task =
      Task.async(fn ->
        started = System.monotonic_time(:millisecond)

        result =
          Storage.call(c.storage, :put, [Config.key(c, id), p.path, {:etag, p.base}, p.md5])

        Config.event(
          :save,
          %{
            duration_ms: System.monotonic_time(:millisecond) - started,
            bytes: Files.size(p.path)
          },
          %{service: c.name, id: id, outcome: elem(result, 0)}
        )

        result
      end)

    %{s | task: task}
  end

  defp settle(s, {:ok, etag}) when etag == s.pending.md5, do: saved(s, etag)

  defp settle(s, {:ok, _}),
    do: {:error, :etag_mismatch, %{s | retry: min(s.retry * 2, s.c.retry_max)}}

  defp settle(s, {:error, reason}) when reason in [:precondition_failed, :not_found] do
    case Storage.call(s.c.storage, :head, [Config.key(s.c, s.id)]) do
      {:ok, etag} when etag == s.pending.md5 ->
        saved(s, etag)

      {:error, :not_found} ->
        s = close(s)
        drop_pending(s)
        Files.remove_db(s.c, s.id)
        {:exit, :not_found, %{s | closed: true, pending: nil}}

      {:ok, _} ->
        conflict(s)

      {:error, error} ->
        {:error, error, %{s | retry: min(s.retry * 2, s.c.retry_max)}}
    end
  end

  defp settle(s, {:error, reason}),
    do: {:error, reason, %{s | retry: min(s.retry * 2, s.c.retry_max)}}

  defp saved(s, etag) do
    Files.put_meta(s.c, s.id, etag, nil, :dirty)
    Files.remove(s.pending.path)
    Files.sync_dir(Path.dirname(s.pending.path))
    gen = if is_nil(s.pending.gen), do: s.saved_gen, else: s.pending.gen
    {:ok, %{s | base: etag, saved_gen: gen, pending: nil, task: nil, retry: 1_000}}
  end

  defp flush(s) do
    case start_save(s) do
      {:ok, %{task: nil} = s} ->
        {:ok, s}

      {:ok, s} ->
        result = Task.await(s.task, :infinity)

        case settle(%{s | task: nil}, result) do
          {:ok, s} -> flush(s)
          other -> other
        end

      other ->
        other
    end
  end

  defp shutdown_flush(s) do
    case flush(s) do
      {:ok, s} ->
        s

      {:exit, _, s} ->
        s

      {:error, reason, s} ->
        unsaved(s, reason)
        Process.sleep(if(reason == :busy, do: 3_000, else: s.retry))
        if Process.alive?(s.cache), do: shutdown_flush(s), else: exit(:kill)
    end
  end

  defp conflict(s) do
    s = kill_upload(s)
    checkpoint_wait(s.db)
    s = close(s)
    node_name = node() |> Atom.to_string() |> Base.url_encode64(padding: false)
    stamp = DateTime.utc_now() |> DateTime.to_iso8601() |> String.replace(":", "")
    filename = s.id <> "--" <> stamp <> "-" <> node_name <> "-" <> Files.unique()
    destination = Path.join([s.c.cache_dir, "conflicts", filename])
    File.rename!(Files.path(s.c, s.id), destination)
    Files.sync_dir(Path.dirname(destination))
    Files.sync_dir(Path.dirname(Files.path(s.c, s.id)))
    Files.remove(Files.meta_path(s.c, s.id))
    Files.sync_dir(Path.dirname(Files.meta_path(s.c, s.id)))
    drop_pending(s)
    Config.event(:conflict, %{}, %{service: s.c.name, id: s.id, file: destination})
    send(s.cache, :sweep)
    {:exit, :conflict, %{s | closed: true, pending: nil}}
  end

  defp checkpoint_wait(nil), do: :ok

  defp checkpoint_wait(db) do
    case SQL.checkpoint(db) do
      :ok ->
        :ok

      {:error, :busy} ->
        Process.sleep(3_000)
        checkpoint_wait(db)

      {:error, reason} ->
        exit({:checkpoint_failed, reason})
    end
  end

  defp drop_pending(%{pending: nil} = s) do
    case Files.meta(s.c, s.id) do
      %{pending: md5} when not is_nil(md5) -> Files.remove(Files.snapshot(s.c, s.id, md5))
      _ -> :ok
    end
  end

  defp drop_pending(s), do: Files.remove(s.pending.path)
  defp kill_upload(%{task: nil} = s), do: s

  defp kill_upload(s) do
    Task.shutdown(s.task, :brutal_kill)
    %{s | task: nil}
  end

  defp close(%{db: nil} = s), do: s

  defp close(s) do
    :ok = DB.close(s.db)
    %{s | db: nil}
  end

  defp unsaved(s, reason),
    do: Config.event(:unsaved, %{}, %{service: s.c.name, id: s.id, reason: reason})

  defp timers(s), do: s |> idle_timer() |> save_timer(s.c.save_interval)

  defp idle_timer(s) do
    if s.idle, do: Process.cancel_timer(s.idle)
    %{s | idle: Process.send_after(self(), :idle, s.c.idle_timeout)}
  end

  defp save_timer(s, delay) do
    if s.save_timer, do: Process.cancel_timer(s.save_timer)
    %{s | save_timer: Process.send_after(self(), :save, delay)}
  end
end
