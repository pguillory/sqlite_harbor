defmodule SqliteHarbor.Cache do
  @moduledoc false
  use GenServer
  alias SqliteHarbor.{Config, Files, Owner, Storage}

  def start_link(c),
    do: GenServer.start_link(__MODULE__, c, name: Config.local(__MODULE__, c.name))

  def config(service), do: GenServer.call(Config.local(__MODULE__, service), :config)
  def checkout(pid, id), do: GenServer.call(pid, {:checkout, id}, :infinity)
  def checkin(pid, id), do: GenServer.call(pid, {:checkin, id}, :infinity)
  def drain(pid), do: GenServer.call(pid, :drain, :infinity)

  def instances(service) do
    for {__MODULE__, _node, ^service} = name <- :global.registered_names(),
        pid = :global.whereis_name(name),
        is_pid(pid),
        do: pid
  end

  def deletion_call(pid, operation, id) do
    GenServer.call(pid, {operation, id}, :infinity)
  catch
    :exit, reason -> {:error, {:cache_unavailable, reason}}
  end

  def init(c) do
    Process.flag(:trap_exit, true)
    Files.init(c)
    SqliteHarbor.SQL.heap_limit(c.hard_heap_limit)
    boot_cleanup(c)

    {:ok,
     %{
       c: c,
       held: %{},
       refs: %{},
       draining: false,
       recovering: MapSet.new(),
       failed: MapSet.new(),
       deleting: %{},
       sweep_timer: nil,
       conflict_task: nil,
       recovery_tasks: %{},
       tasks: %{}
     }}
  end

  def handle_call(:config, _from, s), do: {:reply, s.c, s}
  def handle_call(:drain, _from, s), do: {:reply, :ok, %{s | draining: true}}

  def handle_call({:begin_delete, id}, {pid, _}, s) do
    holder = s.held[id]

    cond do
      :global.whereis_name(Owner.name(s.c.name, id)) != pid ->
        {:reply, {:error, :not_owner}, s}

      holder && holder != pid && Process.alive?(holder) ->
        {:reply, {:error, :in_use}, s}

      true ->
        s = stop_conflict_task(s)
        s = end_delete(s, id)
        ref = Process.monitor(pid)
        {:reply, :ok, %{s | deleting: Map.put(s.deleting, id, {pid, ref})}}
    end
  end

  def handle_call({:purge, id}, {pid, _}, s) do
    case s.deleting[id] do
      {^pid, _ref} ->
        result =
          try do
            Files.purge(s.c, id)
          rescue
            e in File.Error -> {:error, {:local_cleanup, e.reason}}
          end

        {:reply, result, s}

      _ ->
        {:reply, {:error, :not_owner}, s}
    end
  end

  def handle_call({:end_delete, id}, {pid, _}, s) do
    s = if match?({^pid, _}, s.deleting[id]), do: end_delete(s, id), else: s
    {:reply, :ok, s}
  end

  def handle_call({:checkout, id}, {pid, _}, s) do
    holder = s.held[id]

    cond do
      s.draining ->
        {:reply, {:error, :draining}, s}

      Map.has_key?(s.deleting, id) ->
        {:reply, {:error, :deleting}, s}

      :global.whereis_name(Owner.name(s.c.name, id)) != pid ->
        {:reply, {:error, :not_owner}, s}

      holder && Process.alive?(holder) ->
        {:reply, {:error, :in_use}, s}

      true ->
        s = if holder, do: release(s, id), else: s
        ref = Process.monitor(pid)

        {:reply, :ok,
         %{
           s
           | held: Map.put(s.held, id, pid),
             refs: Map.put(s.refs, ref, id),
             failed: MapSet.delete(s.failed, id)
         }}
    end
  end

  def handle_call({:checkin, id}, {pid, _}, s) do
    s = if s.held[id] == pid, do: release(s, id), else: s
    {:reply, :ok, %{s | recovering: MapSet.delete(s.recovering, id)}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, s) do
    s =
      Enum.reduce(s.deleting, s, fn
        {id, {_pid, ^ref}}, s -> end_delete(s, id)
        _, s -> s
      end)

    case Map.fetch(s.refs, ref) do
      {:ok, id} ->
        repeated = MapSet.member?(s.recovering, id)
        s = release(s, id)

        cond do
          repeated and reason != :normal and reason != :shutdown ->
            Config.event(:adoption_failed, %{}, %{service: s.c.name, id: id, reason: reason})

            {:noreply,
             %{s | failed: MapSet.put(s.failed, id), recovering: MapSet.delete(s.recovering, id)}}

          reason in [:normal, :shutdown] ->
            {:noreply, %{s | recovering: MapSet.delete(s.recovering, id)}}

          true ->
            {:noreply, maybe_recover(s, id)}
        end

      :error ->
        recovering =
          case s.recovery_tasks[ref] do
            nil -> s.recovering
            id -> MapSet.delete(s.recovering, id)
          end

        {:noreply,
         %{
           s
           | tasks: Map.delete(s.tasks, ref),
             recovering: recovering,
             recovery_tasks: Map.delete(s.recovery_tasks, ref),
             conflict_task: if(s.conflict_task == ref, do: nil, else: s.conflict_task)
         }}
    end
  end

  def handle_info(:sweep, s) do
    if s.sweep_timer, do: Process.cancel_timer(s.sweep_timer)
    s = %{s | sweep_timer: Process.send_after(self(), :sweep, s.c.recheck_interval)}

    s =
      unless s.draining do
        s = Enum.reduce(ids(s.c), s, &maybe_recover(&2, &1))
        start_sweep_task(s)
      else
        s
      end

    {:noreply, s}
  end

  def handle_info({ref, {:recovery, id, result}}, s) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    if result == :elsewhere,
      do: Config.event(:adoption_stranded, %{}, %{service: s.c.name, id: id})

    # A running recovery owner remains marked until check-in/crash.
    s = %{s | tasks: Map.delete(s.tasks, ref), recovery_tasks: Map.delete(s.recovery_tasks, ref)}

    s =
      if result in [:elsewhere, :not_started],
        do: %{s | recovering: MapSet.delete(s.recovering, id)},
        else: s

    {:noreply, s}
  end

  def handle_info({ref, :swept}, s) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    {:noreply,
     %{
       s
       | tasks: Map.delete(s.tasks, ref),
         conflict_task: if(s.conflict_task == ref, do: nil, else: s.conflict_task)
     }}
  end

  def handle_info({:EXIT, _pid, :normal}, s), do: {:noreply, s}

  def handle_info({:EXIT, pid, reason}, s) do
    # Linked sweep/upload failures are operational errors, never detached work.
    Config.event(:cache_task_failed, %{}, %{service: s.c.name, pid: pid, reason: reason})
    {:noreply, s}
  end

  def terminate(_reason, s) do
    for {_ref, pid} <- s.tasks, Process.alive?(pid), do: Process.exit(pid, :kill)
    :ok
  end

  defp release(s, id) do
    refs = for {ref, ^id} <- s.refs, do: ref
    Enum.each(refs, &Process.demonitor(&1, [:flush]))
    %{s | held: Map.delete(s.held, id), refs: Map.drop(s.refs, refs)}
  end

  defp end_delete(s, id) do
    if entry = s.deleting[id], do: Process.demonitor(elem(entry, 1), [:flush])
    %{s | deleting: Map.delete(s.deleting, id)}
  end

  defp stop_conflict_task(%{conflict_task: nil} = s), do: s

  defp stop_conflict_task(s) do
    ref = s.conflict_task

    if pid = s.tasks[ref] do
      Process.unlink(pid)
      Process.exit(pid, :kill)

      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> :ok
      end
    end

    Process.demonitor(ref, [:flush])
    %{s | conflict_task: nil, tasks: Map.delete(s.tasks, ref)}
  end

  defp maybe_recover(s, id) do
    cond do
      s.draining or Map.has_key?(s.deleting, id) or Map.has_key?(s.held, id) or
        MapSet.member?(s.recovering, id) or
          MapSet.member?(s.failed, id) ->
        s

      not Files.dirty?(s.c, id) ->
        s

      true ->
        c = s.c

        task =
          Task.async(fn ->
            result =
              case Owner.start(c, id, :normal) do
                {:ok, pid} ->
                  # Ensure boot activations consume initialization errors too;
                  # otherwise an error-only owner would hold the name indefinitely.
                  try do
                    case GenServer.call(pid, :save, :infinity) do
                      :ok -> :started
                      {:error, _} -> :not_started
                    end
                  catch
                    :exit, _ -> :not_started
                  end

                {:error, {:already_started, pid}} when node(pid) != node() ->
                  :elsewhere

                {:error, _} ->
                  :not_started
              end

            {:recovery, id, result}
          end)

        %{
          s
          | recovering: MapSet.put(s.recovering, id),
            recovery_tasks: Map.put(s.recovery_tasks, task.ref, id),
            tasks: Map.put(s.tasks, task.ref, task.pid)
        }
    end
  end

  defp ids(c) do
    Path.wildcard(Path.join([c.cache_dir, "db", "*.meta"]))
    |> Enum.map(&Path.basename(&1, ".meta"))
    |> Enum.filter(&SqliteHarbor.valid_id?/1)
  end

  defp boot_cleanup(c) do
    Files.clean_scratch_at_boot(c)

    keep =
      Enum.reduce(ids(c), MapSet.new(), fn id, keep ->
        case Files.meta(c, id) do
          %{pending: pending} ->
            if File.exists?(Files.path(c, id)) do
              if pending, do: MapSet.put(keep, Files.snapshot(c, id, pending)), else: keep
            else
              Files.remove(Files.meta_path(c, id))
              keep
            end

          _ ->
            keep
        end
      end)

    for p <- Path.wildcard(Path.join([c.cache_dir, "snapshots", "*"])),
        not MapSet.member?(keep, p),
        do: Files.remove(p)
  end

  defp start_sweep_task(s) do
    # Reserve clean files in the ledger by doing the quick eviction synchronously;
    # conflict network I/O alone runs in a linked task.
    evict(s)

    if s.conflict_task && Map.has_key?(s.tasks, s.conflict_task) do
      s
    else
      c = s.c
      deleting = s.deleting

      task =
        Task.async(fn ->
          for path <- Path.wildcard(Path.join([c.cache_dir, "conflicts", "*"])),
              File.regular?(path),
              [id, suffix] <- [String.split(Path.basename(path), "--", parts: 2)],
              SqliteHarbor.valid_id?(id),
              not Map.has_key?(deleting, id),
              do: sweep_conflict(c, path, id, suffix)

          :swept
        end)

      %{s | tasks: Map.put(s.tasks, task.ref, task.pid), conflict_task: task.ref}
    end
  end

  defp sweep_conflict(c, path, id, suffix) do
    case Storage.call(c.storage, :head, [Config.key(c, id)]) do
      {:error, :not_found} ->
        Files.remove(path)
        Config.event(:conflict_discarded, %{}, %{service: c.name, id: id, file: path})

      {:ok, _} ->
        key = c.prefix <> "conflicts/" <> id <> "/" <> suffix
        md5 = Files.hash(path)

        case Storage.call(c.storage, :put, [key, path, :absent, md5]) do
          {:ok, _} ->
            Files.remove(path)

          {:error, :precondition_failed} ->
            if Storage.call(c.storage, :head, [key]) == {:ok, md5}, do: Files.remove(path)

          {:error, _} ->
            :ok
        end

      {:error, _} ->
        :ok
    end
  end

  defp evict(s) do
    candidates =
      ids(s.c)
      |> Enum.reject(&Map.has_key?(s.held, &1))
      |> Enum.filter(
        &(match?(%{status: :clean}, Files.meta(s.c, &1)) and not Files.dirty?(s.c, &1))
      )
      |> Enum.flat_map(fn id ->
        case File.stat(Files.path(s.c, id)) do
          {:ok, stat} ->
            [{id, stat}]

          {:error, :enoent} ->
            []

          {:error, reason} ->
            raise File.Error, reason: reason, action: "stat cache", path: Files.path(s.c, id)
        end
      end)
      |> Enum.sort_by(fn {_id, stat} -> stat.mtime end)

    total = Enum.reduce(candidates, 0, fn {_, stat}, n -> n + stat.size end)

    Enum.reduce(candidates, total, fn {id, stat}, bytes ->
      if bytes > s.c.cache_budget_bytes do
        Files.remove_db(s.c, id)
        Config.event(:eviction, %{bytes: stat.size}, %{service: s.c.name, id: id})
        bytes - stat.size
      else
        bytes
      end
    end)
  end
end
