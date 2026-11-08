defmodule SqliteHarbor.Deletion do
  @moduledoc false
  # The owner's side of the delete protocol across every connected Cache. The
  # Cache side (exclusions, local purge) lives in Cache, next to its state.
  # Call these only from the owner process itself: Caches check ownership
  # against, and monitor, the calling pid.
  alias SqliteHarbor.{Cache, Config, Storage}

  def exclude(caches, c, id) do
    with :ok <- cache_calls(caches, :begin_delete, id) do
      Storage.call(c.storage, :delete, [Config.key(c, id)])
    end
  end

  def purge(caches, c, id) do
    local = cache_calls(caches, :purge, id)
    remote = purge_conflicts(c, id, nil)
    if local == :ok, do: remote, else: local
  end

  def release(caches, id), do: cache_calls(caches, :end_delete, id)

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
end
