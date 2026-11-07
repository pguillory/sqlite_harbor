defmodule SqliteHarbor.Drain do
  @moduledoc false
  use GenServer
  alias SqliteHarbor.Cache
  def start_link(c), do: GenServer.start_link(__MODULE__, c)

  def init(c) do
    Process.flag(:trap_exit, true)
    cache = :global.whereis_name({Cache, node(), c.name})
    # Owners' supervisor is now available, so boot adoption cannot race its start.
    send(cache, :sweep)
    {:ok, cache}
  end

  def terminate(_, cache) do
    if is_pid(cache) and Process.alive?(cache), do: Cache.drain(cache)
    :ok
  end
end
