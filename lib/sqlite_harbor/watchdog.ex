defmodule SqliteHarbor.Watchdog do
  @moduledoc false
  use GenServer
  alias Exqlite.Sqlite3
  def start_link(owner, cache), do: GenServer.start_link(__MODULE__, {owner, cache})
  def arm(pid, conn, deadline, token), do: GenServer.call(pid, {:arm, conn, deadline, token})
  def disarm(pid, token), do: GenServer.call(pid, {:disarm, token})

  def init({owner, cache}) do
    Process.monitor(cache)
    {:ok, %{owner: owner, call: nil, timer: nil}}
  end

  def handle_call({:arm, conn, deadline, token}, _from, s) do
    timer =
      Process.send_after(
        self(),
        {:expire, token},
        max(0, deadline - System.monotonic_time(:millisecond))
      )

    {:reply, :ok, %{s | call: {conn, token}, timer: timer}}
  end

  def handle_call({:disarm, token}, _from, %{call: {_, token}} = s) do
    Process.cancel_timer(s.timer)
    {:reply, :ok, %{s | call: nil, timer: nil}}
  end

  def handle_info({:expire, token}, %{call: {conn, token}} = s) do
    Sqlite3.cancel(conn)
    {:noreply, %{s | timer: Process.send_after(self(), {:expire, token}, 50)}}
  end

  def handle_info({:expire, _}, s), do: {:noreply, s}

  def handle_info({:DOWN, _, :process, _, _}, s) do
    Process.exit(s.owner, :kill)
    {:stop, :normal, s}
  end
end
