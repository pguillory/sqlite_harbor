defmodule SqliteHarbor.Supervisor do
  @moduledoc false
  use Supervisor
  alias SqliteHarbor.{Cache, Config, Drain}

  def start_link(c),
    do: Supervisor.start_link(__MODULE__, c, name: Config.local(__MODULE__, c.name))

  def init(c) do
    for key <- [:connect_all, :prevent_overlapping_partitions] do
      unless Application.get_env(:kernel, key, true) == true,
        do: raise(ArgumentError, "kernel #{key} must be true for global ownership")
    end

    children = [
      {Cache, c},
      %{
        id: SqliteHarbor.Owners,
        start:
          {DynamicSupervisor, :start_link,
           [[strategy: :one_for_one, name: Config.local(SqliteHarbor.Owners, c.name)]]},
        shutdown: :infinity
      },
      {Drain, c}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
