defmodule SqliteHarbor.Test.Store do
  @moduledoc false
  @behaviour SqliteHarbor.Storage
  alias SqliteHarbor.Storage.Local

  def start_link(opts),
    do: Agent.start_link(fn -> %{root: Keyword.fetch!(opts, :root), faults: [], calls: []} end)

  def child_spec(opts), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}

  def fault(pid, operation, action, key \\ :any) do
    Agent.update(pid, fn s -> %{s | faults: s.faults ++ [{operation, key, action}]} end)
  end

  def calls(pid), do: Agent.get(pid, &Enum.reverse(&1.calls))
  def get(pid, key, destination, etag), do: perform(pid, :get, [key, destination, etag])
  def head(pid, key), do: perform(pid, :head, [key])
  def put(pid, key, source, condition, md5), do: perform(pid, :put, [key, source, condition, md5])
  def delete(pid, key), do: perform(pid, :delete, [key])
  def list(pid, prefix, cursor, limit), do: perform(pid, :list, [prefix, cursor, limit])

  # One authoritative node executes Local calls, including requests from peers.
  def perform(pid, op, args) when node(pid) != node(),
    do: :erpc.call(node(pid), __MODULE__, :perform, [pid, op, args], :infinity)

  def perform(pid, op, args) do
    key = hd(args)

    {root, action} =
      Agent.get_and_update(pid, fn s ->
        index =
          Enum.find_index(s.faults, fn {fop, fkey, _} ->
            fop == op and (fkey == :any or fkey == key)
          end)

        {action, faults} =
          if is_nil(index),
            do: {:pass, s.faults},
            else:
              (
                {{_, _, a}, rest} = List.pop_at(s.faults, index)
                {a, rest}
              )

        {{s.root, action}, %{s | faults: faults, calls: [{op, args} | s.calls]}}
      end)

    run = fn -> apply(Local, op, [[root: root] | args]) end

    case action do
      :pass ->
        run.()

      {:error, reason} ->
        {:error, reason}

      {:land_then_error, reason} ->
        result = run.()
        true = result == :ok or match?({:ok, _}, result)
        {:error, reason}

      {:block, notify, ref} ->
        send(notify, {:blocked, ref, self(), args})

        receive do
          {:release, ^ref} -> run.()
        end

      {:corrupt_get, bytes} ->
        {:ok, etag} = run.()
        File.write!(Enum.at(args, 1), bytes)
        {:ok, etag}
    end
  end
end
