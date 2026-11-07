defmodule SqliteHarbor.Storage.Local do
  @moduledoc """
  Directory-backed storage for a single BEAM node. Conditions are serialized per
  object on that node. Do not share this backend between production nodes; use S3.
  Options: `root: "/absolute/path"`. ETag is the content MD5.
  """
  @behaviour SqliteHarbor.Storage
  alias SqliteHarbor.Files

  def get(opts, key, destination, etag) do
    locked(opts, key, fn path ->
      case digest(path) do
        {:ok, ^etag} when not is_nil(etag) ->
          :not_modified

        {:ok, current} ->
          File.cp!(path, destination)
          {:ok, current}

        error ->
          error
      end
    end)
  end

  def head(opts, key), do: locked(opts, key, &digest/1)

  def put(opts, key, source, condition, md5) do
    locked(opts, key, fn path ->
      current = digest(path)

      allowed =
        case {condition, current} do
          {:none, _} -> true
          {:absent, {:error, :not_found}} -> true
          {{:etag, expected}, {:ok, actual}} -> expected == actual
          _ -> false
        end

      cond do
        not allowed ->
          {:error, :precondition_failed}

        Files.hash(source) != md5 ->
          {:error, :bad_digest}

        true ->
          File.mkdir_p!(Path.dirname(path))
          tmp = path <> "." <> Files.unique() <> ".tmp"
          Files.copy(source, tmp)
          File.rename!(tmp, path)
          Files.sync_dir(Path.dirname(path))
          {:ok, md5}
      end
    end)
  end

  def delete(opts, key),
    do:
      locked(opts, key, fn path ->
        Files.remove(path)
        :ok
      end)

  def list(opts, prefix, cursor, limit) do
    root = root(opts)

    keys =
      Path.wildcard(Path.join(root, "**/*"), match_dot: true)
      |> Enum.filter(&File.regular?/1)
      |> Enum.map(&Path.relative_to(&1, root))
      |> Enum.filter(&(String.starts_with?(&1, prefix) and not String.ends_with?(&1, ".tmp")))
      |> Enum.sort()
      |> Enum.filter(&(is_nil(cursor) or &1 > cursor))

    page = Enum.take(keys, limit)
    next = if length(keys) > limit, do: List.last(page), else: nil
    {:ok, Enum.map(page, &String.replace_prefix(&1, prefix, "")), next}
  end

  defp root(opts), do: opts |> Keyword.fetch!(:root) |> Path.expand()

  defp locked(opts, key, fun) do
    unless String.match?(key, ~r/\A[A-Za-z0-9_\.~\/-]+\z/) and ".." not in String.split(key, "/"),
      do: raise(ArgumentError, "unsafe object key")

    path = Path.join(root(opts), key)
    :global.trans({{__MODULE__, path}, self()}, fn -> fun.(path) end, [node()])
  end

  defp digest(path) do
    if File.regular?(path), do: {:ok, Files.hash(path)}, else: {:error, :not_found}
  end
end
