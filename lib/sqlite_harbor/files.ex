defmodule SqliteHarbor.Files do
  @moduledoc false
  def path(c, id), do: Path.join([c.cache_dir, "db", id <> ".sqlite"])
  def meta_path(c, id), do: Path.join([c.cache_dir, "db", id <> ".meta"])
  def snapshot(c, id, md5), do: Path.join([c.cache_dir, "snapshots", id <> "." <> md5])
  def unique, do: Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
  def temp(c), do: Path.join([c.cache_dir, "tmp", unique()])

  def init(c) do
    for dir <- ["db", "snapshots", "tmp", "conflicts"],
        do: File.mkdir_p!(Path.join(c.cache_dir, dir))
  end

  def clean_scratch_at_boot(c) do
    key = {__MODULE__, :scratch_cleaned, c.cache_dir}
    # Cache restarts must preserve files still owned by API callers. This marker
    # lasts only for this VM; an actual node restart cleans abandoned scratch.
    unless :persistent_term.get(key, false) do
      for p <- Path.wildcard(Path.join([c.cache_dir, "tmp", "*"])), do: remove(p)
      :persistent_term.put(key, true)
    end
  end

  def sync_dir(path) do
    {:ok, fd} = :file.open(String.to_charlist(path), [:read, :raw, :directory])

    try do
      :ok = :file.sync(fd)
    after
      :file.close(fd)
    end
  end

  def sync(path) do
    {:ok, fd} = :file.open(String.to_charlist(path), [:read, :raw])

    try do
      :ok = :file.sync(fd)
    after
      :file.close(fd)
    end
  end

  def atomic(path, bytes) do
    tmp = path <> "." <> unique() <> ".tmp"
    File.write!(tmp, bytes, [:binary, :sync])
    File.rename!(tmp, path)
    sync_dir(Path.dirname(path))
  end

  def meta(c, id) do
    case File.read(meta_path(c, id)) do
      {:ok, bytes} -> :erlang.binary_to_term(bytes, [:safe])
      {:error, :enoent} -> nil
    end
  end

  def put_meta(c, id, base, pending, status),
    do:
      atomic(
        meta_path(c, id),
        :erlang.term_to_binary(%{base: base, pending: pending, status: status})
      )

  def hash(path, algorithm \\ :md5) do
    path
    |> File.stream!([], 64 * 1024)
    |> Enum.reduce(:crypto.hash_init(algorithm), &:crypto.hash_update(&2, &1))
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
  end

  def copy(from, to) do
    File.cp!(from, to)
    sync(to)
    sync_dir(Path.dirname(to))
  end

  def size(path) do
    case File.stat(path) do
      {:ok, stat} -> stat.size
      {:error, :enoent} -> 0
    end
  end

  def remove(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> raise File.Error, reason: reason, action: "remove", path: path
    end
  end

  def remove_db(c, id) do
    for path <- [path(c, id), path(c, id) <> "-wal", path(c, id) <> "-shm", meta_path(c, id)],
        do: remove(path)

    sync_dir(Path.join(c.cache_dir, "db"))
  end

  def scratch_remove(path), do: Enum.each([path, path <> "-wal", path <> "-shm"], &remove/1)

  def dirty?(c, id) do
    case meta(c, id) do
      %{status: :dirty} -> File.exists?(path(c, id))
      %{status: :clean} -> size(path(c, id) <> "-wal") > 0
      nil -> false
    end
  end
end
