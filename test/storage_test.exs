defmodule SqliteHarbor.StorageTest do
  use ExUnit.Case, async: true
  alias SqliteHarbor.{Files, Storage.Local}

  test "Local conditions, ETags, conditional downloads and cursors agree" do
    root = Path.join(System.tmp_dir!(), "harbor-local-#{Files.unique()}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    source = Path.join(root, "source")
    destination = Path.join(root, "destination")
    opts = [root: Path.join(root, "objects")]
    File.write!(source, "one")
    md5 = Files.hash(source)
    assert {:ok, ^md5} = Local.put(opts, "db/a", source, :absent, md5)
    assert {:error, :precondition_failed} = Local.put(opts, "db/a", source, :absent, md5)
    assert {:error, :precondition_failed} = Local.put(opts, "db/a", source, {:etag, "wrong"}, md5)
    assert :not_modified = Local.get(opts, "db/a", destination, md5)
    assert {:ok, ^md5} = Local.get(opts, "db/a", destination, nil)
    assert File.read!(destination) == "one"
    assert {:ok, ^md5} = Local.put(opts, "db/b", source, :absent, md5)
    assert {:ok, ["a"], cursor} = Local.list(opts, "db/", nil, 1)
    assert {:ok, ["b"], nil} = Local.list(opts, "db/", cursor, 1)
    File.write!(source, "two")
    new = Files.hash(source)
    assert {:ok, ^new} = Local.put(opts, "db/a", source, {:etag, md5}, new)
    assert {:ok, ^new} = Local.head(opts, "db/a")
    assert :ok = Local.delete(opts, "db/a")
    assert :ok = Local.delete(opts, "db/a")
    assert {:error, :not_found} = Local.head(opts, "db/a")
    assert {:error, :precondition_failed} = Local.put(opts, "db/a", source, {:etag, new}, new)
  end
end
