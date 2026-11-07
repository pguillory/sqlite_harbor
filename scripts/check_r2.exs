# Usage: mix run scripts/check_r2.exs BUCKET /private/path/credentials.json
alias SqliteHarbor.{Files, Storage.S3}

[bucket, credential_path] = System.argv()

credentials =
  case Jason.decode(File.read!(credential_path)) do
    {:ok, value} when is_map(value) -> value
    _ -> raise "Credential file must contain a JSON object"
  end

credential = fn key ->
  case Map.get(credentials, key) do
    value when is_binary(value) and byte_size(value) > 0 -> value
    _ -> raise "Missing credential field #{key}"
  end
end

opts = [
  bucket: bucket,
  endpoint: credential.("endpoint"),
  region: Map.get(credentials, "region", "auto"),
  access_key_id: credential.("access_key_id"),
  secret_access_key: credential.("secret_access_key")
]

root = Path.join(System.tmp_dir!(), "sqlite-harbor-probe-" <> Files.unique())
File.mkdir_p!(root)
source = Path.join(root, "source")
download = Path.join(root, "download")
prefix = "_sqlite_harbor_probe/" <> Files.unique() <> "/"
key = prefix <> "object"

check = fn passed, label ->
  if passed, do: IO.puts("PASS " <> label), else: raise("FAIL " <> label)
end

try do
  File.write!(source, "sqlite-harbor-one")
  first = Files.hash(source)

  check.(
    S3.put(opts, key, source, :absent, first) == {:ok, first},
    "create-only PUT and MD5 ETag"
  )

  check.(S3.list(opts, prefix, nil, 10) == {:ok, ["object"], nil}, "prefix-scoped LIST")

  check.(
    S3.put(opts, key, source, :absent, first) == {:error, :precondition_failed},
    "reject duplicate create"
  )

  check.(S3.get(opts, key, download, first) == :not_modified, "conditional GET returns 304")
  File.write!(source, "sqlite-harbor-two")
  second = Files.hash(source)

  check.(
    S3.put(opts, key, source, {:etag, first}, second) == {:ok, second},
    "matching If-Match PUT"
  )

  check.(
    S3.put(opts, key, source, {:etag, first}, second) == {:error, :precondition_failed},
    "reject stale If-Match PUT"
  )

  check.(
    S3.get(opts, key, download, nil) == {:ok, second} and Files.hash(download) == second,
    "GET body integrity"
  )

  check.(S3.delete(opts, key) == :ok, "DELETE")
  missing = S3.put(opts, key, source, {:etag, second}, second)

  check.(
    missing in [{:error, :not_found}, {:error, :precondition_failed}],
    "If-Match on missing object refuses write"
  )

  check.(S3.head(opts, key) == {:error, :not_found}, "missing object stayed absent")
  check.(S3.delete(opts, key) == :ok, "idempotent DELETE")
  IO.puts("Conditional storage probe passed. Delete your empty temporary bucket.")
after
  # Known unique probe key only. No bucket or unrelated object deletion.
  case S3.delete(opts, key) do
    :ok ->
      :ok

    {:error, _} ->
      IO.puts(
        :stderr,
        "Probe object cleanup failed; remove the _sqlite_harbor_probe/ object manually."
      )
  end

  File.rm_rf!(root)
end
