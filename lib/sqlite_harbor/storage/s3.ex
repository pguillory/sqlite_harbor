defmodule SqliteHarbor.Storage.S3 do
  @moduledoc """
  S3-compatible single-part storage through ReqS3. Options: `endpoint`, `bucket`,
  `region` (R2: `"auto"`), `access_key_id`, `secret_access_key`.
  Credentials should be injected by the host; never put them in source control.
  Requests have no automatic retries. Connect timeout 5s, receive timeout 30s.
  """
  @behaviour SqliteHarbor.Storage
  alias SqliteHarbor.Files

  def get(opts, key, destination, etag) do
    headers = if etag, do: [{"if-none-match", quote_etag(etag)}], else: []

    case request(opts, :get, url(opts, key),
           headers: headers,
           decode_body: false,
           into: File.stream!(destination, [:write, :binary])
         ) do
      {:ok, %{status: 200} = r} -> etag_result(r)
      {:ok, %{status: 304}} -> :not_modified
      other -> failure(other)
    end
  end

  def head(opts, key) do
    case request(opts, :head, url(opts, key), decode_body: false) do
      {:ok, %{status: 200} = r} -> etag_result(r)
      other -> failure(other)
    end
  end

  def put(opts, key, source, condition, md5) do
    condition_headers =
      case condition do
        :none -> []
        :absent -> [{"if-none-match", "*"}]
        {:etag, e} -> [{"if-match", quote_etag(e)}]
      end

    headers =
      condition_headers ++
        [
          {"content-length", Integer.to_string(Files.size(source))},
          {"content-md5", md5 |> Base.decode16!(case: :mixed) |> Base.encode64()},
          {"content-type", "application/octet-stream"}
        ]

    case request(opts, :put, url(opts, key),
           headers: headers,
           body: File.stream!(source, [], 64 * 1024),
           decode_body: false
         ) do
      {:ok, %{status: status} = r} when status in 200..299 -> etag_result(r)
      other -> failure(other)
    end
  end

  def delete(opts, key) do
    case request(opts, :delete, url(opts, key), []) do
      {:ok, %{status: status}} when status in 200..299 or status == 404 -> :ok
      other -> failure(other)
    end
  end

  def list(opts, prefix, cursor, limit) do
    params =
      [{"list-type", "2"}, {"prefix", prefix}, {"max-keys", limit}] ++
        if(cursor, do: [{"continuation-token", cursor}], else: [])

    case request(opts, :get, "s3://#{required(opts, :bucket)}", params: params) do
      {:ok, %{status: 200, body: %{"ListBucketResult" => body}}} ->
        keys = Enum.map(body["Contents"] || [], &String.replace_prefix(&1["Key"], prefix, ""))

        {:ok, keys,
         if(body["IsTruncated"] == "true", do: body["NextContinuationToken"], else: nil)}

      other ->
        failure(other)
    end
  end

  defp url(opts, key) do
    encoded =
      key
      |> String.split("/")
      |> Enum.map_join("/", &URI.encode(&1, fn c -> URI.char_unreserved?(c) end))

    "s3://#{required(opts, :bucket)}/#{encoded}"
  end

  defp request(opts, method, url, extras) do
    req =
      Req.new(
        method: method,
        url: url,
        aws_sigv4: [
          service: :s3,
          access_key_id: required(opts, :access_key_id),
          secret_access_key: required(opts, :secret_access_key),
          region: Keyword.get(opts, :region, "auto")
        ],
        retry: false,
        redirect: false,
        compressed: false,
        receive_timeout: 30_000,
        connect_options: [timeout: 5_000]
      )
      |> ReqS3.attach(aws_endpoint_url_s3: required(opts, :endpoint))

    Req.request(req, extras)
  end

  defp quote_etag(value), do: ~s("#{value}")

  defp etag_result(response) do
    case Req.Response.get_header(response, "etag") do
      [value] ->
        etag = String.trim(value, "\"")

        if String.match?(etag, ~r/\A[0-9a-f]{32}\z/),
          do: {:ok, etag},
          else: {:error, :invalid_etag}

      _ ->
        {:error, :invalid_etag}
    end
  end

  defp required(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and byte_size(value) > 0 -> value
      _ -> raise ArgumentError, "missing or invalid S3 option #{key}"
    end
  end

  defp failure({:ok, %{status: 404}}), do: {:error, :not_found}

  defp failure({:ok, %{status: status}}) when status in [409, 412],
    do: {:error, :precondition_failed}

  defp failure({:ok, %{status: status}}), do: {:error, {:http, status}}
  defp failure({:error, exception}), do: {:error, {:transport, Exception.message(exception)}}
end
