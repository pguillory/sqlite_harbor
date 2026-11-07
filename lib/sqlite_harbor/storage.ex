defmodule SqliteHarbor.Storage do
  @moduledoc """
  Single-part object storage. ETags are lowercase MD5 hex without quotes.
  Conditions must be atomic: `:absent`, `{:etag, etag}` or `:none`.
  Implementations return `{:error, :not_found | :precondition_failed | term}`.
  GET uses a disposable destination supplied by the caller; 304 returns `:not_modified`.
  LIST returns keys relative to the requested prefix and an opaque continuation cursor.
  """
  @callback get(term, String.t(), Path.t(), String.t() | nil) ::
              {:ok, String.t()} | :not_modified | {:error, term}
  @callback head(term, String.t()) :: {:ok, String.t()} | {:error, term}
  @callback put(term, String.t(), Path.t(), :none | :absent | {:etag, String.t()}, String.t()) ::
              {:ok, String.t()} | {:error, term}
  @callback delete(term, String.t()) :: :ok | {:error, term}
  @callback list(term, String.t(), term, pos_integer) ::
              {:ok, [String.t()], term} | {:error, term}

  def call({module, opts}, fun, args), do: apply(module, fun, [opts | args])
end
