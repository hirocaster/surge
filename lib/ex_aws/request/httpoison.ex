defmodule ExAws.Request.HTTPoison do
  @behaviour ExAws.Request.HttpClient
  def request(method, url, body, headers, opts) do
    HTTPoison.request(method, url, body, headers, opts)
  end
end
