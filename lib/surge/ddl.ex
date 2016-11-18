defmodule Surge.DDL do

  require Surge.Exceptions

  def create_table(model) do
    table_name      = model.__canonical_name__
    keys            = model.__keys__
    keys_schema     = pk_schema(keys)
    key_definitions = pk_spec(keys)
    [read, write]   = model.__throughput__

    table_name
    |> ExAws.Dynamo.create_table(keys_schema, key_definitions, read, write)
    |> ExAws.request
  end

  def describe_table(model) do
    case model.__canonical_name__
    |> ExAws.Dynamo.describe_table
    |> ExAws.request do
      {:ok, map} when is_map(map)->
        map["Table"]
      {:error, message} ->
        Surge.Exceptions.dynamic_raise message
    end
  end

  def delete_table(model) do
    model.__canonical_name__
    |> ExAws.Dynamo.delete_table
    |> ExAws.request
  end

  defp pk_schema([hash: {hname, _htype}, range: {rname, _rtype}]), do: [{hname, :hash}, {rname, :range}]
  defp pk_schema([hash: {hname, _htype}]), do: [{hname, :hash}]

  defp pk_spec([hash: {hname, htype}, range: {rname, rtype}]) do
    [{hname, htype}, {rname, rtype}]
  end
  defp pk_spec([hash: {hname, htype}]) do
    [{hname, htype}]
  end
end
