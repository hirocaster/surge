defmodule Surge.DDL do

  require Surge.Exceptions

  def create_table(model) do
    table_name            = model.__table_name__
    keys                  = model.__keys__
    secondary_keys        = model.__secondary_keys__
    global_keys           = model.__global_keys__
    keys_schema           = pk_schema(keys)
    attribute_definitions = pk_spec(keys) ++ secondary_keys ++ global_keys
    [read, write]         = model.__throughput__
    local_indexes         = model.__local_indexes__
    global_indexes        = model.__global_indexes__

    table_name
    |> ExAws.Dynamo.create_table(keys_schema, attribute_definitions, read, write, global_indexes, local_indexes)
    |> ExAws.request
  end

  def describe_table(model) do
    case model.__table_name__
    |> ExAws.Dynamo.describe_table
    |> ExAws.request do
      {:ok, map} when is_map(map)->
        map["Table"]
      {:error, message} ->
        Surge.Exceptions.dynamic_raise message
    end
  end

  def update_table(model) do
    table_name            = model.__table_name__
    [read, write]         = model.__throughput__

    attributes = %{
      "ProvisionedThroughput" => %{
        "ReadCapacityUnits"  => read,
        "WriteCapacityUnits" => write
      }
    }

    table_name
    |> ExAws.Dynamo.update_table(attributes)
    |> ExAws.request
  end

  def delete_table(model) do
    model.__table_name__
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
