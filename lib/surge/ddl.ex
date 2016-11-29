defmodule Surge.DDL do

  require Surge.Exceptions

  def create_table(model) do
    table_name            = model.__table_name__
    keys                  = model.__keys__
    secondary_keys        = model.__secondary_keys__
    global_keys           = model.__global_keys__
    keys_schema           = pk_schema(keys)
    attribute_definitions = pk_spec(keys) ++ secondary_keys ++ global_keys |> Enum.uniq
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
      {:error, msg} ->
        Surge.Exceptions.dynamic_raise msg
    end
  end

  def update_table(model) do
    keys                  = model.__keys__
    secondary_keys        = model.__secondary_keys__
    global_keys           = model.__global_keys__
    attribute_definitions = pk_spec(keys) ++ secondary_keys ++ global_keys
    [read, write]         = model.__throughput__
    global_indexes        = model.__global_indexes__
    describe_table        = describe_table(model)

    attributes = %{}
    |> update_attribute_defnitions(attribute_definitions, describe_table)
    |> update_provisioned_throughput([read, write], describe_table)
    |> update_global_secondary_indexes(global_indexes, describe_table)

    model.__table_name__
    |> ExAws.Dynamo.update_table(attributes)
    |> ExAws.request
  end

  defp update_provisioned_throughput(request_body, [read, write], describe_table) do
    exists_read  = Map.get(describe_table["ProvisionedThroughput"], "ReadCapacityUnits")
    exists_write = Map.get(describe_table["ProvisionedThroughput"], "WriteCapacityUnits")

    if [exists_read, exists_write] == [read, write] do
      request_body
    else
      update_previsioned_throughput = %{"ProvisionedThroughput" => %{
                                       "ReadCapacityUnits"  => read,
                                       "WriteCapacityUnits" => write
                                     }}
      Map.merge(request_body, update_previsioned_throughput)
    end
  end

  defp update_attribute_defnitions(request_body, attribute_definitions, describe_table) do
    add_attributes_defnitions = Enum.reject(attribute_definitions, fn({key, _}) -> Enum.member?(exsists_attribute_names(describe_table), key) end)

    if Enum.count(add_attributes_defnitions) > 0 do
      update_attributes = %{
        "AttributeDefinitions" => add_attributes_defnitions |> encode_key_definitions,
      }

      Map.merge(request_body, update_attributes)
    else
      request_body
    end
  end

  defp exsists_attribute_names(describe_table) do
    describe_table["AttributeDefinitions"]
    |> Enum.map(&(String.to_atom(&1["AttributeName"]))) # => [:id, :time]
  end

  defp update_global_secondary_indexes(request_body, global_indexes, describe_table) do
    case describe_table["GlobalSecondaryIndexes"] do
      nil ->
        global_indexes_update_body(request_body, create_index_body_in_global_indexes_update(global_indexes))

      exists_indexes_by_describe_table when is_list(exists_indexes_by_describe_table) ->
        exists_indexes = exists_indexes_by_describe_table
        |> Surge.Util.camel_array_map_to_snake_array_map

        # diff create(model - db)
        create_indexes_body = global_indexes
        |> subtraction_index(exists_indexes)
        |> create_index_body_in_global_indexes_update

        # diff delete index (db - model)
        delete_indexes_body = exists_indexes
        |> subtraction_index(global_indexes)
        |> delete_index_body_in_global_indexes_update

        request_body
        |> global_indexes_update_body(create_indexes_body)
        |> global_indexes_update_body(delete_indexes_body)
    end
  end

  defp subtraction_index(base_indexes, subtraction_indexes) do
    Enum.reject(base_indexes, fn(exists_index) ->
      Enum.find(subtraction_indexes, nil, fn(sub_index) ->
        sub_index.index_name == exists_index.index_name
      end)
    end)
  end

  defp global_indexes_update_body(request_body, indexes_updates) do
    if Enum.count(indexes_updates) > 0 do
      global_secondary_index_updates = %{
        "GlobalSecondaryIndexUpdates" => indexes_updates
      }
      Map.merge(request_body, global_secondary_index_updates)
    else
      request_body
    end
  end

  defp create_index_body_in_global_indexes_update(global_indexes) do
    for index <- global_indexes do
      %{"Create" => index}
    end
  end

  defp delete_index_body_in_global_indexes_update(global_indexes) do
    for index <- global_indexes do
      %{"Delete" => index}
    end
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

  defp encode_key_definitions(attrs) do
    attrs |> Enum.map(fn({name, type}) ->
      %{"AttributeName" => name, "AttributeType" => type |> ExAws.Dynamo.Encoder.atom_to_dynamo_type}
    end)
  end
end
