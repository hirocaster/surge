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
      {:error, message} ->
        Surge.Exceptions.dynamic_raise message
    end
  end

  def update_table(model) do
    table_name            = model.__table_name__
    keys                  = model.__keys__
    secondary_keys        = model.__secondary_keys__
    global_keys           = model.__global_keys__
    attribute_definitions = pk_spec(keys) ++ secondary_keys ++ global_keys
    [read, write]         = model.__throughput__

    attributes = %{}

    eixist_attribute_definitions = describe_table(model)["AttributeDefinitions"]
    exisst_attribute_names = Enum.map(eixist_attribute_definitions, &(String.to_atom(&1["AttributeName"]))) # [:id, :time]

    update_attr = Enum.reject(attribute_definitions, fn({key, _}) -> Enum.member?(exisst_attribute_names, key) end)

    if Enum.count(update_attr) > 0 do
      update_attributes = %{
        "AttributeDefinitions" => update_attr |> encode_key_definitions,
      }
      attributes = Map.merge(attributes, update_attributes)
    end


    table_info = describe_table(model)
    exist_read  = Map.get(table_info["ProvisionedThroughput"], "ReadCapacityUnits")
    exist_write = Map.get(table_info["ProvisionedThroughput"], "WriteCapacityUnits")

    unless [exist_read, exist_write] == [read, write] do
      update_previsioned_throughput = %{"ProvisionedThroughput" => %{
                                       "ReadCapacityUnits"  => read,
                                       "WriteCapacityUnits" => write
                                     }}
      attributes = Map.merge(attributes, update_previsioned_throughput)
    end


    global_indexes = model.__global_indexes__

    case describe_table(model)["GlobalSecondaryIndexes"] do
      nil ->
        if Enum.count(global_indexes) > 0 do

          indexes = for global_index <- global_indexes do
            %{"Create" => global_index}
          end

          global_secondary_index_updates = %{
            "GlobalSecondaryIndexUpdates" => indexes
          }
          attributes = Map.merge(attributes, global_secondary_index_updates)
        else
          nil
        end

      index_of_list when is_list(index_of_list) ->

        # diff create(model - db)
        create_indexes = Enum.reject(global_indexes, fn(index) ->

          Enum.find(index_of_list, nil, fn(exists_index) ->
            index.index_name == exists_index["IndexName"]
          end)
        end)

        indexes = for global_index <- create_indexes do
          %{"Create" => global_index}
        end
        global_secondary_index_updates = %{
          "GlobalSecondaryIndexUpdates" => indexes
        }
        attributes = Map.merge(attributes, global_secondary_index_updates)

        # diff delete index (db - model)
        delete_indexes = Enum.reject(index_of_list, fn(exists_index) ->

          Enum.find(global_indexes, nil, fn(model_index) ->
            model_index.index_name == exists_index["IndexName"]
          end)
        end)

        if Enum.count(delete_indexes) > 0 do
          indexes = for global_index <- delete_indexes do
            %{"Delete" => global_index}
          end
          index_updates = %{
            "GlobalSecondaryIndexUpdates" => indexes
          }
          attributes = Map.merge(attributes, index_updates)
        end
    end

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

  defp encode_key_definitions(attrs) do
    attrs |> Enum.map(fn({name, type}) ->
      %{"AttributeName" => name, "AttributeType" => type |> ExAws.Dynamo.Encoder.atom_to_dynamo_type}
    end)
  end
end
