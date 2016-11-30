defmodule Surge.Query do

  require Surge.Exceptions

  def query(where: [exp | values], for: model), do: query(where: [exp | values], for: model, limit: nil, order: :asec)
  def query(where: [exp | values], for: model, limit: limit), do: query(where: [exp | values], for: model, limit: limit, order: :asec)
  def query(where: [exp | values], for: model, limit: limit, order: order) do
    query_param = build_query([exp | values], model, limit, order)

    case request(query_param) do
      {:ok, result} ->
        Enum.map(result["Items"], fn(item) -> decode(item, model) end)
      {:error, msg} ->
        Surge.Exceptions.dynamic_raise msg
    end
  end

  def build_query([exp | values], model, limit \\ nil, order \\ :asec) do
    table_name = model.__table_name__

    {key_condition_expression, attribute_values} = Surge.Query.expression_and_values(exp, values)
    attribute_names = Surge.Query.expression_attribute_names(exp, model)

    opts = %{
      key_condition_expression: key_condition_expression,
      expression_attribute_values: attribute_values,
      expression_attribute_names: attribute_names
    } |> limit(limit) |> order(order)


    ExAws.Dynamo.query(table_name, opts)
  end

  defp limit(opts, limit) when is_nil(limit), do: opts
  defp limit(opts, limit) do
    Map.merge(opts, %{limit: limit})
  end

  defp order(opts, order) when order == :asec, do: opts
  defp order(opts, order) when order == :desec do
      Map.merge(opts, %{scan_index_forward: false})
  end

  defp decode(values, model) when is_map(values) do
    ExAws.Dynamo.decode_item(values, as: model)
  end

  defp request(query_param) do
    query_param |> ExAws.request
  end

  def expression_and_values(exp, values) do
    question_replace_to_value_and_values_list(exp, [],values, 1)
  end

  defp question_replace_to_value_and_values_list(exp, values_list, values, _) when values == [] do
    {exp, values_list}
  end
  defp question_replace_to_value_and_values_list(exp, values_list, values, n) do
    value            = List.first(values)
    added_values_list = values_list ++ ["value#{n}": value]
    deleted_values   = List.delete(values, value)

    exp
    |> String.replace("?", ":value#{n}", global: false)
    |> question_replace_to_value_and_values_list(added_values_list, deleted_values, n + 1)
  end

  def expression_attribute_names(key_condition_expression, model) do
    model
    |> names_of_keys
    |> expression_using_keys(key_condition_expression)
    |> expression_attribute_names_format
  end

  defp expression_attribute_names_format(key_names_of_list) do
    key_names_of_list
    |> Enum.map(fn(key) ->
      s_key = Atom.to_string(key)
      ["##{s_key}": "#{s_key}"]
    end) |> List.flatten
  end

  defp expression_using_keys(keys, key_condition_expression) do
    keys
    |> Enum.map(fn(key) ->
      if String.contains?(key_condition_expression, "##{Atom.to_string(key)}") do
        key
      end
    end)
    |> Enum.reject(fn(x) -> x == nil end)
  end

  defp names_of_keys(model) do
    key_names(model.__keys__) ++ key_names(model.__secondary_keys__) ++ key_names(model.__global_keys__) |> Enum.uniq
  end

  defp key_names([hash: {hname, _htype}, range: {rname, _rtype}]) do
    [hname, rname]
  end
  defp key_names([hash: {hname, _htype}]) do
    [hname]
  end
  defp key_names(keys) when is_list(keys) do
    Enum.map(keys, fn({key, _value}) -> key end)
  end
end
