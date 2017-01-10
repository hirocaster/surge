defmodule Surge.Query do

  require Surge.Exceptions

  def query(params) when is_list(params) do
    where  = params[:where]
    for    = params[:for]
    index  = params[:index]  || nil
    limit  = params[:limit]  || nil
    offset = params[:offset] || nil
    order  = params[:order]  || :asec
    filter = params[:filter] || nil

    do_query(where: where, for: for, index: index, limit: limit, offset: offset, order: order, filter: filter)
  end

  defp do_query(where: [exp | values], for: model, index: index, limit: limit, offset: offset, order: order, filter: filter) do
    [exp | values]
    |> build_query(model, index, limit, offset, order, filter)
    |> request!(model)
  end

  def build_query([exp | values], model, index \\ nil, limit \\ nil, offset \\ nil, order \\ :asec, filter \\ nil) do
    table_name = model.__table_name__

    indexes = Enum.map(model.__local_indexes__ ++ model.__global_indexes__, &(&1.index_name))
    index_name = Enum.find(indexes, &(&1 |> String.split(".") |> List.last == index |> Atom.to_string))

    {key_condition_expression, attribute_values} = Surge.Query.expression_and_values(exp, values)
    attribute_names = Surge.Query.expression_attribute_names(exp, model)

    opts = %{
      key_condition_expression: key_condition_expression,
      expression_attribute_values: attribute_values,
      expression_attribute_names: attribute_names
    } |> index(index_name) |> filter(filter, model) |> limit(limit) |> offset(offset) |> order(order)

    ExAws.Dynamo.query(table_name, opts)
  end

  def scan(params) when is_list(params) do
    filter = params[:filter] || nil
    for    = params[:for]
    limit  = params[:limit]  || nil

    do_scan(filter: filter, for: for, limit: limit)
  end

  defp do_scan(filter: filter, for: model, limit: limit) do
    filter
    |> build_scan_query(model, limit)
    |> request!(model)
  end

  def build_scan_query(filter, model, limit) when is_nil(filter) do
    table_name = model.__table_name__
    opts = %{} |> limit(limit)
    ExAws.Dynamo.scan(table_name, opts)
  end

  def build_scan_query([exp | values], model, limit) do
    table_name = model.__table_name__

    {filter_expression, attribute_values} = Surge.Query.expression_and_values(exp, values)
    attribute_names = Surge.Query.expression_attribute_names(exp, model)

    opts = %{
      filter_expression: filter_expression,
      expression_attribute_values: attribute_values,
      expression_attribute_names: attribute_names
    } |> limit(limit)

    ExAws.Dynamo.scan(table_name, opts)
  end

  defp index(opts, name) when is_nil(name), do: opts
  defp index(opts, name) do
    Map.merge(opts, %{index_name: name})
  end

  defp filter(opts, filter, _model) when is_nil(filter), do: opts
  defp filter(opts, [exp | values], model) do
    {filter_expression, attribute_values} = Surge.Query.expression_and_values(exp, values, "filter_value")
    attribute_names = Surge.Query.expression_attribute_names(exp, model)

    Map.merge(opts, %{
          filter_expression: filter_expression,
          expression_attribute_names: opts.expression_attribute_names ++ attribute_names,
          expression_attribute_values: opts.expression_attribute_values ++ attribute_values
              })
  end

  defp limit(opts, limit) when is_nil(limit), do: opts
  defp limit(opts, limit) do
    Map.merge(opts, %{limit: limit})
  end

  defp offset(opts, offset) when is_nil(offset), do: opts
  defp offset(opts, offset) do
    Map.merge(opts, %{exclusive_start_key: offset})
  end

  defp order(opts, order) when order == :asec, do: opts
  defp order(opts, order) when order == :desec do
      Map.merge(opts, %{scan_index_forward: false})
  end

  defp decode(values, model) when is_map(values) do
    ExAws.Dynamo.decode_item(values, as: model)
  end

  defp request!(query_param, model) do
    case request(query_param) do
      {:ok, result} ->
        Enum.map(result["Items"], fn(item) -> decode(item, model) end)
      {:error, msg} ->
        Surge.Exceptions.dynamic_raise msg
    end
  end

  defp request(query_param) do
    query_param |> ExAws.request
  end

  def expression_and_values(exp, values, prefix \\ "value") do
    question_replace_to_value_and_values_list(exp, [],values, 1, prefix)
  end

  defp question_replace_to_value_and_values_list(exp, values_list, values, _, _prefix) when values == [] do
    {exp, values_list}
  end
  defp question_replace_to_value_and_values_list(exp, values_list, values, n, prefix) do
    value            = List.first(values)
    added_values_list = values_list ++ ["#{prefix}#{n}": value]
    deleted_values   = List.delete(values, value)

    exp
    |> String.replace("?", ":#{prefix}#{n}", global: false)
    |> question_replace_to_value_and_values_list(added_values_list, deleted_values, n + 1, prefix)
  end

  def expression_attribute_names(key_condition_expression, model) do
    model
    |> names_of_keys_and_attributes
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

  defp names_of_keys_and_attributes(model) do
    key_names(model.__keys__) ++ key_names(model.__secondary_keys__) ++ key_names(model.__global_keys__) ++ key_names(model.__attributes__) |> Enum.uniq
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
