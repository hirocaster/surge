defmodule Surge.DML do
  def put_item(value, into: model) do
    table_name = model.__table_name__
    with req <- ExAws.Dynamo.put_item(table_name, Map.from_struct(value)),
         {:ok, result} <- ExAws.request(req),
      do: result
  end

  def get_item(model, hash) do
    {name, _}  = model.__keys__[:hash]
    do_get_item(model, [{name, hash}])
  end

  def get_item(model, hash, range) do
    {hash_name, _}  = model.__keys__[:hash]
    {range_name, _} = get_range_key!(model)

    do_get_item(model, [{hash_name, hash}, {range_name, range}])
  end

  defp do_get_item(model, opts) do
    table_name = model.__table_name__
    with req <- ExAws.Dynamo.get_item(table_name, opts),
         {:ok, result} <- ExAws.request(req),
           decoded <- decode(result, model),
      do: decoded
  end

  defp get_range_key!(model) do
    if model.__keys__[:range] do
      model.__keys__[:range]
    else
      raise Surge.Exceptions.NoDefindedRangeException, "No defined range key in #{model}"
    end
  end

  defp decode(values, _) when values == %{} do
    nil
  end
  defp decode(values, model) when is_map(values) do
    ExAws.Dynamo.decode_item(values, as: model)
  end
end
