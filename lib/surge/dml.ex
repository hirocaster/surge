defmodule Surge.DML do
  def put_item(value, into: model) do
    put_item(value, into: model, opts: [])
  end
  def put_item(value, into: model, opts: opts) do
    table_name = model.__table_name__
    with req <- ExAws.Dynamo.put_item(table_name, Map.from_struct(value), opts),
         {:ok, result} <- ExAws.request(req),
      do: result
  end

  def get_item(hash: hash, from: model), do: get_item(hash: hash, from: model, opts: [])
  def get_item(hash: hash, from: model, opts: opts) do
    {name, _}  = model.__keys__[:hash]
    do_get_item(model, [{name, hash}], opts)
  end

  def get_item(hash: hash, range: range, from: model), do: get_item(hash: hash, range: range, from: model, opts: [])
  def get_item(hash: hash, range: range, from: model, opts: opts) do
    {hash_name, _}  = model.__keys__[:hash]
    {range_name, _} = get_range_key!(model)

    do_get_item(model, [{hash_name, hash}, {range_name, range}], opts)
  end


  defp do_get_item(model, keys, opts) do
    table_name = model.__table_name__
    with req <- ExAws.Dynamo.get_item(table_name, keys, opts),
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

  def delete_item(hash: hash, from: model), do: delete_item(hash: hash, from: model, opts: [])
  def delete_item(hash: hash, from: model, opts: opts) do
    {name, _}  = model.__keys__[:hash]
    do_delete_item(model, [{name, hash}], opts)
  end

  def delete_item(hash: hash, range: range, from: model), do: delete_item(hash: hash, range: range, from: model, opts: [])
  def delete_item(hash: hash, range: range, from: model, opts: opts) do
    {hash_name, _}  = model.__keys__[:hash]
    {range_name, _} = get_range_key!(model)

    do_delete_item(model, [{hash_name, hash}, {range_name, range}], opts)
  end

  defp do_delete_item(model, keys, opts) do
    table_name = model.__table_name__
    with req <- ExAws.Dynamo.delete_item(table_name, keys, opts),
         {:ok, result} <- ExAws.request(req),
      do: result
  end
end
