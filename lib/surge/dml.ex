defmodule Surge.DML do
  def put_item(value, into: model) do
    table_name = model.__table_name__
    with req <- ExAws.Dynamo.put_item(table_name, Map.from_struct(value)),
         {:ok, result} <- ExAws.request(req),
      do: result
  end

  def get_item(model, hash) do
    table_name = model.__table_name__
    {name, _}  = model.__keys__[:hash]

    with req <- ExAws.Dynamo.get_item(table_name, [{name, hash}]),
         {:ok, result} <- ExAws.request(req),
           decoded <- decode(result, model),
      do: decoded
  end

  defp decode(values, _) when values == %{} do
    nil
  end
  defp decode(values, model) when is_map(values) do
    ExAws.Dynamo.decode_item(values, as: model)
  end
end
