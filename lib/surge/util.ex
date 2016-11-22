defmodule Surge.Util do
  def camel_array_map_to_snake_array_map(camel_array_map) do
    Enum.map(camel_array_map, fn(map) -> Map.new(Enum.map(map, fn({key, value}) -> {String.to_atom(Macro.underscore(key)), value} end)) end)
  end
end
