defmodule Surge.UtilTest do
  use ExUnit.Case

  test "Convert camel_array_map to snake_array_map" do
    camel_map  = [%{"IndexName" => "hello.index"}, %{"IndexName" => "world.index"}]
    snake_map = [%{index_name: "hello.index"}, %{index_name: "world.index"}]
    assert Surge.Util.camel_array_map_to_snake_array_map(camel_map) == snake_map
    assert List.first(snake_map).index_name == "hello.index"
  end

end
