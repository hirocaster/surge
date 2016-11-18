defmodule Surge.ModelTest do
  use ExUnit.Case

  test "default" do
    defmodule EmptyModel do
      use Surge.Model
    end
    namespace = Application.get_env(:surge, :namespace) || "surge"
    assert EmptyModel.__namespace__ == namespace
    assert EmptyModel.__keys__ == [hash: {:id, {:number, nil}}]
    assert EmptyModel.__canonical_name__ == "#{namespace}.EmptyModel"
    assert EmptyModel.__throughput__ == [3, 1]
  end

  defmodule HashModel do
    use Surge.Model
    hash id: {:string, ""}
    throughput read: 10, write: 3
    attributes name: {:string, "foo"}, age: {:number, 0}
  end

  test "HashModel" do
    assert HashModel.__keys__ == [hash: {:id, :string}]
    assert HashModel.__throughput__ == [10, 3]
    assert HashModel.__attributes__ == [age: {:number, 0}, name: {:string, "foo"}, id: {:string, ""}]

    hash_model = %Surge.ModelTest.HashModel{}
    assert hash_model.name == "foo"
    assert hash_model.age == 0
  end

  defmodule HashRangeModel do
    use Surge.Model
    hash id: {:string, ""}
    range time: {:number, nil}
    attributes name: {:string, "foo"}, age: {:number, 0}
  end

  test "HashRangeModel" do
    assert HashRangeModel.__keys__ == [hash: {:id, :string}, range: {:time, :number}]

    hash_range_model = %Surge.ModelTest.HashRangeModel{}
    assert hash_range_model.name == "foo"
    assert hash_range_model.age == 0
  end
end
