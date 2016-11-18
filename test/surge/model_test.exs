defmodule Surge.ModelTest do
  use ExUnit.Case

  test "default" do
    defmodule EmptyModel do
      use Surge.Model
    end
    namespace = Application.get_env(:surge, :namespace) || "surge"
    assert EmptyModel.__namespace__ == namespace
    assert EmptyModel.__keys__ == [hash: {:id, :number}]
    assert EmptyModel.__canonical_name__ == "#{namespace}.EmptyModel"
    assert EmptyModel.__throughput__ == [3, 1]
  end

  test "HashModel" do
    defmodule HashModel do
      use Surge.Model
      hash id: :string
      throughput read: 10, write: 3
    end
    assert HashModel.__keys__ == [hash: {:id, :string}]
    assert HashModel.__throughput__ == [10, 3]
  end

  test "HashRangeModel" do
    defmodule HashRangeModel do
      use Surge.Model
      hash id: :string
      range time: :number
    end
    assert HashRangeModel.__keys__ == [hash: {:id, :string}, range: {:time, :number}]
  end
end
