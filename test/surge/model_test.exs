defmodule Surge.ModelTest do
  use ExUnit.Case

  defmodule HashModel do
    use Surge.Model

    schema do
      hash id: {:string, ""}
      attributes name: {:string, "foo"}, age: {:number, 0}
      throughput read: 10, write: 3
    end

    def is_hash_model?(data) do
      case data do
        %HashModel{} -> true
        _ -> false
      end
    end
  end

  test "HashModel" do
    assert HashModel.__keys__ == [hash: {:id, :string}]
    assert HashModel.__table_name__ == "Surge.Test.HashModel"
    assert HashModel.__throughput__ == [10, 3]
    assert HashModel.__attributes__ == [age: {:number, 0}, name: {:string, "foo"}, id: {:string, ""}]

    hash_model = %Surge.ModelTest.HashModel{}
    assert hash_model.name == "foo"
    assert hash_model.age == 0
    assert Surge.ModelTest.HashModel.is_hash_model?(hash_model) == true
    assert Surge.ModelTest.HashModel.is_hash_model?("false") == false
  end

  defmodule HashRangeModel do
    use Surge.Model
    schema do
      hash id: {:string, ""}
      range time: {:number, nil}
      attributes name: {:string, "foo"}, age: {:number, 0}
      index local: Name, range: :name, projection: :keys
    end
  end

  test "HashRangeModel" do
    assert HashRangeModel.__keys__ == [hash: {:id, :string}, range: {:time, :number}]

    hash_range_model = %Surge.ModelTest.HashRangeModel{}
    assert hash_range_model.name == "foo"
    assert hash_range_model.age == 0
  end

  test "Define unknown type attribute" do
    assert_raise ArgumentError, "Unknown dynamo type for value: :unknown_type", fn ->
      defmodule UnknownTypeModel do
        use Surge.Model
        schema do
          hash id: {:number, nil}
          attributes name: {:unknown_type, nil}
        end
      end
    end
  end
end
