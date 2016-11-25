defmodule Surge.DMLTest do
  use ExUnit.Case

  import Surge.DML

  defmodule HashModel do
    use Surge.Model
    hash id: {:number, nil}
    attributes name: {:string, "foo"}, age: {:number, 0}
    index global: :age, hash: :age, projection: :keys
  end

  test "PutItem/GetItem" do
    Surge.DDL.delete_table HashModel
    Surge.DDL.create_table HashModel

    alice = %HashModel{id: 1, name: "alice", age: 20}

    assert %{} == put_item(alice, into: HashModel)
    assert Surge.DDL.describe_table(HashModel)["ItemCount"] == 1

    assert alice == get_item(HashModel, 1)
    assert nil == get_item(HashModel, 999)

    assert_raise Surge.Exceptions.NoDefindedRangeException, fn -> get_item(HashModel, 999, 999) end

    assert %{} == delete_item(hash: 1, from: HashModel)
    assert Surge.DDL.describe_table(HashModel)["ItemCount"] == 0

    assert %{} == delete_item(hash: 999, from: HashModel)
  end

  defmodule HashRangeModel do
    use Surge.Model
    hash id: {:number, nil}
    range time: {:number, nil}
    attributes name: {:string, "foo"}, age: {:number, 0}
  end

  test "PutItem/GetItem HashRange" do
    Surge.DDL.delete_table HashRangeModel
    Surge.DDL.create_table HashRangeModel

    alice = %HashRangeModel{id: 1, time: 100, name: "alice", age: 20}

    assert %{} == put_item(alice, into: HashRangeModel)
    assert Surge.DDL.describe_table(HashRangeModel)["ItemCount"] == 1

    assert alice == get_item(HashRangeModel, 1, 100)
    assert nil == get_item(HashRangeModel, 999, 999)

    assert %{} == delete_item(hash: 1, range: 100, from: HashRangeModel)
    assert Surge.DDL.describe_table(HashRangeModel)["ItemCount"] == 0
  end
end
