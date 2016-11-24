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
  end
end
