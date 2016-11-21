defmodule Surge.DDLTest do
  use ExUnit.Case

  import Surge.DDL

  test "HashModel" do
    defmodule HashModel do
      use Surge.Model
      hash id: {:string, ""}
    end

    delete_table HashModel
    create_table HashModel

    table_info = describe_table HashModel
    assert table_info["AttributeDefinitions"] == [%{"AttributeName" => "id", "AttributeType" => "S"}]
    assert table_info["KeySchema"] == [%{"AttributeName" => "id", "KeyType" => "HASH"}]
    assert Map.get(table_info["ProvisionedThroughput"], "ReadCapacityUnits") == 3
    assert Map.get(table_info["ProvisionedThroughput"], "WriteCapacityUnits") == 1

    assert HashModel.__throughput__ == [3, 1]

    defmodule UpdateHashModel do
      use Surge.Model
      table_name "Surge.Test.HashModel"
      hash id: {:string, ""}
      throughput read: 10, write: 3
    end

    assert UpdateHashModel.__throughput__ == [10, 3]

    update_table UpdateHashModel

    updated_table_info = describe_table UpdateHashModel
    assert Map.get(updated_table_info["ProvisionedThroughput"], "ReadCapacityUnits") == 10
    assert Map.get(updated_table_info["ProvisionedThroughput"], "WriteCapacityUnits") == 3

    delete_table HashModel
  end

  test "HashRangeModel" do
    defmodule HashRangeModel do
      use Surge.Model
      hash id: {:string, ""}
      range time: {:number, nil}
      attributes name: {:string, "foo"}, age: {:number, 0}, address: {:string, "example.st"}, sex: {:string, ""}
      throughput read: 10, write: 3
      index local: :name, range: :name, projection: :keys
      index local: :age, range: :age, projection: [:age]
      index local: :address, range: :address, projection: :all
      index global: :age_sex, hash: :age, range: :sex, projection: :keys, throughput: [read: 5, write: 2]
    end

    delete_table HashRangeModel
    {:ok, _} = create_table HashRangeModel

    table_info = describe_table HashRangeModel

    assert table_info["AttributeDefinitions"] == [%{"AttributeName" => "id", "AttributeType" => "S"},
                                                  %{"AttributeName" => "time", "AttributeType" => "N"},
                                                  %{"AttributeName" => "address","AttributeType" => "S"},
                                                  %{"AttributeName" => "age", "AttributeType" => "N"},
                                                  %{"AttributeName" => "name", "AttributeType" => "S"},
                                                  %{"AttributeName" => "sex", "AttributeType" => "S"}]

    assert table_info["KeySchema"] == [%{"AttributeName" => "id", "KeyType" => "HASH"},
                                       %{"AttributeName" => "time", "KeyType" => "RANGE"}]
    assert Map.get(table_info["ProvisionedThroughput"], "ReadCapacityUnits") == 10
    assert Map.get(table_info["ProvisionedThroughput"], "WriteCapacityUnits") == 3

    assert Enum.count(table_info["LocalSecondaryIndexes"]) == 3
    assert Enum.count(table_info["GlobalSecondaryIndexes"]) == 1

    global_secondary_index = List.first(table_info["GlobalSecondaryIndexes"])
    assert global_secondary_index["IndexName"] == "Surge.Test.HashRangeModel.indexes.age_sex"
    assert global_secondary_index["KeySchema"] == [%{"AttributeName" => "age", "KeyType" => "HASH"},
                                                   %{"AttributeName" => "sex", "KeyType" => "RANGE"}]
    assert global_secondary_index["ProvisionedThroughput"] == %{"ReadCapacityUnits" => 5,
                                                                "WriteCapacityUnits" => 2}

    {:ok, _} = delete_table HashRangeModel
  end

  test "No Table" do
    defmodule NoTableModel do
      use Surge.Model
      hash id: {:string, ""}
    end

    assert_raise Surge.Exceptions.ResourceNotFoundException, "Cannot do operations on a non-existent table", fn -> describe_table NoTableModel end
  end
end
