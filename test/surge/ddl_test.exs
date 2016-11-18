defmodule Surge.DDLTest do
  use ExUnit.Case

  import Surge.DDL

  test "HashModel" do
    defmodule HashModel do
      use Surge.Model
      hash id: :string
    end

    create_table  HashModel

    table_info = describe_table HashModel
    assert table_info["AttributeDefinitions"] == [%{"AttributeName" => "id", "AttributeType" => "S"}]
    assert table_info["KeySchema"] == [%{"AttributeName" => "id", "KeyType" => "HASH"}]
    assert Map.get(table_info["ProvisionedThroughput"], "ReadCapacityUnits") == 3
    assert Map.get(table_info["ProvisionedThroughput"], "WriteCapacityUnits") == 1

    delete_table HashModel
  end

  test "HashRangeModel" do
    defmodule HashRangeModel do
      use Surge.Model
      hash id: :string
      range time: :number
      throughput read: 10, write: 3
    end

    create_table HashRangeModel

    table_info = describe_table HashRangeModel
    assert table_info["AttributeDefinitions"] == [%{"AttributeName" => "id", "AttributeType" => "S"}, %{"AttributeName" => "time", "AttributeType" => "N"}]
    assert table_info["KeySchema"] == [%{"AttributeName" => "id", "KeyType" => "HASH"}, %{"AttributeName" => "time", "KeyType" => "RANGE"}]
    assert Map.get(table_info["ProvisionedThroughput"], "ReadCapacityUnits") == 10
    assert Map.get(table_info["ProvisionedThroughput"], "WriteCapacityUnits") == 3

    delete_table HashRangeModel
  end

  test "No Table" do
    defmodule NoTableModel do
      use Surge.Model
      hash id: :string
    end

    assert_raise Surge.Exceptions.ResourceNotFoundException, "Cannot do operations on a non-existent table", fn -> describe_table NoTableModel end
  end
end
