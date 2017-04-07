defmodule Surge.DDLTest do
  use ExUnit.Case

  import Surge.DDL

  test "HashModel" do
    defmodule HashModel do
      use Surge.Model
      schema do
        hash id: {:string, ""}
      end
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
      schema do
        table_name "Surge.Test.HashModel"
        hash id: {:string, ""}
        throughput read: 10, write: 3
      end
    end

    assert UpdateHashModel.__throughput__ == [10, 3]

    describe_table UpdateHashModel
    update_table UpdateHashModel

    updated_table_info = describe_table UpdateHashModel
    assert Map.get(updated_table_info["ProvisionedThroughput"], "ReadCapacityUnits") == 10
    assert Map.get(updated_table_info["ProvisionedThroughput"], "WriteCapacityUnits") == 3

    delete_table HashModel
  end

  test "HashRangeModel" do
    defmodule HashRangeModel do
      use Surge.Model
      schema do
        hash id: {:string, ""}
        range time: {:number, nil}
        attributes name: {:string, "foo"}, age: {:number, 0}, address: {:string, "example.st"}, sex: {:string, ""}
        throughput read: 10, write: 3
        index local: :name, range: :name, projection: :keys
        index local: :age, range: :age, projection: [:age]
        index local: :address, range: :address, projection: :all
        index global: :age_sex, hash: :age, range: :sex, projection: :keys, throughput: [read: 5, write: 2]
      end
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

  test "GlobalSecondaryIndex hash only" do
    defmodule StaffTestModel do
      use Surge.Model
      schema do
        hash id: {:number, nil}
        attributes staff_id: {:number, nil}
        index global: :staff_id, hash: :staff_id, projection: :keys
      end
    end

    delete_table StaffTestModel
    {:ok, _} = create_table StaffTestModel
    table_info = describe_table StaffTestModel
    global_secondary_index = List.first(table_info["GlobalSecondaryIndexes"])
    assert global_secondary_index["IndexName"] == "Surge.Test.StaffTestModel.indexes.staff_id"
    assert global_secondary_index["KeySchema"] == [%{"AttributeName" => "staff_id", "KeyType" => "HASH"}]
  end

  test "GlobalIndexModel" do
    defmodule GlobalIndexModel do
      use Surge.Model
      schema do
        hash id: {:string, ""}
        range time: {:number, nil}
        attributes name: {:string, "foo"}, age: {:number, 0}, address: {:string, "example.st"}, sex: {:string, ""}
      end
    end

    delete_table GlobalIndexModel
    create_table GlobalIndexModel

    defmodule AddGlobalIndexModel do
      use Surge.Model
      schema do
        table_name "Surge.Test.GlobalIndexModel"
        hash id: {:string, ""}
        range time: {:number, nil}
        attributes name: {:string, "foo"}, age: {:number, 0}, address: {:string, "example.st"}, sex: {:string, ""}
        index global: :age_sex, hash: :age, projection: :keys, throughput: [read: 5, write: 2]
      end
    end

    {:ok, _} = update_table AddGlobalIndexModel
    table_info = describe_table AddGlobalIndexModel

    global_secondary_index = List.first(table_info["GlobalSecondaryIndexes"])
    assert global_secondary_index["IndexName"] == "Surge.Test.GlobalIndexModel.indexes.age_sex"
    assert global_secondary_index["KeySchema"] == [%{"AttributeName" => "age", "KeyType" => "HASH"}]
    assert global_secondary_index["ProvisionedThroughput"] == %{"ReadCapacityUnits" => 5,
                                                                "WriteCapacityUnits" => 2}

    defmodule AddAddGlobalIndexModel do
      use Surge.Model
      schema do
        table_name "Surge.Test.GlobalIndexModel"
        hash id: {:string, ""}
        range time: {:number, nil}
        attributes name: {:string, "foo"}, age: {:number, 0}, address: {:string, "example.st"}, sex: {:string, ""}
        index global: :age_sex, hash: :age, projection: :keys, throughput: [read: 5, write: 2]
        index global: :address_age, hash: :address, range: :age, projection: :keys, throughput: [read: 10, write: 4]
      end
    end

    :timer.sleep(1000);
    {:ok, _} = update_table AddAddGlobalIndexModel
    :timer.sleep(1000);
    table_info = describe_table AddAddGlobalIndexModel
    assert Enum.count(table_info["GlobalSecondaryIndexes"]) == 2

    defmodule DeleteGlobalIndexModel do
      use Surge.Model
      schema do
        table_name "Surge.Test.GlobalIndexModel"
        hash id: {:string, ""}
        range time: {:number, nil}
        attributes name: {:string, "foo"}, age: {:number, 0}, address: {:string, "example.st"}, sex: {:string, ""}
        index global: :address_age, hash: :address, range: :age, projection: :keys, throughput: [read: 10, write: 4]
      end
    end

    {:ok, _} = update_table DeleteGlobalIndexModel
    :timer.sleep(1000);
    table_info = describe_table DeleteGlobalIndexModel
    assert Enum.count(table_info["GlobalSecondaryIndexes"]) == 1

    global_secondary_index = List.first(table_info["GlobalSecondaryIndexes"])
    assert global_secondary_index["IndexName"] == "Surge.Test.GlobalIndexModel.indexes.address_age"
    assert global_secondary_index["ProvisionedThroughput"] == %{"ReadCapacityUnits" => 10,
                                                                "WriteCapacityUnits" => 4}
  end

  test "No Table" do
    defmodule NoTableModel do
      use Surge.Model
      schema do
        hash id: {:string, ""}
      end
    end

    assert_raise Surge.Exceptions.ResourceNotFoundException, "Cannot do operations on a non-existent table", fn -> describe_table NoTableModel end
  end
end
