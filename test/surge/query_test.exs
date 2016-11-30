defmodule Surge.QueryTest do
  use ExUnit.Case

  defmodule HashRangeModel do
    use Surge.Model
    hash id: {:number, nil}
    range time: {:number, nil}
    attributes name: {:string, "foo"}, age: {:number, 0}, address: {:string, "example.st"}, sex: {:string, ""}
    index local: :name, range: :name, projection: :keys
    index local: :age, range: :age, projection: [:age]
    index local: :address, range: :address, projection: :all
    index global: :age_sex, hash: :age, range: :sex, projection: :keys, throughput: [read: 5, write: 2]
  end

  test "expression_attribute_names" do
    Surge.DDL.delete_table HashRangeModel
    Surge.DDL.create_table HashRangeModel

    key_condition_expression = "#id = ? and #time >= ?"
    assert ["#id": "id", "#time": "time"] == Surge.Query.expression_attribute_names(key_condition_expression, HashRangeModel)
  end

  test "expression_and_values" do
    Surge.DDL.delete_table HashRangeModel
    Surge.DDL.create_table HashRangeModel

    key_condition_expression = "#id = ? and #time >= ?"
    values = [1, 100]

    {exp, values_map} = Surge.Query.expression_and_values(key_condition_expression, values)

    assert "#id = :value1 and #time >= :value2" == exp
    assert [value1: 1, value2: 100] == values_map
  end

  test "build_query" do
    expect = %{"ExpressionAttributeNames" => %{"#id": "id", "#time": "time"},
               "ExpressionAttributeValues" => %{":value1" => %{"N" => "2"},
                                                ":value2" => %{"N" => "100"}},
               "KeyConditionExpression" => "#id = :value1 and #time >= :value2",
               "TableName" => "Surge.Test.HashRangeModel"}

    query_param = Surge.Query.build_query(["#id = ? and #time >= ?", 2, 100], HashRangeModel)

    assert expect == query_param.data
  end

  test "build_query + filter" do
    expect = %{"ExpressionAttributeNames" => %{"#id": "id", "#time": "time", "#age": "age"},
               "ExpressionAttributeValues" => %{":value1" => %{"N" => "2"},
                                                ":value2" => %{"N" => "100"},
                                                ":filter_value1" => %{"N" => "10"}},
               "KeyConditionExpression" => "#id = :value1 and #time >= :value2",
               "FilterExpression" => "#age >= :filter_value1",
               "TableName" => "Surge.Test.HashRangeModel"}

    query_param = Surge.Query.build_query(["#id = ? and #time >= ?", 2, 100], HashRangeModel, nil, :asec, ["#age >= ?", 10])

    assert expect == query_param.data
  end

  test "query" do
    Surge.DDL.delete_table HashRangeModel
    Surge.DDL.create_table HashRangeModel

    alice = %HashRangeModel{id: 2, time: 100, name: "alice", age: 20}
    Surge.DML.put_item(alice, into: HashRangeModel)

    bob = %HashRangeModel{id: 2, time: 200, name: "bob", age: 21}
    Surge.DML.put_item(bob, into: HashRangeModel)

    result = Surge.Query.query(where: ["#id = ? and #time >= ?", 2, 100], for: HashRangeModel)

    assert 2 == Enum.count(result)
    assert [alice, bob] == result

    result = Surge.Query.query(where: ["#id = ? and #time >= ?", 2, 100], for: HashRangeModel, filter: ["#age >= ?", 10])

    assert 2 == Enum.count(result)
    assert [alice, bob] == result

    result = Surge.Query.query(where: ["#id = ? and #time >= ?", 2, 100], for: HashRangeModel, filter: ["#age = ?", 21])

    assert 1 == Enum.count(result)
    assert [bob] == result

    result = Surge.Query.query(where: ["#id = ? and #time >= ?", 2, 100], for: HashRangeModel, limit: 1)

    assert 1 == Enum.count(result)
    assert alice == List.first(result)

    result = Surge.Query.query(where: ["#id = ? and #time >= ?", 2, 100], for: HashRangeModel, limit: 1, order: :desec)

    assert 1 == Enum.count(result)
    assert bob == List.first(result)
  end

  test "raise invalid operator in query" do
    Surge.DDL.delete_table HashRangeModel
    Surge.DDL.create_table HashRangeModel

    assert_raise Surge.Exceptions.ValidationException,
      "Invalid operator used in KeyConditionExpression: OR",
      fn -> Surge.Query.query(where: ["#id = ? OR #time >= ?", 2, 100], for: HashRangeModel) end
  end

  test "build_scan_query" do
    expect = %{"ExpressionAttributeNames" => %{"#id": "id", "#time": "time"},
               "ExpressionAttributeValues" => %{":value1" => %{"N" => "2"},
                                                ":value2" => %{"N" => "100"}},
               "FilterExpression" => "#id = :value1 and #time >= :value2",
               "TableName" => "Surge.Test.HashRangeModel"}

    query_param = Surge.Query.build_scan_query(["#id = ? and #time >= ?", 2, 100], HashRangeModel)

    assert expect == query_param.data
  end

  test "scan" do
    Surge.DDL.delete_table HashRangeModel
    Surge.DDL.create_table HashRangeModel

    alice = %HashRangeModel{id: 2, time: 100, name: "alice", age: 20}
    Surge.DML.put_item(alice, into: HashRangeModel)

    bob = %HashRangeModel{id: 2, time: 200, name: "bob", age: 20}
    Surge.DML.put_item(bob, into: HashRangeModel)

    result = Surge.Query.scan(filter: ["#id = ? and #time >= ?", 2, 100], for: HashRangeModel)

    assert 2 == Enum.count(result)
    assert [alice, bob] == result

    result = Surge.Query.scan(filter: ["#id = ? and #time >= ?", 2, 100], for: HashRangeModel, limit: 1)

    assert 1 == Enum.count(result)
    assert alice == List.first(result)
  end
end
