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
    assert %{"#id" => "id", "#time" => "time"} == Surge.Query.expression_attribute_names(key_condition_expression, HashRangeModel)
  end

  test "expression_and_values" do
    Surge.DDL.delete_table HashRangeModel
    Surge.DDL.create_table HashRangeModel

    key_condition_expression = "#id = ? and #time >= ?"
    values = [1, 100]

    {exp, values_map} = Surge.Query.expression_and_values(key_condition_expression, values)

    assert "#id = :value1 and #time >= :value2" == exp
    assert %{":value1" => 1, ":value2" => 100} == values_map
  end
end
