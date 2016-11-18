defmodule Surge.ExceptionsTest do
  use ExUnit.Case

  require Surge.Exceptions

  test "Raise from dynamic_exception/1" do
    assert_raise Surge.Exceptions.TestException, "dynamic_raise/1 exception test", fn ->
      Surge.Exceptions.dynamic_raise({"TestException", "dynamic_raise/1 exception test"}) end
  end

  test "Don't show warning message, defined exception module" do
    raw = {"TestException", "dynamic_raise/1 exception test"}
    assert_raise Surge.Exceptions.TestException, "dynamic_raise/1 exception test", fn ->
      Surge.Exceptions.dynamic_raise(raw) end
    assert_raise Surge.Exceptions.TestException, "dynamic_raise/1 exception test", fn ->
      Surge.Exceptions.dynamic_raise(raw) end
  end
end
