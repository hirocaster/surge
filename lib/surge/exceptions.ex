defmodule Surge.Exceptions do

  @doc """
  Dynamic defined exception module and raise from table.

  raw_exception = {"FooException", "Message"}
  Surge.Exceptions.dynamic_raise(raw_exception) # => Define `Surge.Exceptions.FooException` module & it raise now
  """
  defmacro dynamic_raise(exception) when is_tuple(exception) do
    quote do
      exception_namespace =  unquote(__MODULE__)

      module_name = elem(unquote(exception), 0)
      message     = elem(unquote(exception), 1)

      case Code.eval_string "Code.ensure_loaded?(#{exception_namespace}.#{module_name})" do
        {false, _} ->
          Code.eval_string "defmodule #{exception_namespace}.#{module_name} do
                              defexception message: \"default message\"
                            end"
        {true, _} ->
          nil # defined the exception module
      end

      Code.eval_string "raise #{exception_namespace}.#{module_name}, \"#{message}\""
    end
  end
end

defmodule Surge.Exceptions.NoDefindedRangeException do
  defexception message: "No defined range key"
end
