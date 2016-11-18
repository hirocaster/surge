defmodule Surge do
  defmacro __using__(_) do
    quote do
      require Surge.DDL
      import Surge.DDL
    end
  end
end
