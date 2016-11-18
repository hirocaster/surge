defmodule Surge.Model do
  defmacro __using__(_) do
    quote do
      default_namespace = Application.get_env(:surge, :namespace, "surge")

      table_name = __MODULE__
      |> Atom.to_string
      |> String.split(".")
      |> List.last

      Module.put_attribute(__MODULE__, :namespace, default_namespace)
      Module.put_attribute(__MODULE__, :table_name, table_name)
      Module.put_attribute(__MODULE__, :throughput, [3,1])
      Module.register_attribute(__MODULE__, :attributes, accumulate: true)
      Module.put_attribute(__MODULE__, :keys, [hash: {:id, {:number, nil}}])

      import Surge.Model
      @before_compile unquote(__MODULE__)
    end
  end

  defmacro __before_compile__(env) do
    target = env.module

    namespace = Module.get_attribute(target, :namespace)
    table_name = Module.get_attribute(target, :table_name)
    Module.put_attribute(target, :canonical_table_name, "#{namespace}.#{table_name}")
    Module.eval_quoted __CALLER__, [
      Surge.Model.__def_struct__(target),
      Surge.Model.__def_helper_funcs__(target)
    ]
  end

  def __def_struct__(mod) do
    attribs              = Module.get_attribute(mod, :attributes)
    fields               = attribs |> Enum.map(fn {name, {_type, default}} -> {name, default} end)

    quote bind_quoted: [fields: fields] do
      quote do
        defstruct unquote(fields)
      end
    end
  end

  def __def_helper_funcs__(mod) do
    namespace            = Module.get_attribute(mod, :namespace)
    canonical_table_name = Module.get_attribute(mod, :canonical_table_name)
    keys                 = Module.get_attribute(mod, :keys)
    attribs              = Module.get_attribute(mod, :attributes)
    throughput           = Module.get_attribute(mod, :throughput)

    quote do
      def __namespace__, do: unquote(namespace)
      def __canonical_name__, do: unquote(canonical_table_name)
      def __keys__, do: unquote(keys)
      def __attributes__, do: unquote(attribs)
      def __throughput__, do: unquote(throughput)
    end
  end

  def __key__(mod, key_type, name, type) when key_type in [:hash, :range] do
    updated_keys = mod
      |> Module.get_attribute(:keys)
      |> Keyword.delete(key_type)
      |> Keyword.put(key_type, {name, type})
      |> Enum.sort

    Module.put_attribute(mod, :keys, updated_keys)
  end

  defmacro attributes(decl) do
    {list_of_attrs, _} = Code.eval_quoted(decl)
    for attr <- list_of_attrs do
      quote do: attribute([unquote(attr)])
    end
  end

  defmacro attribute(decl) do
    quote bind_quoted: [decl: decl] do
      {name, type, default} = case decl do
                       [{name, {type, default}}] -> {name, type, default}
                     end
      Surge.Model.__attribute__(__MODULE__, name, type, default)
    end
  end

  defmacro hash(pk) do
    quote bind_quoted: [pk: pk] do
      {name, type, default} = case pk do
                       [{name, {type, default}}] -> {name, type, default}
                     end

      Surge.Model.__attribute__(__MODULE__, name, type, default)
      Surge.Model.__key__(__MODULE__, :hash, name, type)
    end
  end

  defmacro range(sort_key) do
    quote bind_quoted: [sort_key: sort_key] do
      {name, type, default} = case sort_key do
                       [{name, {type, default}}] -> {name, type, default}
                     end

      Surge.Model.__attribute__(__MODULE__, name, type, default)
      Surge.Model.__key__(__MODULE__, :range, name, type)
    end
  end

  defmacro throughput(read: read, write: write) do
    quote do
      Module.put_attribute(__MODULE__, :throughput, [unquote(read), unquote(write)])
    end
  end

  def __attribute__(mod, name, type, default) do
    existing_attributes = Module.get_attribute(mod, :attributes)
    if Keyword.has_key?(existing_attributes, name) do
      raise ArgumentError, "Duplicate attribute #{name}"
    end
    Module.put_attribute(mod, :attributes, {name, {type, default}})
  end
end
