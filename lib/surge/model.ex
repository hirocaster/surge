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
      Module.put_attribute(__MODULE__, :keys, [hash: {:id, :number}])

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
      Surge.Model.__def_helper_funcs__(target)
    ]
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

  defmacro hash(pk) do
    quote bind_quoted: [pk: pk] do
      {name, type} = case pk do
                       [{name, type}] -> {name, type}
                     end

      Surge.Model.__attribute__(__MODULE__, name, type)
      Surge.Model.__key__(__MODULE__, :hash, name, type)
    end
  end

  defmacro range(sort_key) do
    quote bind_quoted: [sort_key: sort_key] do
      {name, type} = case sort_key do
                       [{name, type}] -> {name, type}
                     end

      Surge.Model.__attribute__(__MODULE__, name, type)
      Surge.Model.__key__(__MODULE__, :range, name, type)
    end
  end

  defmacro throughput(read: read, write: write) do
    quote do
      Module.put_attribute(__MODULE__, :throughput, [unquote(read), unquote(write)])
    end
  end

  def __attribute__(mod, name, type) do
    existing_attributes = Module.get_attribute(mod, :attributes)
    if Keyword.has_key?(existing_attributes, name) do
      raise ArgumentError, "Duplicate attribute #{name}"
    end
    Module.put_attribute(mod, :attributes, {name, type})
  end
end
