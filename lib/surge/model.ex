defmodule Surge.Model do
  defmacro __using__(_) do
    quote do

      app_namespace = __MODULE__
      |> Atom.to_string
      |> String.split(".")
      |> List.delete_at(0)
      |> List.first

      mix_env = Mix.env
      |> Atom.to_string
      |> Macro.camelize

      default_namespace = Application.get_env(:surge, :namespace, "#{app_namespace}.#{mix_env}")

      table_name = __MODULE__
      |> Atom.to_string
      |> String.split(".")
      |> List.last

      default_table_name = "#{default_namespace}.#{table_name}"

      Module.put_attribute(__MODULE__, :table_name, default_table_name)
      Module.put_attribute(__MODULE__, :throughput, [3,1])
      Module.register_attribute(__MODULE__, :attributes, accumulate: true)
      Module.put_attribute(__MODULE__, :keys, [hash: {:id, {:number, nil}}])
      Module.put_attribute(__MODULE__, :secondary_keys, [])
      Module.put_attribute(__MODULE__, :global_keys, [])
      Module.register_attribute(__MODULE__, :local_indexes, accumulate: true)
      Module.register_attribute(__MODULE__, :global_indexes, accumulate: true)
      Module.register_attribute(__MODULE__, :all_indexes_def, accumulate: true)

      import Surge.Model
      @before_compile unquote(__MODULE__)
    end
  end

  defmacro __before_compile__(env) do
    target = env.module

    table_name = Module.get_attribute(target, :table_name)
    Module.put_attribute(target, :table_name, table_name)

    Module.eval_quoted __CALLER__, [
      Surge.Model.__def_struct__(target),
      Surge.Model.__def_indexes__(target),
      Surge.Model.__def_helper_funcs__(target)
    ]
  end

  def __def_struct__(mod) do
    # keys                 = Module.get_attribute(mod, :keys)
    attribs              = Module.get_attribute(mod, :attributes)
    fields               = attribs |> Enum.map(fn {name, {_type, default}} -> {name, default} end)

    # meta = %{table: canonical_table_name,
    #          keys: keys,
    #          attributes: attribs}

    # fields = [__meta__: Macro.escape(Macro.escape(meta))] ++ fields # double-escape for the doubly-quoted

    # quote in quote because we eval_quoted the result of the function
    quote bind_quoted: [fields: fields] do
      quote do
        defstruct unquote(fields)
      end
    end
  end

  def __def_indexes__(mod) do
    table_name      = Module.get_attribute(mod, :table_name)
    table_keys      = Module.get_attribute(mod, :keys)
    table_atts      = Module.get_attribute(mod, :attributes)
    all_indexes_def = Module.get_attribute(mod, :all_indexes_def)
    all_indexes_def |> Enum.each(&Surge.Model.__def_index__(mod, table_name, table_keys, table_atts, &1))
  end

  def __def_index__(mod, table_name, table_keys, table_atts, [{index_type, index_name} | rest]) do
    case index_type do
      :local ->
        [hash: {hash,_}, range: {range,_}] = table_keys
        hash  = Keyword.get(rest, :hash, hash)
        range = Keyword.get(rest, :range, range)
        projection_type = Keyword.get(rest, :projection, :keys)
        projection = projection(projection_type)
        index_name = index_name |> Atom.to_string |> String.split(".") |> List.last
        index_def = %{
          index_name: "#{table_name}.indexes.#{index_name}",
          key_schema: [%{attribute_name: hash, key_type: "HASH"},
                       %{attribute_name: range, key_type: "RANGE"}],
          projection: projection
        }
        {type, _default} = table_atts[range]
        Surge.Model.__secondary_key__(mod, range, type)
        Module.put_attribute(mod, :local_indexes, Macro.escape(index_def))
        :local
      :global ->
        hash  = rest[:hash]
        range = Keyword.get(rest, :range, nil)
        projection_type = Keyword.get(rest, :projection, :keys)
        projection = projection(projection_type)
        index_name = index_name |> Atom.to_string |> String.split(".") |> List.last

        {hash_type, _default} = table_atts[hash]
        Surge.Model.__global_key__(mod, hash, hash_type)

        key_schema = if range do
          {range_type, _default} = table_atts[range]
          Surge.Model.__global_key__(mod, range, range_type)
          [%{attribute_name: hash, key_type: "HASH"},
           %{attribute_name: range, key_type: "RANGE"}]
        else
            [%{attribute_name: hash, key_type: "HASH"}]
        end

        index_def = %{
          index_name: "#{table_name}.indexes.#{index_name}",
          key_schema: key_schema,
          projection: projection
        }
        [read: read, write: write] = Keyword.get(rest, :throughput, [read: 2, write: 1])
        index_def = Map.put(index_def, :provisioned_throughput, %{
          read_capacity_units: read,
          write_capacity_units: write,
        })
        Module.put_attribute(mod, :global_indexes, Macro.escape(index_def))
        :global
    end

    # fields = table_atts |> Enum.map(fn {name, type} -> {name, Type.default_value(type)} end)
    # meta = %IndexMetadata{ type: index_type, table: table, keys: [hash, range], name: index_name,
    #    attributes: []}
    # fields = [__meta__: Macro.escape(Macro.escape(meta))] ++ fields # double-escape for the doubly-quoted

    # quote bind_quoted: [fields: fields, index_name: index_name] do
    #   quote do
    #     defmodule unquote(index_name) do
    #       defstruct unquote(fields)
    #     end
    #   end
    # end
  end

  defp projection(:keys), do: %{projection_type: "KEYS_ONLY"}
  defp projection(:all), do: %{projection_type: "ALL"}
  defp projection(atts) when is_list(atts) do
    %{projection_type: "INCLUDE", non_key_attributes: atts |> Enum.map(&Atom.to_string(&1))}
  end

  def __def_helper_funcs__(mod) do
    table_name           = Module.get_attribute(mod, :table_name)
    keys                 = Module.get_attribute(mod, :keys)
    secondary_keys       = Module.get_attribute(mod, :secondary_keys)
    global_keys          = Module.get_attribute(mod, :global_keys)
    attribs              = Module.get_attribute(mod, :attributes)
    throughput           = Module.get_attribute(mod, :throughput)
    local_indexes        = Module.get_attribute(mod, :local_indexes)
    global_indexes       = Module.get_attribute(mod, :global_indexes)

    quote do
      def __table_name__, do: unquote(table_name)
      def __secondary_keys__, do: unquote(secondary_keys)
      def __global_keys__, do: unquote(global_keys)
      def __keys__, do: unquote(keys)
      def __attributes__, do: unquote(attribs)
      def __throughput__, do: unquote(throughput)
      def __local_indexes__, do: unquote(local_indexes)
      def __global_indexes__, do: unquote(global_indexes)
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

  def __secondary_key__(mod, name, type) do
    updated_secondary_keys = mod
      |> Module.get_attribute(:secondary_keys)
      |> Keyword.delete(name)
      |> Keyword.put(name, type)
      |> Enum.sort
    Module.put_attribute(mod, :secondary_keys, updated_secondary_keys)
  end

  def __global_key__(mod, name, type) do
    updated_global_keys = mod
      |> Module.get_attribute(:global_keys)
      |> Keyword.delete(name)
      |> Keyword.put(name, type)
      |> Enum.sort
    Module.put_attribute(mod, :global_keys, updated_global_keys)
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

  defmacro table_name(table_name) do
    quote bind_quoted: [table_name: table_name] do
      Surge.Model.__table_name__(__MODULE__, table_name)
    end
  end

  def __table_name__(mod, table_name) do
    Module.put_attribute(mod, :table_name, table_name)
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

  defmacro index(kw_list) do
    quote do
      Module.put_attribute(__MODULE__, :all_indexes_def, unquote(kw_list))
    end
  end
end
