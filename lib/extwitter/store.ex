defmodule ExTwitter.Store do

  @stores [global: ExTwitter.Store.Env, local: ExTwitter.Store.ProcessDict]
  @types [:local, :global]
  @default_type :global

  def setup(type \\ auto_detect) do
    module = @stores[type]
    module.setup
    module
  end

  def get(type, key) when type in @types, do: setup(type).get(key)
  def set(type, key, value) when type in @types, do: setup(type).set(key, value)
  def get(store, key) when is_atom(store), do: store.get(key)
  def set(store, key, value) when is_atom(store), do: store.set(key, value)

  def get(key), do: get(auto_detect, key)
  def set(key, value), do: set(auto_detect, key, value)

  def auto_detect do
    if Process.get(:_ex_twitter_use, false) do
      :local
    else
      @default_type
    end
  end

  defmodule Env do
    def setup(), do: :ok
    def get(key) do
      Application.get_env(:ex_twitter, key, nil)
    end
    def set(key, value) do
      Application.set_env(:ex_twitter, key, value)
    end
  end

  defmodule ProcessDict do
    def setup(), do: Process.put(build_key("use"), true)
    def get(key) do
      Process.get(build_key(key))
    end
    def set(key, value) do
      Process.put(build_key(key), value)
    end

    def build_key(key) do
      "_ex_twitter_#{key}" |> String.to_atom
    end
  end

end
