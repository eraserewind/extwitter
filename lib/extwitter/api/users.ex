defmodule ExTwitter.API.Users do
  @moduledoc """
  Provides users API interfaces.
  """

  import ExTwitter.API.Base

  def user_lookup(options) when is_list(options) do
    params = ExTwitter.Parser.parse_request_params(options)
    request(:get, "1.1/users/lookup.json", params)
      |> Enum.map(&ExTwitter.Parser.parse_user/1)
  end

  def user_lookup(screen_name, options \\ []) do
    user_lookup([screen_name: screen_name] ++ options)
  end

  def user_search(query, options \\ []) do
    params = ExTwitter.Parser.parse_request_params([q: query] ++ options)
    request(:get, "1.1/users/search.json", params)
      |> Enum.map(&ExTwitter.Parser.parse_user/1)
  end

  def user(user_id, screen_name, options \\ []) do
    params = ExTwitter.Parser.parse_request_params([user_id: user_id, screen_name: screen_name] ++ options)
    request(:get, "1.1/users/show.json", params)
      |> ExTwitter.Parser.parse_user
  end
end
