defmodule ExTwitter.API.Streaming do
  @moduledoc """
  Provides streaming API interfaces.
  """

  @doc """
  The default timeout value (in milliseconds) for how long keeps waiting until next message arrives.
  """
  @default_stream_timeout 60_000
  @default_control_timeout 10_000

  @doc """
  Returns a small random sample of all public statuses.
  This method returns the Stream that holds the list of tweets.
  """
  def stream_sample(options \\ []) do
    {options, configs} = seperate_configs_from_options(options)
    params = ExTwitter.Parser.parse_request_params(options)
    pid = async_request(self, :stream, :get, "1.1/statuses/sample.json", params, configs)
    create_stream(pid, @default_stream_timeout)
  end

  @doc """
  Returns public statuses that match one or more filter predicates.
  This method returns the Stream that holds the list of tweets.
  Specify at least one of the [follow, track, locations] options.
  """
  def stream_filter(options, timeout \\ @default_stream_timeout) do
    {options, configs} = seperate_configs_from_options(options)
    params = ExTwitter.Parser.parse_request_params(options)
    pid = async_request(self, :stream, :post, "1.1/statuses/filter.json", params, configs)
    create_stream(pid, timeout)
  end

  @doc """
  Returns the User Stream
  """
  def stream_user(options, timeout \\ @default_stream_timeout) do
    {options, configs} = seperate_configs_from_options(options)
    params = ExTwitter.Parser.parse_request_params(options)
    pid = async_request(self, :user, :get, "1.1/user.json", params, configs)
    create_stream(pid, timeout)
  end

  defp seperate_configs_from_options(options) do
    config  = Keyword.take(options, [:receive_messages])
    options = Keyword.delete(options, :receive_messages)
    {options, config}
  end

  @doc """
  An interface to control the stream which keeps running infinitely.
  options can be used to specify timeout (ex. [timeout: 10000]).
  """
  def stream_control(pid, :stop, options \\ []) do
    timeout = options[:timeout] || @default_control_timeout

    send pid, {:control_stop, self}

    receive do
      :ok -> :ok
    after
      timeout -> :timeout
    end
  end

  defp async_request(processor, stream_type, method, path, params, configs) do
    oauth = ExTwitter.Config.get_tuples |> ExTwitter.API.Base.verify_params
    consumer = {oauth[:consumer_key], oauth[:consumer_secret], :hmac_sha1}

    spawn(fn ->
      response = ExTwitter.OAuth.request_async(
        method, request_url(stream_type, path), params, consumer, oauth[:access_token], oauth[:access_token_secret])

      case response do
        {:ok, request_id} ->
          process_stream(processor, request_id, configs)
        {:error, reason} ->
          send processor, {:error, reason}
      end
    end)
  end

  defp create_stream(pid, timeout) do
    Stream.resource(
      fn -> pid end,
      fn(pid) -> receive_next_tweet(pid, timeout) end,
      fn(pid) -> send pid, {:cancel, self} end)
  end

  defp receive_next_tweet(pid, timeout) do
    receive do
      {:stream, tweet} ->
        {[tweet], pid}

      {:control_stop, requester} ->
        send pid, {:cancel, self}
        send requester, :ok
        {:halt, pid}

      _ ->
        receive_next_tweet(pid, timeout)
    after
      timeout ->
        send pid, {:cancel, self}
        {:halt, pid}
    end
  end

  defp process_stream(processor, request_id, configs, acc \\ []) do
    receive do
      {:http, {request_id, :stream_start, headers}} ->
        send processor, {:header, headers}
        process_stream(processor, request_id, configs)

      {:http, {request_id, :stream, part}} ->
        cond do
          is_empty_message(part) ->
            process_stream(processor, request_id, configs, acc)

          is_end_of_message(part) ->
            message = Enum.reverse([part|acc])
                        |> Enum.join("")
                        |> parse_tweet_message(configs)
            if message do
              send processor, message
            end
            process_stream(processor, request_id, configs, [])

          true ->
            process_stream(processor, request_id, configs, [part|acc])
        end

      {:http, {_request_id, {:error, reason}}} ->
        send processor, {:error, reason}

      {:cancel, requester} ->
        :httpc.cancel_request(request_id)
        send requester, :ok

      _ ->
        process_stream(processor, request_id, configs)
    end
  end

  defp is_empty_message(part), do: part == "\r\n"
  defp is_end_of_message(part), do: part =~ ~r/\r\n$/

  defp parse_tweet_message(json, configs) do
    try do
      case ExTwitter.JSON.decode(json) do
        {:ok, tweet} ->
          if ExTwitter.JSON.get(tweet, "id_str") != [] do
            {:stream, ExTwitter.Parser.parse_tweet(tweet)}
          else
            if configs[:receive_messages] do
              parse_control_message(tweet)
            else
              nil
            end
          end

        {:error, error} ->
          {:error, {error, json}}
      end
    rescue
      error ->
        IO.inspect [error: error, json: json]
        nil
    end
  end

  defp parse_control_message(message) do
    case message do
      %{"direct_message" => dm} ->
        {:stream, parse_direct_message(dm)}

      %{"delete" => tweet} ->
        {:stream, %ExTwitter.Model.DeletedTweet{status: tweet["status"]}}

      %{"scrub_geo" => tweet} ->
        {:stream, %ExTwitter.Model.ScrubUserGeo{
                    id: tweet["user_id"],
                    up_to_status_id: tweet["up_to_status_id"]}}

      %{"limit" => limit} ->
        {:stream, %ExTwitter.Model.Limit{track: limit["track"]}}

      %{"warning" => warning=%{code: "FALLING_BEHIND"}} ->
        {:stream, %ExTwitter.Model.StallWarning{
                    message: warning["message"], percent_full: warning["percent_full"]}}

      %{"warning" => warning=%{code: "FOLLOWS_OVER_LIMIT"}} ->
        {:stream, %ExTwitter.Model.FollowsLimitWarning{
                    message: warning["message"], user_id: warning["user_id"]}}

      %{"friends" => list} when is_list(list) ->
        {:stream, %ExTwitter.Model.FriendsList{list: list}}

      event = %{"event" => type, "created_at" => at, "source" => source, "target" => target} ->
        {:stream, %ExTwitter.Model.Event{
                    event: type,
                    created_at: at,
                    source: create_struct(ExTwitter.Model.User, source),
                    target: create_struct(ExTwitter.Model.User, target),
                    target_object: parse_event_target_object(event)}}

      %{"disconnect" => disconnect} ->
        {:stream, %ExTwitter.Model.Disconnect{
                    code: disconnect["code"],
                    reason: disconnect["reason"],
                    stream_name: disconnect["stream_name"]}}

      %{"status_withheld" => %{"id" => id, "user_id" => user_id, "withheld_in_countries" => countries}} ->
        {:stream, %ExTwitter.Model.WithheldTweet{id: id, user_id: id, in_countries: countries}}

      %{"user_withheld" => %{"id" => id, "withheld_in_countries" => countries}} ->
        {:stream, %ExTwitter.Model.WithheldUser{id: id, in_countries: countries}}

      true -> nil
    end
  end

  # Convert `kv` keys to atoms and create a `struct` from it.
  defp create_struct(struct, kv) do
    struct(struct, kv |> Enum.map(fn({k,v}) -> {String.to_atom(k), v} end))
  end

  # Parses an Event target object
  # https://dev.twitter.com/streaming/overview/messages-types#Events_event
  @event_target_tweet ["favorite", "unfavorite"]
  @event_target_list ["list_created", "list_destroyed", "list_updated", "list_member_added", "list_member_removed",
    "list_user_subscribed", "list_user_unsubscribed"]

    defp parse_event_target_object(event=%{"event" => type}) when type in @event_target_tweet do
    create_struct(ExTwitter.Model.Tweet, Dict.get(event, "target_object"))
  end

  defp parse_event_target_object(event=%{"event" => type}) when type in @event_target_list do
    create_struct(ExTwitter.Model.List, Dict.get(event, "target_object"))
  end

  defp parse_event_target_object(event), do: Map.get(event, "target_object", nil)

  # Direct Message
  defp parse_direct_message(dm) do
    entities = create_struct(ExTwitter.Model.Entities, dm["entities"])
    recipient = create_struct(ExTwitter.Model.User, dm["recipient"])
    sender = create_struct(ExTwitter.Model.User, dm["sender"])
    %ExTwitter.Model.DirectMessage{
      id: dm["id"],
      entities: entities,
      recipient: recipient,
      sender: sender,
      text: dm["text"]}
  end

  defp request_url(:stream, path) do
    "https://stream.twitter.com/#{path}" |> to_char_list
  end

  defp request_url(:user, path) do
    "https://userstream.twitter.com/#{path}" |> to_char_list
  end
end
