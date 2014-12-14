defmodule ExTwitter.Model.Tweet do
  defstruct contributors: nil, coordinates: nil, created_at: nil, entities: nil,
    favorite_count: nil, favorited: nil, geo: nil, id: nil, id_str: nil,
    in_reply_to_screen_name: nil, in_reply_to_status_id: nil,
    in_reply_to_status_id_str: nil, in_reply_to_user_id: nil,
    in_reply_to_user_id_str: nil, lang: nil, place: nil,
    retweet_count: nil, retweeted: nil, source: nil, text: nil,
    truncated: nil, user: nil
end

defmodule ExTwitter.Model.User do
  defstruct contributors_enabled: nil, created_at: nil, default_profile: nil,
    default_profile_image: nil, description: nil, entities: nil,
    favourites_count: nil, follow_request_sent: nil, followers_count: nil,
    following: nil, friends_count: nil, geo_enabled: nil, id: nil,
    id_str: nil, is_translation_enabled: nil, is_translator: nil,
    lang: nil, listed_count: nil, location: nil, name: nil,
    notifications: nil, profile_background_color: nil,
    profile_background_image_url: nil,
    profile_background_image_url_https: nil, profile_background_tile: nil,
    profile_image_url: nil, profile_image_url_https: nil,
    profile_link_color: nil, profile_sidebar_border_color: nil,
    profile_sidebar_fill_color: nil, profile_text_color: nil,
    profile_use_background_image: nil, protected: nil, screen_name: nil,
    statuses_count: nil, time_zone: nil, url: nil, utc_offset: nil,
    verified: nil
end

defmodule ExTwitter.Model.Entities do
  defstruct hashtags: nil, symbols: nil, urls: nil, user_mentions: nil
end

defmodule ExTwitter.Model.Trend do
  defstruct events: nil, name: nil, promoted_content: nil, query: nil
end

defmodule ExTwitter.Model.List do
  defstruct slug: nil, name: nil, created_at: nil, uri: nil, subscriber_count: nil,
    id_str: nil, member_count: nil, mode: nil, id: nil, full_name: nil,
    description: nil, user: nil, following: nil
end

defmodule ExTwitter.Model.Place do
  defstruct id: nil, url: nil, place_type: nil, name: nil, full_name: nil,
    country_code: nil, country: nil, contained_within: nil,
    bounding_box: nil, attributes: nil
end

defmodule ExTwitter.Model.DirectMessage do
  defstruct id: nil, created_at: nil, entities: nil, recipient: nil, sender: nil,
    text: nil
end

defmodule ExTwitter.Model.Geo do
  defstruct type: nil, coordinates: nil
end

defmodule ExTwitter.Model.DeletedTweet do
  defstruct status: nil
end

defmodule ExTwitter.Model.ScrubUserGeo do
  defstruct id: nil, up_to_status_id: nil
end

defmodule ExTwitter.Model.WithheldTweet do
  defstruct id: nil, user_id: nil, in_countries: []
end

defmodule ExTwitter.Model.WithheldUser do
  defstruct id: nil, in_countries: []
end

defmodule ExTwitter.Model.Limit do
  defstruct track: nil
end

defmodule ExTwitter.Model.StallWarning do
  defstruct message: nil, percent_full: nil
end

defmodule ExTwitter.Model.FollowsLimitWarning do
  defstruct message: nil, user_id: nil
end

defmodule ExTwitter.Model.FriendsList do
  defstruct list: []
end

defmodule ExTwitter.Model.Event do
  defstruct event: nil, created_at: nil, source: nil, target: nil, target_object: nil
end

defmodule ExTwitter.Model.Disconnect do
  defstruct code: nil, reason: nil, stream_name: nil
end
