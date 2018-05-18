defmodule TestUtil.Rest do
  def get_message(host) do
    "#{host}/queue/q/message" |> HTTPoison.get!()
  end

  def post_message(host, message) do
    "#{host}/queue/q/message" |> HTTPoison.post!(message)
  end

  def accept_message(host, id) do
    "#{host}/queue/q/message/#{id}/accept" |> HTTPoison.post!("")
  end

  def replicate_message(host, id, timestamp, message) do
    "#{host}/queue/q/message/#{id}/timestamp/#{timestamp}/replicate"
    |> HTTPoison.post!(message)
  end

  def tombstone_message(host, id) do
    "#{host}/queue/q/message/#{id}/tombstone" |> HTTPoison.post!("")
  end

  def flush_queue(host) do
    "#{host}/queue/q/flush" |> HTTPoison.post!("")
  end

  def headers_message_id([{"message_id", id} | _]), do: id
  def headers_message_id([head | tail]), do: headers_message_id(tail)
  def headers_message_id([]), do: nil

  def random_message do
    RandomBytes.base62(1024)
  end
end
