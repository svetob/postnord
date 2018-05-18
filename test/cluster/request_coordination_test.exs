defmodule Postnord.Test.Cluster.RequestCoordination do
  require Logger
  use ExUnit.Case, async: false

  @moduledoc """
  Cluster tests which verify requests are correctly coordinated
  accross multiple nodes.
  """

  @req_opts [timeout: 10_000_000]

  setup_all do
    ports = [2021, 2022, 2023]
    cluster = TestUtil.Cluster.create(ports)
    Process.sleep(1_000)

    on_exit fn ->
      TestUtil.Cluster.teardown(cluster)
    end

    uris = ports |> Enum.map(fn p -> "localhost:#{p}" end)

    [cluster: cluster, uris: uris]
  end

  setup context do
    on_exit fn ->
      Logger.info "Flushing cluster"
      # TODO
    end
    context
  end


  test "can write to one node then read from another", context do
    [uri_a, uri_b, _] = context[:uris]

    message = RandomBytes.base62(1024)

    # TODO
  end

  test "cannot see message from any node after queue flush", context do
    uri_a = hd(context[:uris])

    message = RandomBytes.base62(1024)

    # TODO Write

    # TODO Flush

    Process.sleep(100)

    context[:uris] |> Enum.each(fn node ->
      # TODO Assert empty read
    end)
  end


  test "cannot see a message from any node once it has been read", context do
    [uri_a, uri_b, _] = context[:uris]

    message = RandomBytes.base62(1024)

    # TODO Write

    # TODO Read

    context[:uris] |> Enum.each(fn node ->
      # TODO Assert empty read
    end)
  end

  test "cannot see a message from any node once it has been accepted", context do
    [uri_a, uri_b, _] = context[:uris]

    message = RandomBytes.base62(1024)

    # TODO Write

    # TODO Read

    # TODO Accept

    Process.sleep(100)

    context[:uris] |> Enum.each(fn node ->
      # TODO Assert empty read
    end)
  end
end
