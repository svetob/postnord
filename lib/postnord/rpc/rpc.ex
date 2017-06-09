defmodule Postnord.RPC do
  require Logger

  def replicate(id, bytes, caller \\ self()) do
    hosts = replica_nodes()
    hosts |> Enum.each(fn host ->
      spawn fn ->
        result = replicate_to(host, id, bytes)
        send caller, {:replicate, host, result}
      end
    end)
    hosts
  end

  def replicate_await(hosts, timeout \\ 5_000) do
    replicate_await(hosts, cluster_quorum(), Enum.count(hosts), timeout)
  end

  defp replicate_await(_, quorum, ok, _timeout) when ok >= quorum do
    :ok
  end
  defp replicate_await([], quorum, ok, _timeout) when ok < quorum do
    {:error, "#{quorum} replicas needed, #{ok} succeded"}
  end
  defp replicate_await(hosts, quorum, ok, timeout) when ok >= quorum do
    receive do
      {:replicate, host, :ok} ->
        replicate_await(hosts |> List.delete(host), quorum, ok + 1, timeout)
      {:replicate, host, {:error, reason}} ->
        Logger.warn "Replication to #{host} failed: #{inspect reason}"
        replicate_await(hosts |> List.delete(host), quorum, ok, timeout)
    after
      timeout -> {:error, :timeout}
    end
  end

  defp cluster_quorum do
    nodes = :postnord |> Application.get_env(:replica_nodes) |> Enum.count()
    round(Float.ceil(nodes/2))
  end

  defp replica_nodes do
    Application.get_env(:postnord, :replica_nodes)
  end

  defp replicate_to(host, id, bytes) do
    try do
      url = "#{host}0/__replicate/#{id}"
      case HTTPoison.post!(url, bytes) do
        %HTTPoison.Response{status_code: 201} ->
          :ok
        %HTTPoison.Response{status_code: 500} = resp ->
          {:error, resp.body}
      end
    rescue
      e in HTTPoison.Error ->
        {:error, e.reason}
    end
  end
end
