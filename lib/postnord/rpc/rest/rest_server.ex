defmodule Postnord.Rest.Server do
  require Logger

  @moduledoc """
  Launches the Cowboy REST server.

  NOTE: Because gRPC uses Cowboy 2.0 for HTTP/2 support, and Plug is only
  compatible with 1.x, we must build our REST server using the Cowboy API
  directly as opposed to the preferred method of using Plug.Router/Builder.
  """

  def start_link(port) do
    Logger.info "Starting #{__MODULE__} on port #{port}"
   	:cowboy.start_clear(:http, 100, [{:port, port}], %{
   		env: %{dispatch: dispatch()}
   	})
  end

  def dispatch do
    :cowboy_router.compile([
      {:_, [
        {"/[_status]", Postnord.Rest.Route.Status, []},
        {"/queue/:queue/message", Postnord.Rest.Route.Message, []}
      ]}
    ])
  end

end
