# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

config :postnord,
  data_path: "data/",
  port: 2010,
  grpc_port: 2011,
  replica_nodes: []

config :postnord, Postnord.IndexLog,
  buffer_size: (128 * 1024),
  flush_timeout: 5

config :postnord, Postnord.MessageLog,
  buffer_size: (4 * 1024),
  flush_timeout: 5

config :logger,
  level: :info,
  compile_time_purge_level: :info

config :grpc,
  start_server: true

# Disable lager, use default elixir Logger
config :lager, :error_logger_redirect, false
config :lager, :error_logger_whitelist, [Logger.ErrorHandler]
config :lager, :crash_log, false
config :lager, :handlers, [{LagerLogger, [level: :debug]}]

import_config "#{Mix.env}.exs"
