# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

config :postnord,
  data_path: "data/",
  port: 2010,
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

import_config "#{Mix.env}.exs"
