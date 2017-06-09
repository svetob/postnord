use Mix.Config

config :postnord,
  data_path: "data/",
  port: 2010,
  replica_nodes: ["localhost:2010","localhost:2010"]

config :logger,
    level: :info,
    compile_time_purge_level: :info
