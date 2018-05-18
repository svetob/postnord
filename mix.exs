defmodule Postnord.Mixfile do
  use Mix.Project

  def project do
    [
      app: :postnord,
      version: "0.1.0",
      elixir: "~> 1.4.5",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      deps: deps()
    ]
  end

  # Configuration for escript binary compiler
  def escript_config do
    [
      main_module: Postnord,
      path: "bin/postnord"
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:random_bytes, "~> 1.0"},
      {:httpoison, "~> 0.11.1"},
      {:poison, "~> 3.1"},

      # CLI
      {:commando, "~> 0.1"},

      # HTTP REST API

      # Logging
      {:lager_logger, "~> 1.0"},
      {:lager, "3.5.1", override: true},

      # Dev tools
      {:credo, "~> 0.5", only: :dev, runtime: false},
      {:dialyxir, "~> 0.4", only: :dev, runtime: false},
      #{:remix, "~> 0.0.1", only: :dev}, # Automatic hot code reload when saving file

      # Test utils
      {:httpotion, "~> 3.0.2"}
    ]
  end
end
