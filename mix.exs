defmodule Postnord.Mixfile do
  use Mix.Project

  def project do
    [
      app: :postnord,
      version: "0.1.0",
      elixir: "~> 1.6",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
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
      # ID generation and representation
      {:random_bytes, "~> 1.0"},
      {:base62, "~> 1.2"},

      # HTTP client
      {:httpoison, "~> 1.1.1"},

      # REST API
      {:cowboy, "~> 2.4", override: true},
      {:plug, "~> 1.5"},
      {:poison, "~> 3.1"},

      # CLI
      {:commando, "~> 0.1"},

      # Dev tools
      {:credo, "~> 0.5", only: :dev, runtime: false},
      {:dialyxir, "~> 0.4", only: :dev, runtime: false},
      {:remix, "~> 0.0.1", only: :dev},

      # Test utils
      {:httpotion, "~> 3.0.2"}
    ]
  end
end
