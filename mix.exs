defmodule Postnord.Mixfile do
  use Mix.Project

  def project do
    [app: :postnord,
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
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

      # CLI
      {:commando, "~> 0.1"},

      # HTTP REST API
      {:cowboy, "~> 1.0"},
      {:plug, "~> 1.1"},

      # Dev tools
      {:credo, "~> 0.5", only: :dev, runtime: false},
      {:dialyxir, "~> 0.4", only: :dev, runtime: false}
    ]
  end
end
