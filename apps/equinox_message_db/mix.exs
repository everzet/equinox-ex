defmodule Equinox.MessageDb.MixProject do
  use Mix.Project

  def project do
    [
      app: :equinox_message_db,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:equinox_core, in_umbrella: true},
      {:nimble_options, "~> 1.1"},
      {:postgrex, "~> 0.17"},
      {:jason, "~> 1.0"},
      {:mox, "~> 1.1", only: :test},
      {:benchee, "~> 1.2", only: :dev}
    ]
  end
end
