defmodule BedrockMinimalHostApp.MixProject do
  use Mix.Project

  def project do
    [
      app: :bedrock_minimal_host_app,
      version: "0.1.0",
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {BedrockMinimalHostApp.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:bedrock, "~> 0.4.0"},
      {:bedrock_job_queue, "~> 0.1"},
      {:bypass, "~> 2.1", only: :test},
      {:ecto_sql, "~> 3.13"},
      {:postgrex, ">= 0.0.0"},
      {:squid_mesh, path: "../.."}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get", "squid_mesh.install", "ecto.setup"],
      "ecto.setup": ["ecto.create", "ecto.migrate"],
      "ecto.reset": ["ecto.drop", "ecto.setup"]
    ]
  end
end
