defmodule BedrockMinimalHostApp.BedrockRepo do
  @moduledoc """
  Bedrock repository for the example app job queue.
  """

  use Bedrock.Repo, cluster: BedrockMinimalHostApp.BedrockCluster
end
