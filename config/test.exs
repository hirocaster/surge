use Mix.Config

config :logger, level: :warn

config :ex_aws, :dynamodb,
  scheme: "http://",
  host: System.get_env("DYNAMODB_HOST") || "localhost",
  port: 8000,
  region: "us-east-1",
  http_client: ExAws.Request.HTTPoison,
  http_opts: []

# for debug http proxy
# config :ex_aws, :dynamodb,
#   scheme: "http://",
#   host: "dynamo.localhost.charlesproxy.com",
#   port: 8000,
#   region: "us-east-1",
#   http_client: ExAws.Request.HTTPoison,
#   http_opts: [{:proxy, "http://localhost:9999"}]
