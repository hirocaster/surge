use Mix.Config

config :logger, level: :warn

config :ex_aws, :dynamodb,
  scheme: "http://",
  host: "dynamo.localhost.charlesproxy.com",
  port: 8000,
  region: "us-east-1",
  http_client: ExAws.Request.HTTPoison,
  http_opts: [{:proxy, "http://localhost:9999"}]
