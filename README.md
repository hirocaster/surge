# Surge

Amazon DynamoDB for Elixir

The library is still very much a work in progress.

## Installation

  1. Add `surge` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:surge, "~> 0.0.1"}]
    end
    ```

  2. Ensure `surge` is started before your application:

    ```elixir
    def application do
      [applications: [:surge]]
    end
    ```
