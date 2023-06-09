defmodule BroadcastWorkload do
  alias BroadcastWorkload.Router

  def read_input do
    case IO.read(:stdio, :line) do
      :eof ->
        :ok

      {:error, reason} ->
        IO.puts(:stderr, "failed to process requests #{reason}")

      line ->
        line
        |> Jason.decode!(keys: :atoms)
        |> Router.dispatch()

        read_input()
    end
  end

  def start do
    children = [
      BroadcastWorkload.NodeRegistry,
      BroadcastWorkload.MessageRepository,
    ]

    opts = [strategy: :one_for_one, name: BroadcastWorkload.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def main(_args \\ []) do
    start()
    read_input()
  end
end
