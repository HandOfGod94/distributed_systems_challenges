defmodule BroadcastWorkload.Router do
  alias BroadcastWorkload.Controller

  def routes do
    %{
      "init" => &Controller.handle_init/1,
      "topology" => &Controller.handle_topology/1,
      "broadcast" => &Controller.handle_broadcast/1,
      "read" => &Controller.handle_read/1,
      "broadcast_ok" => &Controller.handle_broadcast_ok/1
    }
  end

  def dispatch(command) do
    with {:ok, handler_fn} <- resolve_method(command),
         {:ok, %{} = result} <- handler_fn.(command),
         {:ok, result} <- Jason.encode(result) do
      IO.puts(result)
    else
      {:ok, :noop} -> IO.puts(:stderr, "no operation required")
      {:error, error} -> IO.puts(:stderr, error)
    end
  end

  def resolve_method(%{body: %{type: type}}) do
    handler_fn = routes()[type]

    if handler_fn == nil do
      {:ok, :noop}
    else
      {:ok, handler_fn}
    end
  end
end
