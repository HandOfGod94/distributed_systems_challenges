defmodule BroadcastWorkload.Requester do
  use GenServer

  @timeout 100

  def start_link(init_args) do 
    GenServer.start_link(__MODULE__, init_args)
  end

  def fire(server) do 
    GenServer.cast(server, %{})
  end

  @impl GenServer
  def init(%{request: request, client: client}) do 
    {:ok, %{client: client, request: request, got_response: false}}
  end

  @impl GenServer
  def handle_cast(request, state) do 
    {:noreply, state, {:continue, request}}
  end

  @impl GenServer
  def handle_continue(_request, state) do 
    if !state.got_response do 
      send_request(state.request)
      Process.send_after(self(), :timeout, @timeout)
    end
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:timeout, state) do 
    {:noreply, state, {:continue, state.request}}
  end

  @impl GenServer
  def handle_info(:broadcast_ack, state) do 
    %{body: %{message: message}} = state.request
    send(state.client, {:response, message})
    {:noreply, %{state | got_response: true}}
  end

  defp send_request(request) do
    request
    |> Jason.encode!()
    |> IO.puts()
  end
end
