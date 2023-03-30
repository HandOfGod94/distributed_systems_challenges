defmodule BroadcastWorkload.MessageRepository do
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def save_message(node_id, msg_id, message) do
    GenServer.call(__MODULE__, {:save_message, node_id, msg_id, message})
  end

  def fetch_messages(node_id) do
    GenServer.call(__MODULE__, {:fetch_messages, node_id})
  end

  def has_message?(node_id, msg_id) do
    GenServer.call(__MODULE__, {:has_message?, node_id, msg_id})
  end

  @impl GenServer
  def init(_opts) do
    {:ok, %{}}
  end

  @impl GenServer
  def handle_call({:save_message, node_id, msg_id, message}, _from, state) do
    if Map.has_key?(state, node_id) do
      {:reply, :ok, Map.put(state, node_id, Map.new([{msg_id, message}]))}
    else
      new_state = put_in(state, [node_id, msg_id], message)
      {:reply, :ok, new_state}
    end
  end

  def handle_call({:fetch_messages, node_id}, _from, state) do
    messages =
      state[node_id]
      |> Enum.map(fn {_, v} -> v end)
      |> Enum.into([])

    {:reply, messages, state}
  end

  def handle_call({:has_message?, node_id, msg_id}, _from, state) do
    result = Map.has_key?(state, node_id) && Map.has_key?(state[node_id], msg_id)
    {:reply, result, state}
  end
end
