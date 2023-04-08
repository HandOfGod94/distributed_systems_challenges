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

  def message_present?(node_id, msg_id) do
    GenServer.call(__MODULE__, {:message_present?, node_id, msg_id})
  end

  def save_replies(from_node, msg_id) do
    GenServer.call(__MODULE__, {:save_replies, from_node, msg_id})
  end

  def reply_received?(from_node, msg_id) do
    GenServer.call(__MODULE__, {:reply_received?, from_node, msg_id})
  end

  @impl GenServer
  def init(_opts) do
    {:ok, %{messages_sent: %{}, messages_received: %{}}}
  end

  @impl GenServer
  def handle_call({:save_message, node_id, msg_id, message}, _from, state) do
    messages_sent = state.messages_sent

    messages_sent =
      Map.update(
        messages_sent,
        node_id,
        Map.new([{msg_id, message}]),
        &Map.put_new(&1, msg_id, message)
      )

    {:reply, :ok, %{state | messages_sent: messages_sent}}
  end

  def handle_call({:fetch_messages, node_id}, _from, state) do
    messages =
      state.messages_sent
      |> Map.get(node_id, %{})
      |> Enum.map(fn {_, v} -> v end)
      |> Enum.into([])

    {:reply, messages, state}
  end

  def handle_call({:message_present?, node_id, msg_id}, _from, state) do
    messages_sent = state.messages_sent
    result = Map.has_key?(messages_sent, node_id) && Map.has_key?(messages_sent[node_id], msg_id)
    {:reply, result, state}
  end

  def handle_call({:save_replies, from_node, msg_id}, _from, state) do
    messages_received = state.messages_received

    messages_received =
      Map.update(messages_received, from_node, MapSet.new([msg_id]), &MapSet.put(&1, msg_id))

    {:reply, :ok, %{state | messages_received: messages_received}}
  end

  def handle_call({:reply_received?, from_node, msg_id}, _from, state) do
    messages_received = state.messages_received

    result =
      Map.has_key?(messages_received, from_node) &&
        MapSet.member?(messages_received[from_node], msg_id)

    {:reply, result, state}
  end
end
