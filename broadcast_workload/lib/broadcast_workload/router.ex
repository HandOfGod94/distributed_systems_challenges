defmodule BroadcastWorkload.Router do
  use GenServer

  # client apis
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def dispatch(command) do
    with {:ok, result} <- GenServer.call(__MODULE__, command),
         {:ok, result} <- Jason.encode(result) do
      IO.puts(result)
    else
      {:error, error} -> IO.puts(:stderr, error)
    end
  end

  # server callbacks
  @impl GenServer
  def init(_) do
    {:ok, %{node_id: nil, topology: [], messages: MapSet.new()}}
  end

  @impl GenServer
  def handle_call(%{body: %{type: "init"}} = input, _from, state) do
    %{src: dest, body: %{node_id: node_id, msg_id: msg_id}} = input
    IO.puts(:stderr, "initializing node #{node_id}")

    {:reply,
     {:ok,
      %{
        src: node_id,
        dest: dest,
        body: %{type: "init_ok", in_reply_to: msg_id, msg_id: msg_id + 1}
      }}, %{state | node_id: node_id}}
  end

  @impl GenServer
  def handle_call(%{body: %{type: "topology"}} = input, _from, state) do
    %{src: dest, body: %{topology: topology, msg_id: msg_id}} = input

    {:reply,
     {:ok,
      %{
        src: state.node_id,
        dest: dest,
        body: %{type: "topology_ok", in_reply_to: msg_id, msg_id: msg_id + 1}
      }}, %{state | topology: topology}}
  end

  @impl GenServer
  def handle_call(%{body: %{type: "broadcast"}} = input, _from, state) do
    %{src: dest, body: %{message: message, msg_id: msg_id}} = input

    {:reply,
     {:ok,
      %{
        src: state.node_id,
        dest: dest,
        body: %{type: "broadcast_ok", in_reply_to: msg_id, msg_id: msg_id + 1}
      }}, %{state | messages: MapSet.put(state.messages, message)}}
  end

  @impl GenServer
  def handle_call(%{body: %{type: "read"}} = input, _from, state) do
    %{src: dest, body: %{msg_id: msg_id}} = input

    {:reply,
     {:ok,
      %{
        src: state.node_id,
        dest: dest,
        body: %{
          type: "read_ok",
          messages: MapSet.to_list(state.messages),
          in_reply_to: msg_id,
          msg_id: msg_id + 1
        }
      }}, state}
  end

  def handle_call(unknown_input, _from, state) do
    {:reply,
     {:error,
      "unknown command recieved #{inspect(unknown_input)}. Current state: #{inspect(state)}"},
     state}
  end
end
