defmodule BroadcastWorkload.Handler do
  use GenServer
  alias BroadcastWorkload.Requester

  # client apis
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def dispatch(command) do
    with {:ok, %{} = result} <- GenServer.call(__MODULE__, command),
         {:ok, result} <- Jason.encode(result) do
      IO.puts(result)
    else
      {:ok, :noop} -> IO.puts(:stderr, "idempotent message")
      {:error, error} -> IO.puts(:stderr, error)
    end
  end

  # server callbacks
  @impl GenServer
  def init(_) do
    {:ok, %{node_id: nil, neighbours: [], messages: MapSet.new(), pending: %{}}}
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
    IO.puts(:stderr, "initializing topology #{inspect(topology)}")

    {:reply,
     {:ok,
      %{
        src: state.node_id,
        dest: dest,
        body: %{type: "topology_ok", in_reply_to: msg_id, msg_id: msg_id + 1}
      }}, %{state | neighbours: topology[String.to_atom(state.node_id)]}}
  end

  @impl GenServer
  def handle_call(%{body: %{type: "broadcast"}} = input, _from, state) do
    %{src: dest, body: %{message: message, msg_id: msg_id} = body} = input

    if MapSet.member?(state.messages, message) || dest == state.node_id do
      {:reply, {:ok, :noop}, state}
    else
      new_messages = MapSet.put(state.messages, message)

      {:reply,
       {:ok,
        %{
          src: state.node_id,
          dest: dest,
          body: %{type: "broadcast_ok", in_reply_to: msg_id, msg_id: msg_id + 1}
        }}, %{state | messages: new_messages}, {:continue, body}}
    end
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

  @impl GenServer
  def handle_call(%{body: %{type: "broadcast_ok"}} = input, _from, state) do
    %{dest: src, body: %{in_reply_to: msg_id}} = input
    if state.pending[msg_id][src], do: send(state.pending[msg_id][src], :broadcast_ack)
    {:reply, {:ok, :noop}, state}
  end

  @impl GenServer
  def handle_call(unknown_input, _from, state) do
    {:reply,
     {:error,
      "unknown command recieved #{inspect(unknown_input)}. Current state: #{inspect(state)}"},
     state}
  end

  @impl GenServer
  def handle_info({:response, message}, state) do
    {:noreply, MapSet.put(state.messages, message)}
  end

  @impl GenServer
  def handle_continue(%{msg_id: msg_id} = body, state) do
    IO.puts(
      :stderr,
      "broadcasting to neighbours #{inspect(state.neighbours)} from #{state.node_id}"
    )

    # pending structure = %{msg_id : %{node_id: pid}}
    requests =
      state.neighbours
      |> Enum.reject(&(&1 == state.node_id))
      |> Enum.map(&%{src: state.node_id, dest: &1, body: body})

    pids =
      for request <- requests, into: [] do
        DynamicSupervisor.start_child(
          BroadcastWorkload.DynamicSupervisor,
          {Requester, %{request: request, client: self()}}
        )
      end

    pending =
      pids
      |> Enum.map(&elem(&1, 1))
      |> Enum.zip(state.neighbours)
      |> Enum.into(%{}, fn {neighbour_node, pid} -> {msg_id, Map.new([{neighbour_node, pid}])} end)

    {:noreply, %{state | pending: pending}, {:continue, {:fire, pids}}}
  end

  @impl GenServer
  def handle_continue({:fire, pids}, state) do
    Enum.each(pids, fn {:ok, pid} -> Requester.fire(pid) end)
    {:noreply, state}
  end
end
