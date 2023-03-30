defmodule BroadcastWorkload.NodeRegistry do
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def set_current_node(node_id) do
    GenServer.call(__MODULE__, {:init, node_id})
  end

  def set_topology(topology) do
    GenServer.call(__MODULE__, {:topology, topology})
  end

  def neighbours do 
    GenServer.call(__MODULE__, {:neighbours})
  end

  def current_node_id do
    GenServer.call(__MODULE__, {:current_node})
  end

  @impl GenServer
  def init(_opts) do
    {:ok, %{node_id: "", topology: []}}
  end

  @impl GenServer
  def handle_call({:init, node_id}, _from, state) do
    {:reply, :ok, %{state | node_id: node_id}}
  end

  def handle_call({:topology, topology}, _from, state) do
    {:reply, :ok, %{state | topology: topology}}
  end

  def handle_call({:current_node}, _from, state) do
    {:reply, state.node_id, state}
  end

  def handle_call({:neighbours}, _from, state) do 
    {:reply, state.topology, state}
  end
end
