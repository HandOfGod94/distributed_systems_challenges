defmodule BroadcastWorkload.Controller do
  alias BroadcastWorkload.{NodeRegistry, MessageRepository}

  @timeout 1000

  def handle_init(request) do
    %{src: dest, body: %{node_id: node_id, msg_id: msg_id}} = request
    IO.puts(:stderr, "initializing node #{node_id}")

    NodeRegistry.set_current_node(node_id)
    %{src: node_id, dest: dest, body: %{type: "init_ok", in_reply_to: msg_id, msg_id: msg_id + 1}}
  end

  def handle_topology(request) do
    %{src: dest, body: %{topology: topology, msg_id: msg_id}} = request
    IO.puts(:stderr, "initializing topology #{inspect(topology)}")

    current_node = NodeRegistry.current_node_id()
    NodeRegistry.set_topology(topology[String.to_atom(current_node)])

    %{
      src: current_node,
      dest: dest,
      body: %{type: "topology_ok", in_reply_to: msg_id, msg_id: msg_id + 1}
    }
  end

  def handle_broadcast(request) do
    %{src: dest, body: %{message: message, msg_id: msg_id} = body} = request

    current_node = NodeRegistry.current_node_id()
    neighbours = NodeRegistry.neighbours()

    unless MessageRepository.has_message?(current_node, msg_id) do
      for neighbour <- neighbours, neighbour != current_node do
        # TODO: implment callrpc using elixir Task
        callrpc(neighbour, :boradcast, body, on_response: fn resp -> nil end, timeout: @timeout)
      end

      %{
        src: current_node,
        dest: dest,
        body: %{type: "broadcast_ok", message: message, in_reply_to: msg_id, msg_id: msg_id + 1}
      }
    end
  end

  def handle_read(request) do
    %{src: dest, body: %{msg_id: msg_id}} = request

    current_node = NodeRegistry.current_node_id()
    messages = MessageRepository.fetch_messages(current_node)

    %{
      src: current_node,
      dest: dest,
      body: %{
        type: "read_ok",
        messages: messages,
        in_reply_to: msg_id,
        msg_id: msg_id + 1
      }
    }
  end
end
