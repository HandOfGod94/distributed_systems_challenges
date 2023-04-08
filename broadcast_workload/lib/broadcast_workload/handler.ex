defmodule BroadcastWorkload.Handler do
  alias BroadcastWorkload.{NodeRegistry, MessageRepository}

  @timeout 1000

  def handle_init(request) do
    %{src: dest, body: %{node_id: node_id, msg_id: msg_id}} = request
    IO.puts(:stderr, "initializing node #{node_id}")

    NodeRegistry.set_current_node(node_id)

    {:ok,
     %{
       src: node_id,
       dest: dest,
       body: %{type: "init_ok", in_reply_to: msg_id, msg_id: msg_id + 1}
     }}
  end

  def handle_topology(request) do
    %{src: dest, body: %{topology: topology, msg_id: msg_id}} = request
    IO.puts(:stderr, "initializing topology #{inspect(topology)}")

    current_node = NodeRegistry.current_node_id()
    NodeRegistry.set_topology(topology[String.to_atom(current_node)])

    {:ok,
     %{
       src: current_node,
       dest: dest,
       body: %{type: "topology_ok", in_reply_to: msg_id, msg_id: msg_id + 1}
     }}
  end

  def handle_broadcast(request) do
    %{src: dest, body: %{message: message, msg_id: msg_id} = body} = request

    current_node = NodeRegistry.current_node_id()
    neighbours = NodeRegistry.neighbours()

    unless MessageRepository.message_present?(current_node, message) do
      MessageRepository.save_message(current_node, message)

      neighbours
      |> Enum.reject(fn node -> node == current_node end)
      |> Enum.each(fn node ->
        spawn_link(fn -> broadcast_message(node, body) end)
      end)

      {:ok,
       %{
         src: current_node,
         dest: dest,
         body: %{type: "broadcast_ok", in_reply_to: msg_id, msg_id: msg_id + 1}
       }}
    else
      {:ok, :noop}
    end
  end

  def handle_broadcast_ok(request) do
    %{dest: from_node, body: %{in_reply_to: msg_id}} = request
    MessageRepository.save_replies(from_node, msg_id)

    {:ok, :noop}
  end

  def handle_read(request) do
    %{src: dest, body: %{msg_id: msg_id}} = request

    current_node = NodeRegistry.current_node_id()
    messages = MessageRepository.fetch_messages(current_node)

    {:ok,
     %{
       src: current_node,
       dest: dest,
       body: %{
         type: "read_ok",
         messages: messages,
         in_reply_to: msg_id,
         msg_id: msg_id + 1
       }
     }}
  end

  defp broadcast_message(node_id, %{msg_id: msg_id} = body) do
    unless MessageRepository.reply_received?(node_id, msg_id) do
      send_broadcast_request(node_id, body)
      Process.sleep(@timeout)
      broadcast_message(node_id, body)
    end
  end

  defp send_broadcast_request(node_id, body) do
    %{
      src: NodeRegistry.current_node_id(),
      dest: node_id,
      body: %{
        type: "broadcast",
        message: body.message,
        msg_id: body.msg_id
      }
    }
    |> Jason.encode!()
    |> IO.puts()
  end
end
