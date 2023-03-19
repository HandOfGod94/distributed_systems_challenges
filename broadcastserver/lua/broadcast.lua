local cjson = require("cjson")
local inspect = require("inspect")
local pl_set = require("pl.Set")
cjson.encode_empty_table_as_object(false)
local node_topology = nil
local node_id = nil
local message_store = pl_set({})
local function send_request(node, body)
  return print(cjson.encode({src = node_id, dest = node, body = body}))
end
local function handle_topology(dest_node, body)
  do end (io.stderr):write("\nProcessing topology")
  local _let_1_ = body
  local msg_id = _let_1_["msg_id"]
  local topology = _let_1_["topology"]
  node_topology = topology
  return {src = node_id, dest = dest_node, body = {msg_id = (msg_id + 1), in_reply_to = msg_id, type = "topology_ok"}}
end
local function handle_init(dest_node, body)
  do end (io.stderr):write(("\nInitialzing node " .. node_id))
  local _let_2_ = body
  local msg_id = _let_2_["msg_id"]
  return {src = node_id, dest = dest_node, body = {msg_id = (msg_id + 1), in_reply_to = msg_id, type = "init_ok"}}
end
local function handle_broadcast(dest_node, body)
  do end (io.stderr):write(("\n Initaiting broacast from " .. node_id))
  local _let_3_ = body
  local msg_id = _let_3_["msg_id"]
  local message = _let_3_["message"]
  local neighbours = node_topology[node_id]
  message_store = (message_store + message)
  for _, neighbour_node in ipairs(neighbours) do
    send_request(neighbour_node, body)
  end
  return {src = node_id, dest = dest_node, body = {msg_id = (msg_id + 1), in_reply_to = msg_id, type = "broadcast_ok"}}
end
local function handle_read(dest_node, body)
  local _let_4_ = body
  local msg_id = _let_4_["msg_id"]
  local messages
  if (nil == message_store) then
    messages = {}
  else
    messages = pl_set.values(message_store)
  end
  return {src = node_id, dest = dest_node, body = {msg_id = (msg_id + 1), in_reply_to = msg_id, messages = messages, type = "read_ok"}}
end
local function main()
  while true do
    local input = cjson.decode(io.read("*l"))
    local _let_6_ = input
    local src = _let_6_["src"]
    local body = _let_6_["body"]
    local _let_7_ = body
    local node_id0 = _let_7_["node_id"]
    local type = _let_7_["type"]
    if (nil == node_id) then
      node_id = node_id0
    else
    end
    local _9_ = type
    if (_9_ == "init") then
      print(cjson.encode(handle_init(src, body)))
    elseif (_9_ == "topology") then
      print(cjson.encode(handle_topology(src, body)))
    elseif (_9_ == "broadcast") then
      print(cjson.encode(handle_broadcast(src, body)))
    elseif (_9_ == "read") then
      print(cjson.encode(handle_read(src, body)))
    else
    end
  end
  return nil
end
return main()
