local cjson = require("cjson")
local inspect = require("inspect")
local node_topology = nil
local node_id = nil
local message_store = {nil}
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
  table.concat(message_store, message)
  for node, neighbours in pairs(node_topology) do
    for _, neighbour_node in ipairs(neighbours) do
      send_request(neighbour_node, body)
    end
  end
  return {src = node_id, dest = dest_node, body = {msg_id = (msg_id + 1), in_reply_to = msg_id, type = "broadcast_ok"}}
end
local function handle_read(dest_node, body)
  local _let_4_ = body
  local msg_id = _let_4_["msg_id"]
  return {src = node_id, dest = dest_node, body = {msg_id = (msg_id + 1), in_reply_to = msg_id, messages = message_store, type = "read_ok"}}
end
local function main()
  while true do
    local input = cjson.decode(io.read("*l"))
    local _let_5_ = input
    local src = _let_5_["src"]
    local body = _let_5_["body"]
    local _let_6_ = body
    local node_id0 = _let_6_["node_id"]
    local type = _let_6_["type"]
    if (nil == node_id) then
      node_id = node_id0
    else
    end
    local _8_ = type
    if (_8_ == "init") then
      print(cjson.encode(handle_init(src, body)))
    elseif (_8_ == "topology") then
      print(cjson.encode(handle_topology(src, body)))
    elseif (_8_ == "broadcast") then
      print(cjson.encode(handle_broadcast(src, body)))
    elseif (_8_ == "read") then
      print(cjson.encode(handle_read(src, body)))
    else
    end
  end
  return nil
end
return main()
