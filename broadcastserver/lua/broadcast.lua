local cjson = require("cjson")
local inspect = require("inspect")
local node_topology = nil
local node_id = nil
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
local function main()
  while true do
    local input = cjson.decode(io.read("*l"))
    local _let_3_ = input
    local src = _let_3_["src"]
    local body = _let_3_["body"]
    local _let_4_ = body
    local node_id0 = _let_4_["node_id"]
    local type = _let_4_["type"]
    if (nil == node_id) then
      node_id = node_id0
    else
    end
    local _6_ = type
    if (_6_ == "init") then
      print(cjson.encode(handle_init(src, body)))
    elseif (_6_ == "topology") then
      print(cjson.encode(handle_topology(src, body)))
    else
    end
  end
  return nil
end
return main()
