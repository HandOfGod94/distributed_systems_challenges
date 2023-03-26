local cjson = require("cjson")
local inspect = require("inspect")
local pl_set = require("pl.Set")
local seq = require("pl.seq")
cjson.encode_empty_table_as_object(false)
local node_topology = nil
local node_id = nil
local message_store = pl_set({})
local pending_acks = {}
local acks = {}
local function contains_3f(message, pset)
  return (nil ~= pset[message])
end
local function reply(resp)
  if (nil ~= resp) then
    return print(cjson.encode(resp))
  else
    return nil
  end
end
local function send_request(node, body)
  local _let_2_ = body
  local msg_id = _let_2_["msg_id"]
  local message = _let_2_["message"]
  local pending_ack_req = {}
  pending_ack_req[node] = message
  pending_acks[msg_id] = pending_ack_req
  return reply({src = node_id, dest = node, body = body})
end
local function handle_topology(dest_node, body)
  do end (io.stderr):write("\nProcessing topology")
  local _let_3_ = body
  local msg_id = _let_3_["msg_id"]
  local topology = _let_3_["topology"]
  node_topology = topology
  return {src = node_id, dest = dest_node, body = {msg_id = (msg_id + 1), in_reply_to = msg_id, type = "topology_ok"}}
end
local function handle_init(dest_node, body)
  do end (io.stderr):write(("\nInitialzing node " .. node_id))
  local _let_4_ = body
  local msg_id = _let_4_["msg_id"]
  return {src = node_id, dest = dest_node, body = {msg_id = (msg_id + 1), in_reply_to = msg_id, type = "init_ok"}}
end
local function handle_broadcast(dest_node, body)
  local _let_5_ = body
  local msg_id = _let_5_["msg_id"]
  local message = _let_5_["message"]
  local neighbours = node_topology[node_id]
  if not contains_3f(message, message_store) then
    message_store = (message_store + message)
    local function _6_(_241)
      return (_241 ~= dest_node)
    end
    local function _7_(_241)
      return send_request(_241, body)
    end
    seq.foreach(seq.filter(seq.list(neighbours), _6_), _7_)
    return {src = node_id, dest = dest_node, body = {msg_id = (msg_id + 1), in_reply_to = msg_id, type = "broadcast_ok"}}
  else
    return nil
  end
end
local function handle_broadcast_ok(dest_node, body)
  local _let_9_ = body
  local msg_id = _let_9_["in_reply_to"]
  local message = _let_9_["message"]
  local ackd_msg = {}
  ackd_msg[dest_node] = message
  acks[msg_id] = ackd_msg
  return nil
end
local function handle_read(dest_node, body)
  local _let_10_ = body
  local msg_id = _let_10_["msg_id"]
  return {src = node_id, dest = dest_node, body = {msg_id = (msg_id + 1), in_reply_to = msg_id, messages = pl_set.values(message_store), type = "read_ok"}}
end
local function main()
  while true do
    local input = cjson.decode(io.read("*l"))
    local _let_11_ = input
    local src = _let_11_["src"]
    local body = _let_11_["body"]
    local dest = _let_11_["dest"]
    local _let_12_ = body
    local node_id0 = _let_12_["node_id"]
    local type = _let_12_["type"]
    if (nil == node_id) then
      node_id = node_id0
    else
    end
    local _14_ = type
    if (_14_ == "init") then
      reply(handle_init(src, body))
    elseif (_14_ == "topology") then
      reply(handle_topology(src, body))
    elseif (_14_ == "broadcast") then
      reply(handle_broadcast(src, body))
    elseif (_14_ == "broadcast_ok") then
      handle_broadcast_ok(src, body)
      do end (io.stderr):write(("\nReceived ack for " .. dest .. " from " .. src))
    elseif (_14_ == "read") then
      reply(handle_read(src, body))
    else
    end
  end
  return nil
end
return main()
