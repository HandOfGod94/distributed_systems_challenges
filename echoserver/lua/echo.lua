local cjson = require("cjson")
local inspect = require("inspect")
local tablex = require("pl.tablex")
local function handle_echo(node_id, input)
  local _let_1_ = input
  local src = _let_1_["src"]
  local body = _let_1_["body"]
  local _let_2_ = body
  local msg_id = _let_2_["msg_id"]
  return {src = node_id, dest = src, body = tablex.merge(body, {msg_id = (msg_id + 1), in_reply_to = msg_id, type = "echo_ok"}, true)}
end
local function handle_init(node_id, input)
  local _let_3_ = input
  local src = _let_3_["src"]
  local body = _let_3_["body"]
  local _let_4_ = body
  local msg_id = _let_4_["msg_id"]
  return {src = node_id, dest = src, body = {msg_id = (msg_id + 1), in_reply_to = msg_id, type = "init_ok"}}
end
local function main()
  local node_id = nil
  while true do
    local input = cjson.decode(io.read("*l"))
    local _let_5_ = input
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
      do end (io.stderr):write(("initialized node " .. node_id .. "\n"))
      print(cjson.encode(handle_init(node_id, input)))
    elseif (_8_ == "echo") then
      do end (io.stderr):write(("Echoing body on node " .. node_id .. "\n"))
      print(cjson.encode(handle_echo(node_id, input)))
    else
    end
  end
  return nil
end
return main()
