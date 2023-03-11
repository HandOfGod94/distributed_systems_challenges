local cjson = require("cjson")
local inspect = require("inspect")
local tablex = require("pl.tablex")
local function reply(node_id, input, resp)
  local _let_1_ = input
  local src = _let_1_["src"]
  local body = _let_1_["body"]
  local _let_2_ = body
  local msg_id = _let_2_["msg_id"]
  local reply_body = tablex.merge(body, resp, true)
  do end (reply_body)["msg_id"] = (msg_id + 1)
  do end (reply_body)["in_reply_to"] = msg_id
  input["src"] = node_id
  input["dest"] = src
  input["body"] = reply_body
  return input
end
local function main()
  local node_id = nil
  while true do
    local input = cjson.decode(io.read("*l"))
    local _let_3_ = input
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
      do end (io.stderr):write(("initialized node " .. node_id .. "\n"))
      print(cjson.encode(reply(node_id, input, {type = "init_ok"})))
    elseif (_6_ == "echo") then
      do end (io.stderr):write(("Echoing body on node " .. node_id .. "\n"))
      print(cjson.encode(reply(node_id, input, tablex.merge(body, {type = "echo_ok"}, true))))
    else
    end
  end
  return nil
end
return main()
