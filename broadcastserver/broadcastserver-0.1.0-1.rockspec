package = "broadcastserver"
version = "0.1.0-1"
source = {
  url = "..." -- We don't have one yet
}
description = {
  summary = "broadcastserver for maelstorm",
  detailed = [[
    broadcast server written in fennel
   ]],
  homepage = "http://...", -- We don't have one yet
  license = "MIT/X11" -- or whatever you like
}
dependencies = {
  "lua >= 5.1, < 5.5",
  "lua-cjson == 2.1.0.10-1",
  "penlight == 1.13.1-1"
}
build = {
  -- We'll start here.
  type = "builtin",
  install = {
    bin = {
      broadcastserver = "lua/broadcast.lua"
    }
  },
  modules = {
    broadcastserver = "lua/broadcast.lua"
  }
}
