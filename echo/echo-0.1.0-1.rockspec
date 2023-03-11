package = "echo"
version = "0.1.0-1"
source = {
  url = "..." -- We don't have one yet
}
description = {
  summary = "echo server for malestorm",
  detailed = [[
    echo server written in fennel
   ]],
  homepage = "http://...", -- We don't have one yet
  license = "MIT/X11" -- or whatever you like
}
dependencies = {
  "lua >= 5.1, < 5.5",
  "lua-cjson = 2.1.0.10-1",
  "penlight = 1.13.1-1"
}
build = {
  -- We'll start here.
  type = "builtin",
  modules = {
    echo = "lua/echo.lua"
  }
}
