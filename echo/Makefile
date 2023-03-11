compile:
	fennel --compile fnl/echo.fnl > lua/echo.lua

build: compile
	luarocks build --pin --pack-binary-rock

deps:
	luarocks build --pin --only-deps
