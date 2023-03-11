compile:
	fennel --compile fnl/echo.fnl > lua/echo.lua

build: compile
	luarocks build --pin 

deps:
	luarocks build --pin --only-deps
