app=broadcast

compile:
	fennel --compile fnl/$(app).fnl > lua/$(app).lua

build: compile
	luarocks build --pin 

deps:
	luarocks build --pin --only-deps
