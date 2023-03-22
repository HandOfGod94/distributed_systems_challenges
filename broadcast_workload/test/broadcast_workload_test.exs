defmodule BroadcastWorkloadTest do
  use ExUnit.Case
  doctest BroadcastWorkload

  test "greets the world" do
    assert BroadcastWorkload.hello() == :world
  end
end
