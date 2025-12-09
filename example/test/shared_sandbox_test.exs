defmodule SharedSandboxTest do
  use ExUnit.Case

  test "all processes share a global sandbox" do
    :ok = Craft.start_group("abc", [], Craft.SandboxTestMachine)

    assert :ok = Craft.command({:put, "a", "b"}, "abc")
    assert {:ok, "b"} = Craft.query({:get, "a"}, "abc")
    assert {:ok, "b"} = Craft.query({:get_parallel, "a"}, "abc")

    assert {:ok, ref} = Craft.async_command({:put, "a", "b"}, "abc")

    assert_receive {:"$craft_command", ^ref, :ok}
  end
end
