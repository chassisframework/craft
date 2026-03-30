defmodule CraftSandboxTest do
  use ExUnit.Case

  setup do
    :ok = Craft.Sandbox.join("sandbox-#{inspect(self())}")
    :ok
  end

  describe "error tuple shape" do
    test "command/3 returns 3-tuple error when group is unknown" do
      assert {:error, :unknown_group, %{}} =
               Craft.Sandbox.command(:anything, :nonexistent_group, [])
    end

    test "async_command/3 returns 3-tuple error when group is unknown" do
      assert {:error, :unknown_group, %{}} =
               Craft.Sandbox.async_command(:anything, :nonexistent_group, [])
    end
  end
end
