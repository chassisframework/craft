defmodule InheritExplicitSandboxTest do
  use ExUnit.Case

  setup do
    name = {:hi, "i'm", :a, "sandbox", make_ref()}

    on_exit fn ->
      Craft.Sandbox.stop(name)
    end

    [name: name]
  end

  test "inherits parent sandbox", ctx do
    Craft.Sandbox.join(ctx.name)

    assert :ok = Craft.start_group("abc", [], Craft.SandboxTestMachine)

    me = self()
    pid =
      spawn_link(fn ->
        receive do
          :go ->
            assert :ok = Craft.command({:put, "a", "b"}, "abc")
            send(me, :go)
        end
      end)

    send(pid, :go)

    receive do
      :go ->
        assert {:ok, "b"} = Craft.query({:get, "a"}, "abc")
        assert {:ok, "b"} = Craft.query({:get_parallel, "a"}, "abc")
    end
  end

  test "inherits task caller sandbox", ctx do
    Craft.Sandbox.join(ctx.name)

    assert :ok = Craft.start_group("abc", [], Craft.SandboxTestMachine)

    me = self()
    Task.Supervisor.async(Craft.TestTaskSupervisor, fn ->
        assert :ok = Craft.command({:put, "a", "b"}, "abc")
        send(me, :go)
    end)

    receive do
      :go ->
        assert {:ok, "b"} = Craft.query({:get, "a"}, "abc")
        assert {:ok, "b"} = Craft.query({:get_parallel, "a"}, "abc")
    end
  end
end
