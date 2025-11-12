{:ok, _pid} = Task.Supervisor.start_link(name: Craft.TestTaskSupervisor)
ExUnit.start()
