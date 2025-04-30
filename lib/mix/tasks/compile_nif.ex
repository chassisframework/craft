defmodule Mix.Tasks.Compile.Nif do
  use Mix.Task

  @shortdoc "Compiles the native C NIF if needed"
  @recursive true

  @nif_c "c_src/clock_wrapper.c"
  @nif_so "priv/clock_wrapper.so"

  @impl Mix.Task
  def run(_args) do
    if up_to_date?(@nif_c, @nif_so) do
      Mix.shell().info("NIF is up to date, skipping compilation.")
      :noop
    else
      Mix.shell().info("Compiling NIF...")
      compile_nif()
      :ok
    end
  end

  defp up_to_date?(src, target) do
    File.exists?(target) and
      File.stat!(target).mtime >= File.stat!(src).mtime
  rescue
    _ -> false
  end

  defp compile_nif do
    {:ok, include_path} = get_erts_include_path()

    File.mkdir_p!("priv")

    cmd = """
    clang -fPIC -shared -dynamiclib -undefined dynamic_lookup -I #{include_path} -o #{@nif_so} #{@nif_c}
    """

    {result, exit_code} = System.cmd("sh", ["-c", cmd], stderr_to_stdout: true)

    if exit_code != 0 do
      Mix.raise("NIF compilation failed:\n#{result}")
    end

    Mix.shell().info("NIF compiled successfully.")
  end

  defp get_erts_include_path do
    erl_cmd = """
    erl -noshell -eval 'io:format("~s", [code:root_dir() ++ "/usr/include"]), halt().'
    """

    case System.cmd("sh", ["-c", erl_cmd]) do
      {path, 0} -> {:ok, String.trim(path)}
      {_, _} -> {:error, "Failed to get ERTS include path"}
    end
  end
end
