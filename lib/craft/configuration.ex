# manages locally persistent configurations
#
# not super in love with this approach, it doesn't scale very well, but it avoids a centralized db, so it should make backups easier
#
# :erlang.term_to_binary/2 isn't guaranteed to be consistent over OTP releases, but :dets uses it, so i guess it's actually stable?
#
# the `namespace` parameter is only used by Craft.Sandbox
#
defmodule Craft.Configuration do
  @moduledoc false

  def find(name, namespace \\ "") do
    with {dir, file} <- find_file(name, namespace) do
      file
      |> File.read!()
      |> :erlang.binary_to_term()
      |> Map.put(:data_dir, dir)
    end
  end

  def copy_configuration(name, to_directory, namespace \\ "") do
    with {_dir, file} <- find_file(name, namespace) do
      to_file = configuration_file(to_directory)

      File.cp(file, to_file)
    end
  end

  def delete_member_data(name, namespace \\ "") do
    with {dir, _file} <- find_file(name, namespace) do
      data_dir()
      |> Path.join(dir)
      |> File.rm_rf!()
    end
  end

  def restore_from_backup(path, namespace \\ "") do
    config =
      path
      |> configuration_file()
      |> read_file()

    new_path = make_new_directory(config.name, namespace)

    File.cp_r!(path, new_path)
  end

  defp find_file(name, namespace) do
    dirs =
      data_dir()
      |> Path.join(namespace)
      |> File.ls()

    case dirs do
      {:ok, dirs} ->
        Enum.find_value(dirs, fn dir ->
          path = Path.join([data_dir(), namespace, dir])

          if File.dir?(path) do
            config_file = configuration_file(path)

            config = read_file(config_file)

            if config.name == name do
              {Path.join(namespace, dir), config_file}
            end
          end
        end)

      {:error, _} ->
        nil
    end
  end

  def read_file(file) do
    file
    |> File.read!()
    |> :erlang.binary_to_term()
  end

  def configuration_file(dir) do
    Path.join(dir, "config.erlterm")
  end

  # def update!(config, key, value) do
  #   if config = find(name) do
  #     contents =
  #       config
  #       |> Map.delete(:data_dir)
  #       |> Map.put(key, value)
  #       |> :erlang.term_to_binary()

  #     config.data_dir
  #     |> configuration_file()
  #     |> File.write!(contents)

  #   else
  #     raise "attempted to update missing config for group #{inspect name}"
  #   end
  # end

  def write_new!(name, config, namespace \\ "") do
    if find(name, namespace) do
      raise "configuration for group #{inspect name} already exists in namespace #{inspect namespace}"
    end

    contents =
      config
      |> Map.put(:name, name)
      |> :erlang.term_to_binary()

    name
    |> make_new_directory(namespace)
    |> configuration_file()
    |> File.write!(contents)

    find(name, namespace)
  end

  def make_new_directory(name, namespace) do
    hash =
      name
      |> :erlang.phash2()
      |> Integer.to_string()

    path = Path.join([data_dir(), namespace, hash])

    if File.exists?(path) do
      :crypto.strong_rand_bytes(16)
      |> Base.encode16(case: :lower)
      |> make_new_directory(namespace)
    else
      File.mkdir_p!(path)

      path
    end
  end

  def create_data_dir!() do
    if Application.get_env(:craft, :base_data_dir) do
      data_dir =
        Path.join([
          Application.get_env(:craft, :base_data_dir),
          to_string(Mix.env()),
          to_string(node())
        ])

      File.mkdir_p!(data_dir)

      Application.put_env(:craft, :data_dir, data_dir)
    else
      Application.get_env(:craft, :data_dir, "craft_data") |> File.mkdir_p!()
    end
  end

  def data_dir do
    create_data_dir!()

    Application.get_env(:craft, :data_dir)
  end
end
