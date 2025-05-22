defmodule Craft.Linearizability do
  @moduledoc """
  Determines if/how a list of operations is linearizable, via the Wing/Gong/Lowe algorithm.

  operations should take the form:
  ```
    %{
      id: unique_term,
      client: term,
      called_at: number,
      received_at: number,
      request: {:write, term} | {:read, term}
      response: term
    }
  ```

  timeout errors may have been applied to the system, but other "caught" errors are assumed to not have changed the system state.
  therefore, operations with responses of `{:error, :timeout}` will be considered, but other `{:error, _}` will simply be ignored.
  """
  defmodule Operation do
    defstruct [
      :id,
      :client,
      :called_at,
      :received_at,
      :request,
      :response
    ]
  end

  defmodule TestModel do
    @callback init() :: {:ok, state :: any()}
    @callback write(command :: any(), state :: any()) :: state :: any()
    @callback read(query :: any(), state :: any()) :: result :: any()
  end

  def run do
    File.read!("history")
    |> :erlang.binary_to_term()
    |> linearize(Craft.SimpleMachine)
  end

  def cross_check(linearized_operations, model) do
    {:ok, model_state} = model.init()

    result =
      Enum.reduce_while(linearized_operations, model_state, fn op, model_state ->
        {model_response, new_model_state} =
          case op.request do
            {:write, command} ->
              model.write(command, model_state)

            {:read, query} ->
              {model.read(query, model_state), model_state}
          end

        if model_response == op.response do
          {:cont, new_model_state}
        else
          {:halt, false}
        end
      end)

    !!result
  end

  def linearize([], _model), do: true
  def linearize(history, model) do
    {:ok, model_state} = model.init()

    {history, ignored_ops} =
      history
      |> Enum.split_with(fn
        %{response: {:error, reason}} when reason != :timeout ->
          false

        _ ->
          true
      end)

    result =
      history
      |> Enum.group_by(& &1.client)
      |> Map.new(fn {client, ops} ->
        ops = Enum.sort_by(ops, & &1.called_at)
        {client, ops}
      end)
      |> do_linearize({model, model_state})

    case result do
      {:ok, linearized_history} ->
        {:ok, Enum.reverse(linearized_history), ignored_ops}

      _ ->
        :error
    end
  end

  defp do_linearize(histories_by_client, {model, model_state}, linearized_history \\ [], ops_seen \\ MapSet.new(), cache \\ MapSet.new()) do
    no_ops_left? = Enum.all?(histories_by_client, fn {_client, history} -> Enum.empty?(history) end)

    if no_ops_left? do
      {:ok, linearized_history}
    else
      first_ops =
        Enum.flat_map(histories_by_client, fn
          {_client, []} -> []
          {_client, [op | _]} -> [op]
        end)

      first_return = Enum.min_by(first_ops, & &1.received_at).received_at

      minimal_ops = Enum.reject(first_ops, & &1.called_at > first_return)

      Enum.reduce_while(minimal_ops, cache, fn op, cache ->
        {model_response, new_model_state} =
          case op.request do
            {:write, command} ->
              model.write(command, model_state)

            {:read, query} ->
              {model.read(query, model_state), model_state}
          end

        # if the operation timed out, consider the possiblity that it occurred but the client just didn't get a response
        {response_ok?, model_states_to_consider} =
          case op.response do
            {:error, :timeout} ->
              {true, Enum.dedup([model_state, new_model_state])}

            _ ->
              {model_response == op.response, [new_model_state]}
          end

        if response_ok? do
          Enum.reduce_while(model_states_to_consider, {:cont, cache}, fn model_state, {:cont, cache} ->
            history_without_op = tl(histories_by_client[op.client])
            histories_by_client_without_op = Map.put(histories_by_client, op.client, history_without_op)
            ops_seen = MapSet.put(ops_seen, op)
            cache_key = {ops_seen, model_state}

            if MapSet.member?(cache, cache_key) do
              {:cont, {:cont, cache}}
            else
              case do_linearize(histories_by_client_without_op, {model, model_state}, [op | linearized_history], ops_seen, cache) do
                {:ok, linearized_history} ->
                  {:halt, {:halt, {:ok, linearized_history}}}

                cache ->
                  {:cont, {:cont, MapSet.put(cache, cache_key)}}
              end
            end
          end)
        else
          {:cont, cache}
        end
      end)
    end
  end
end
