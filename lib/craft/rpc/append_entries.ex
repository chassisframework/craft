defmodule Craft.RPC.AppendEntries do
  alias Craft.RPC.AppendEntries.LeadershipTransfer
  alias Craft.Consensus.State
  alias Craft.Consensus.State.LeaderState
  alias Craft.Persistence

  defstruct [
    :term,
    :leader_id,
    :prev_log_index,
    :prev_log_term,
    :entries,
    :leader_commit,
    :leadership_transfer
  ]

  defmodule LeadershipTransfer do
    defstruct [
      :latest_index,
      :latest_term,
      :from # {pid, ref}, from Consensus.cast
    ]
  end

  def new(%State{state: :leader} = state, to_node) do
    next_index = Map.get(state.leader_state.next_indices, to_node)
    prev_log_index = next_index - 1
    {:ok, %{term: prev_log_term}} = Persistence.fetch(state.persistence, prev_log_index)
    entries = Persistence.fetch_from(state.persistence, next_index)

    leadership_transfer =
      case state.leader_state.leadership_transfer do
        %LeaderState.LeadershipTransfer{current_candidate: ^to_node} = transfer ->
          %LeadershipTransfer{
            from: transfer.from,
            latest_index: Persistence.latest_index(state.persistence),
            latest_term: Persistence.latest_term(state.persistence)
          }

        _ ->
          nil
      end

    %__MODULE__{
      term: state.current_term,
      leader_id: node(),
      prev_log_index: prev_log_index,
      prev_log_term: prev_log_term,
      entries: entries,
      leader_commit: state.commit_index,
      leadership_transfer: leadership_transfer
    }
  end

  defmodule Results do
    defstruct [
      :term,
      :from,
      :success,
      :latest_index
    ]

    def new(%State{state: :follower} = state, success) do
      %__MODULE__{
        term: state.current_term,
        from: node(),
        success: success,
        latest_index: Persistence.latest_index(state.persistence)
      }
    end
  end

  # defmodule SuccessResult
  # defmodule FailureResult
end
