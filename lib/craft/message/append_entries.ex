defmodule Craft.Message.AppendEntries do
  alias Craft.Consensus.State
  alias Craft.Consensus.State.LeaderState
  alias Craft.Consensus.State.LeaderState.CongestionControl
  alias Craft.Persistence
  alias Craft.Message.AppendEntries.LeadershipTransfer

  defstruct [
    :term,
    :leader_id,
    :prev_log_index,
    :prev_log_term,
    :entries,
    :leader_last_applied,
    :leadership_transfer,
    :sent_at,
    :lease_expires_at
  ]

  defmodule LeadershipTransfer do
    defstruct [
      :latest_index,
      :latest_term,
      :from # {pid, ref}, from Consensus.cast
    ]
  end

  #
  # TODO: limit size of `entries` so it can be delivered before the next heartbeat
  # i.e. send only N entries, or limit to 10Mb, etc...
  #
  # see :erlang.external_size/2
  #
  def new(%State{state: :leader} = state, to_node) do
    next_index = Map.get(state.leader_state.next_indices, to_node)
    prev_log_index = next_index - 1
    {:ok, %{term: prev_log_term}} = Persistence.fetch(state.persistence, prev_log_index)
    max_index = min(Persistence.latest_index(state.persistence), next_index + CongestionControl.num_entries(state, to_node))
    index_range = next_index..max_index//1
    entries =
      if Range.size(index_range) > 0 do
        Persistence.fetch_between(state.persistence, index_range)
      else
        []
      end

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
      leader_last_applied: state.last_applied,
      leadership_transfer: leadership_transfer,
      sent_at: state.leader_state.quorum_status.current_round_sent_at,
      lease_expires_at: state.lease_expires_at
    }
  end

  defmodule Results do
    alias Craft.Message.AppendEntries

    defstruct [
      :term,
      :from,
      :success,
      :latest_index,
      :conflict_index,
      :conflict_term,
      :heartbeat_sent_at
    ]

    def new(state, append_entries, success, _entry \\ nil)
    def new(%State{state: :follower} = state, %AppendEntries{} = append_entries, _success = true, _entry) do
      %__MODULE__{
        term: state.current_term,
        from: node(),
        success: true,
        latest_index: Persistence.latest_index(state.persistence),
        heartbeat_sent_at: append_entries.sent_at
      }
    end

    # we don't have an entry at the leader's index, send the latest index
    def new(%State{state: :follower} = state, %AppendEntries{} = append_entries, _success = false, _entry = nil) do
      %__MODULE__{
        term: state.current_term,
        from: node(),
        success: false,
        latest_index: Persistence.latest_index(state.persistence),
        heartbeat_sent_at: append_entries.sent_at
      }
    end

    # we have the index, but it doesn't match the leader's term
    def new(%State{state: :follower} = state, %AppendEntries{} = append_entries, _success = false, entry) do
      %__MODULE__{
        term: state.current_term,
        from: node(),
        success: false,
        conflict_term: entry.term,
        conflict_index: earliest_index_of_term(state.persistence, entry.term, append_entries.prev_log_index),
        heartbeat_sent_at: append_entries.sent_at
      }
    end

    defp earliest_index_of_term(persistence, term, starting_index) do
      prev_index = starting_index - 1
      prev_entry = Persistence.fetch(persistence, prev_index)

      case prev_entry do
        %{term: ^term} ->
          earliest_index_of_term(persistence, term, prev_index)

        _ ->
          starting_index
      end
    end
  end
end
