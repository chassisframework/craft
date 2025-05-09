defmodule Craft.RPC.InstallSnapshot do
  alias Craft.Consensus.State
  alias Craft.Persistence

  defstruct [
    :term,
    :leader_id,
    :log_index,
    :log_entry,
    :snapshot_transfer
  ]

  def new(state, for_node) do
    {log_index, snapshot_transfer} =
      case Map.fetch(state.leader_state.snapshot_transfers, for_node) do
        {:ok, {index, snapshot_transfer}} ->
          {index, snapshot_transfer}

        # log-stored snapshot
        :error ->
          {State.latest_snapshot_index(state), nil}
      end

    {:ok, log_entry} = Persistence.fetch(state.persistence, log_index)

    %__MODULE__{
      term: state.current_term,
      leader_id: node(),
      log_index: log_index,
      log_entry: log_entry,
      snapshot_transfer: snapshot_transfer
    }
  end

  defmodule Results do
    alias Craft.RPC.InstallSnapshot

    defstruct [
      # if the leader is deposed during a snapshot transfer, we still want to hear
      # about the results, so we can delete the snapshot if no other nodes are receiving it
      # so we don't include the term here
      :from,
      :success,
      :latest_index
    ]

    def new(%InstallSnapshot{} = install_snapshot, success) do
      %__MODULE__{
        from: node(),
        success: success,
        latest_index: install_snapshot.log_index
      }
    end
  end
end
