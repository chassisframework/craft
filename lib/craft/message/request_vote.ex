defmodule Craft.Message.RequestVote do
  alias Craft.Consensus.State

  defstruct [
    :term,
    :candidate_id,
    :last_log_index,
    :last_log_term,
    :pre_vote,
    :leadership_transfer # section 4.2.3
  ]

  def new(%State{} = state, opts \\ []) do
    pre_vote = Keyword.get(opts, :pre_vote, false)
    leadership_transfer = Keyword.get(opts, :leadership_transfer, false)

    %__MODULE__{
      term: state.current_term,
      candidate_id: node(),
      last_log_index: Craft.Persistence.latest_index(state.persistence),
      last_log_term: Craft.Persistence.latest_term(state.persistence),
      pre_vote: pre_vote,
      leadership_transfer: leadership_transfer
    }
  end

  defmodule Results do
    defstruct [
      :term,
      :from,
      :vote_granted,
      :pre_vote,
      :lease_expires_at
    ]

    def new(%State{} = state, pre_vote, vote_granted) do
      %__MODULE__{
        term: state.current_term,
        from: node(),
        vote_granted: vote_granted,
        pre_vote: pre_vote,
        lease_expires_at: state.lease_expires_at
      }
    end
  end
end
