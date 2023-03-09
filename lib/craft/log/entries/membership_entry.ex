defmodule Craft.Log.MembershipEntry do
  alias Craft.Consensus.State

  defstruct [
    :term,
    :members
  ]

  def new(%State{} = state, change \\ nil) do
    %__MODULE__{
      term: state.current_term,
      members: state.members
    }
  end
end
