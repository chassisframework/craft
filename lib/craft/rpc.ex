defmodule Craft.RPC do
  @moduledoc false

  alias Craft.Consensus
  alias Craft.Consensus.State
  alias Craft.Consensus.State.Members
  alias Craft.RPC.AppendEntries
  alias Craft.RPC.InstallSnapshot
  alias Craft.RPC.RequestVote

  require Logger

  def request_vote(state, opts \\ []) do
    request_vote = RequestVote.new(state, opts)

    for to_node <- Members.other_voting_nodes(state.members) do
      send_message(request_vote, to_node, state)
    end
  end

  def respond_vote(%RequestVote{} = request_vote, vote_granted, state) do
    state
    |> RequestVote.Results.new(request_vote.pre_vote, vote_granted)
    |> send_message(request_vote.candidate_id, state)
  end

  def append_entries(%State{} = state, to_node) do
    state
    |> AppendEntries.new(to_node)
    |> send_message(to_node, state)
  end

  def respond_append_entries(%AppendEntries{} = append_entries, success, %State{} = state) do
    state
    |> AppendEntries.Results.new(append_entries, success)
    |> send_message(append_entries.leader_id, state)
  end

  def install_snapshot(%State{} = state, to_node) do
    state
    |> InstallSnapshot.new(to_node)
    |> send_message(to_node, state)
  end

  def respond_install_snapshot(%InstallSnapshot{} = install_snapshot, success, %State{state: :receiving_snapshot} = state) do
    InstallSnapshot.Results.new(install_snapshot, success)
    |> send_message(install_snapshot.leader_id, state)
  end

  if Mix.env() == :test do
    def send_message(message, to_node, state) do
      Craft.Nexus.cast(state.nexus_pid, {Consensus.name(state.name), to_node}, message)
    end
  else
    def send_message(message, to_node, state) do
      :gen_statem.cast({Consensus.name(state.name), to_node}, message)
    end
  end
end
