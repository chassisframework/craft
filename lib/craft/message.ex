defmodule Craft.Message do
  @moduledoc false

  alias Craft.Consensus.State
  alias Craft.Consensus.State.Members
  alias Craft.HeartbeatSender
  alias Craft.Message.AppendEntries
  alias Craft.Message.InstallSnapshot
  alias Craft.Message.RequestVote

  import Craft.Tracing, only: [telemetry: 3]

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
    case AppendEntries.new(state, to_node) do
      %AppendEntries{entries: [], leadership_transfer: nil} = msg ->
        # message_sent_telemetry(message, to_node)
        # HeartbeatSender.enqueue(msg, to_node, state.name)
        send_message(msg, to_node, state)
        
        msg ->
          send_message(msg, to_node, state)
    end
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

  defp message_sent_telemetry(%AppendEntries{} = append_entries, to_node) do
    telemetry([:craft, :message, :sent, :append_entries], %{num_entries: Enum.count(append_entries.entries)}, %{to_node: to_node})
  end

  defp message_sent_telemetry(%AppendEntries.Results{}, to_node) do
    telemetry([:craft, :message, :sent, :append_entries_results], %{}, %{to_node: to_node})
  end

  defp message_sent_telemetry(%RequestVote{} = request_vote, to_node) do
    telemetry([:craft, :message, :sent, :request_vote], %{}, %{to_node: to_node, pre_vote: request_vote.pre_vote})
  end

  defp message_sent_telemetry(%RequestVote.Results{} = results, to_node) do
    telemetry([:craft, :message, :sent, :request_vote_results], %{}, %{to_node: to_node, vote_granted: results.vote_granted})
  end

  defp message_sent_telemetry(%InstallSnapshot{}, to_node) do
    telemetry([:craft, :message, :sent, :install_snapshot], %{}, %{to_node: to_node})
  end

  defp message_sent_telemetry(%InstallSnapshot.Results{}, to_node) do
    telemetry([:craft, :message, :sent, :install_snapshot_results], %{}, %{to_node: to_node})
  end

  if Mix.env() == :test do
    require Logger

    def send_message(message, to_node, state) do
      import Craft.Tracing, only: [logger_metadata: 2]

      message_sent_telemetry(message, to_node)

      # the nexus is listening to the logger, when it sees a `:sent_msg` trace, it does the actual send itself
      Logger.debug("sent #{inspect message.__struct__}", logger_metadata(state, trace: {:sent_msg, to_node, node(), message}))
    end
  else
    def send_message(message, to_node, state) do
      message_sent_telemetry(message, to_node)

      Craft.Consensus.remote_operation(state.name, to_node, :cast, message)
    end
  end
end
