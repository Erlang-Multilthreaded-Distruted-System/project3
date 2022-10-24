%%%-------------------------------------------------------------------
%%% @author lynn
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(chord_server).

-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3, get_hash/1]).

-define(SERVER, ?MODULE).

-record(chord_server_state, {}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Num_Nodes, Num_Requests) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [Num_Nodes, Num_Requests], []).

init([Num_Nodes, Num_Requests]) ->
  {ok, [Num_Nodes, Num_Requests]}.

handle_call(_Request, _From, State = #chord_server_state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #chord_server_state{}) ->
  {noreply, State}.

handle_info(_Info, State = #chord_server_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #chord_server_state{}) ->
  ok.

code_change(_OldVsn, State = #chord_server_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

generate_nodes(Num_Nodes, Num_Requests, Node_in_ring) ->
  Id = string:concat("chordnode", integer_to_list(Num_Nodes)),
  Hash = get_hash(Id),
  gen_server:start_link({global, Hash}, [Hash, Num_Requests, Node_in_ring]).



get_hash(InputString) ->
  binary:decode_unsigned(crypto:hash(sha, InputString)).
