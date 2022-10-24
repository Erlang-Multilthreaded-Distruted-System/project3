-module(test).
%% API
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, join/2]).

-define(SERVER, ?MODULE).

-record(chord_node_state, {}).

%%  Recieve a node id and register it as server name. Each node has a finger table with m entries
start_link(Node_id, Num_Requests) ->
  M = 10,
  Finger_table = [],
  gen_server:start_link({global, Node_id}, ?MODULE, [Finger_table, Node_id, nil, Node_id, M], []).

init( [Finger_table, Self_id, Predecessor, Successor, M]) ->

  {ok, [Finger_table, Self_id, Predecessor, Successor, M]}.

%%  API
join(Node_id, Node_in_ring) ->
  if Node_in_ring == -1 ->
    ok;
    true ->
      New_successor = find_successor(Node_in_ring, Node_id)
%%      update_successor(Node_id, New_successor)
  end.

%% N == Node_in_ring, find the successor of Id based on N
find_successor(N, Id) ->
  Successor = 14,
  if  Id > N and Id < Successor ->
    Successor;
    true ->
      Finger_table = get_table(N),
      M = get_m(N),
      N_p = closest_preceding_node(M, Id, N, Finger_table),
      find_successor(N_p, id)
  end.
