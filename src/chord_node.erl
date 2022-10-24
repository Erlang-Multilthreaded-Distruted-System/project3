%%%-------------------------------------------------------------------
%%% @author lynn
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(chord_node).

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, join/2, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(chord_node_state, {}).

%%  Recieve a node id and register it as server name. Each node has a finger table with m entries
start_link(Node_id) ->
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
      New_successor = find_successor(Node_in_ring, Node_id),
      update_successor(Node_id, New_successor)
  end.

%% N == Node_in_ring
find_successor(N, Id) ->
  Successor = get_successor(N),
  if  Id > N and Id =< Successor ->
    Successor;
    true ->
      Finger_table = get_table(N),
      M = get_m(N),
      N_p = closest_preceding_node(M, Id, N, Finger_table),
      find_successor(N_p, id)
  end.

closest_preceding_node(0, Id, N, Finger_table) ->
  N;
closest_preceding_node(M, Id, N, Finger_table) ->
  Finger_i = maps:get(Id + math:pow(2, M - 1), Finger_table),
  if  Finger_i  < Id and Finger_i > N ->
    Finger_i;
        true ->
    closest_preceding_node(M - 1, Id, N, Finger_table)
end.


get_successor(Node_in_ring) ->
  {ok, Node_in_ring_successor_id} = gen_server:call({global, Node_in_ring}, {get_successor}),
  Node_in_ring_successor_id.


get_predecessor(Node_in_ring) ->
  {ok, Node_in_ring_predecessor_id} = gen_server:call({global, Node_in_ring}, {get_predecessor}),
  Node_in_ring_predecessor_id.

get_m(Node_in_ring) ->
  {ok, Node_in_ring_m} = gen_server:call({global, Node_in_ring}, {get_m}),
  Node_in_ring_m.

get_table(Node_in_ring) ->
  {ok, Node_in_ring_table} = gen_server:call({global, Node_in_ring}, {get_table}),
  Node_in_ring_table.

update_successor(Node_id, New_successor) ->
  gen_server:cast({global, Node_id}, {update_successor,New_successor}).

update_predecessor(Node_id, New_predecessor) ->
  gen_server:cast({global, Node_id}, {update_predecessor,New_predecessor}).

update_finger_table(Node_id, New_Finger_table) ->
  gen_server:cast({global, Node_id}, {update_finger_table,New_Finger_table}).




handle_call({get_successor}, _From, {ok, [Finger_table, Self_id, Predecessor, Successor, M]}) ->

  {reply, Successor, [Finger_table, Self_id,Successor, Predecessor, M]};

handle_call({get_predecessor}, _From, {ok, [Finger_table, Self_id, Successor, Predecessor, M]}) ->

  {reply, Predecessor, [Finger_table, Self_id, Successor, Predecessor, M]};

handle_call({get_m}, _From,{ok, [Finger_table, Self_id, Successor, Predecessor, M]}) ->

  {reply, M, [{},{}, Self_id, Successor, Predecessor, M]};

handle_call({get_table},_From, {ok, [Finger_table, Self_id, Successor, Predecessor, M]}) ->

  {reply, Finger_table, [Finger_table, Self_id, Successor, Predecessor, M]}.

handle_cast({update_successor,New_successor}, [Finger_table, Self_id, Successor, Predecessor, M]) ->
  maps:put(Self_id + 1, New_successor, Finger_table),
  {noreply, [Finger_table, Self_id, New_successor, Predecessor, M]};

handle_cast({update_predecessor, New_predecessor}, [Finger_table, Self_id, Successor, Predecessor, M]) ->
  {noreply, [Finger_table, Self_id, Successor, New_predecessor, M]};

handle_cast({update_finger_table, New_Finger_table}, [Finger_table, Self_id, Successor, Predecessor, M]) ->
  {noreply, [New_Finger_table, Self_id, Successor, Predecessor, M]}.






handle_info(_Info, State = #chord_node_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #chord_node_state{}) ->
  ok.

code_change(_OldVsn, State = #chord_node_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
init_finger(Node_id, 0, Finger_table) ->
  Finger_table;
init_finger(Node_id, M, Map) ->
  Key = Node_id + math:pow(2,M - 1),
  maps:put(Key, -1, Map).


stabilization(Node_id) ->
  Successor = get_successor(Node_id),
  Predecessor_of_successor =  get_predecessor(Successor),
  if Predecessor_of_successor > Node_id and Predecessor_of_successor < Successor ->
    update_successor(Node_id, Predecessor_of_successor);
    true ->
    continue
  end,
  notify(Successor, Node_id).


notify(N, Node_p) ->
  Predecessor_of_N =  get_predecessor(N),
  if Predecessor_of_N == nil or ( Node_p > Predecessor_of_N and  Node_p < N) ->
    update_predecessor(N, Node_p);
    true->
      ok
  end.

fix_fingers(Node_id, 0, M, New_Finger_table) ->
  update_finger_table(Node_id, 0, M, New_Finger_table);
fix_fingers(Node_id, Next_minus, M, Finger_table) ->
  Next = M - Next_minus + 1,
  New_Value = find_successor(Node_id, math:pow(2, Next - 1)),
  New_Finger_table =  set_Index_List(Finger_table, Next, New_Value),
  fix_fingers(Node_id, Next_minus - 1, M, New_Finger_table).

set_Index_List(L, Index, New) ->
  New_Value= integer_to_list(New),
  lists:sublist(L,Index - 1) ++ New_Value ++ lists:nthtail(Index,L).