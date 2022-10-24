%%%-------------------------------------------------------------------
%%% @author lynn
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(chord_node).

-behaviour(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(chord_node_state, {}).

%%  Recieve a node id and register it as server name. Each node has a finger table with m entries
start_link(Node_id, Num_Requests, Node_in_ring) ->
  M = 10,
  Finger_table = init_finger(Node_id, M, #{}),
  List = [],
  gen_server:start_link({global, Node_id}, ?MODULE, [Finger_table, List, Node_id, Num_Requests, nil, Node_in_ring, M], []).


%% fingertable, list, successor, predecessor are stored in the state as states



%% join
init([Finger_table, List, Self_id, Num_Requests, Predecessor, Successor,M]) ->
  New_successor = find_successor(Successor, Self_id),

  {ok, [Finger_table, List, Self_id, Num_Requests, Predecessor, New_successor, M]}.

%%  API

find_successor(N, Id) ->
  Successor = get_successor(N),
  if  Id > N and Id < Successor ->
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

handle_call({get_successor}, _From, {ok, [Finger_table, List, Self_id, Num_Requests, Predecessor, Successor, M]}) ->

  {reply, Successor, [Finger_table, List, Self_id,Num_Requests, Successor, Predecessor, M]};

handle_call({get_predecessor}, _From, {ok, [Finger_table, List, Self_id, Num_Requests, Successor, Predecessor, M]}) ->

  {reply, Predecessor, [Finger_table, List, Self_id, Num_Requests, Successor, Predecessor, M]};

handle_call({get_m}, _From,{ok, [Finger_table, List, Self_id, Num_Requests, Successor, Predecessor, M]}) ->

  {reply, M, [{},{}, Self_id, Num_Requests, Successor, Predecessor, M]};

handle_call({get_table},_From, {ok, [Finger_table, List, Self_id, Num_Requests, Successor, Predecessor, M]}) ->

  {reply, Finger_table, [Finger_table, List, Self_id, Num_Requests, Successor, Predecessor, M]}.

handle_cast({update_successor,New_successor}, [Finger_table, List, Self_id, Num_Requests, Successor, Predecessor, M]) ->
  maps:put(Self_id + 1, New_successor, Finger_table),
  {noreply, [Finger_table, List, Self_id, Num_Requests, New_successor, Predecessor, M]};

handle_cast({update_predecessor, New_predecessor}, [Finger_table, List, Self_id, Num_Requests, Successor, Predecessor, M]) ->
  {noreply, [Finger_table, List, Self_id, Num_Requests, Successor, New_predecessor, M]}.




handle_info(_Info, State = #chord_node_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #chord_node_state{}) ->
  ok.

code_change(_OldVsn, State = #chord_node_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
init_finger(Node_id, 0, Map) ->
Map;
init_finger(Node_id, M, Map) ->
  Key = Node_id + math:pow(2,M - 1),
  maps:put(Key, -1, Map).


stabilization(Node_id, Node_in_ring) ->
  Successor = get_successor(Node_id),
  Predecessor_of_successor =  get_predecessor(Successor),
  if Predecessor_of_successor > Node_id and Predecessor_of_successor < Successor ->
    update_successor(Node_id, Predecessor_of_successor);
    true ->
    continue
  end,
  notify(Node_id, Node_in_ring).


notify(Node_id, Node_in_ring) ->
  Predecessor = get_predecessor(Node_id),
  if Predecessor == nil or (Node_in_ring > Predecessor and Node_in_ring < Node_id) ->
    update_predecessor(Node_id, Node_in_ring).

