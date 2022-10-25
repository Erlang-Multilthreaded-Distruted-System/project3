%%%-------------------------------------------------------------------
%%% @author lynn
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Oct 2022 4:06 PM
%%%-------------------------------------------------------------------
-module(chord_worker).
-author("Ruilin Zhu, Zihan Wei").
-behaviour(gen_server).


%% API
-export([start_link/2, init/1, handle_call/3, handle_cast/2, handle_info/2,join/2,stabilize/1,notify/2,fix_fingers/3,setPre/2,setSuc/2]).

start_link(Index,Power) ->
  Successor = Index,
  gen_server:start_link({global, string:concat("chord", integer_to_list(Index))}, ?MODULE, [{Index,Power,-1, Successor,[],[]}], []).

init([{SelfIndex,Power,Predecessor,Successor,FingerTable,DatList}]) ->
  io:format("chord worker number ~w starts.... ~n", [SelfIndex]),
  {ok, {SelfIndex,Power,Predecessor,Successor,FingerTable,DatList}}.

join(MyIndex,TargetIndex) ->
  gen_server:cast({global, string:concat("chord", integer_to_list(MyIndex))}, {join,TargetIndex}).

find_successor(TargetIndex,ID) ->
  gen_server:call({global, string:concat("chord", integer_to_list(TargetIndex))}, {find_successor,ID}).

stabilize(SelfIndex) ->
  gen_server:cast({global, string:concat("chord", integer_to_list(SelfIndex))}, {stabilize}).

notify(NewSuccessor,NewPredecessor) ->
  gen_server:cast({global, string:concat("chord", integer_to_list(NewSuccessor))}, {notify,NewPredecessor}).

fix_fingers(MyIndex,FT,0)->
  gen_server:call({global, string:concat("chord", integer_to_list(MyIndex))}, {fix_fingers,FT});
fix_fingers(MyIndex,FT,Power)->
  Target = MyIndex + round(math:pow(2,Power-1)),
  TargetIndex = binary:decode_unsigned(crypto:hash(sha, Target)),
  Finger = find_successor(MyIndex,TargetIndex),
  fix_fingers(MyIndex,[Finger]++FT,Power-1).

handle_call({find_successor,ID}, _From, {SelfIndex,Power,Predecessor,Successor,FingerTable,DatList}) ->
  % Initial State, only 1 node in the chord
  if
    % Chord Initializing state
    SelfIndex == Successor ->
      Result = SelfIndex;
    % Normal operation
    SelfIndex < ID and ID < Successor ->
      Result = Successor;
    % Last node in the chord
    Successor < SelfIndex and SelfIndex < ID  ->
      Result = Successor;
    true ->
      % Lookup the finger table
      Len = length(FingerTable),
      if
        % if finger table doesn't exits, ask it successor to query
        Len == 0 ->
          Result = find_successor(Successor,ID);
        % else query the finger table
        true ->
          TargetNode = closest_preceding_node(FingerTable,SelfIndex,ID,Power),
          if
            TargetNode == SelfIndex ->
              Result = SelfIndex;
            true ->
              Result = find_successor(TargetNode,ID)
          end
      end
  end,
  {reply, Result, {SelfIndex,Power,Predecessor,Successor,FingerTable,DatList}};

handle_call({getSuccessorPre}, _From, {SelfIndex,Power,Predecessor,Successor,FingerTable,DatList}) ->
  {reply, Predecessor, {SelfIndex,Power,Predecessor,Successor,FingerTable,DatList}};

handle_call({fix_fingers,FT}, _From, {SelfIndex,Power,Predecessor,Successor,FingerTable,DatList}) ->
  {noreply, {SelfIndex,Power,Predecessor,Successor,FT,DatList}}.

setPre(TargetIndex,MyIndex)->
  gen_server:cast({global, string:concat("chord", integer_to_list(TargetIndex))}, {setPre,MyIndex}).

setSuc(TargetIndex,MyIndex)->
  gen_server:cast({global, string:concat("chord", integer_to_list(TargetIndex))}, {setSuc,MyIndex}).

getSuccessorPre(SuccessorIndex)->
  gen_server:call({global, string:concat("chord", integer_to_list(SuccessorIndex))}, {getSuccessorPre}).

handle_cast({join,TargetIndex}, {SelfIndex,Power,Predecessor,Successor,FingerTable,DatList}) ->
  SuccessorIndex = find_successor(TargetIndex,SelfIndex),
  setPre(SuccessorIndex,SelfIndex),
  {noreply, {SelfIndex,Power,-1,SuccessorIndex,FingerTable,DatList}};

handle_cast({setPre,PredecessorIndex}, {SelfIndex,Power,Predecessor,Successor,FingerTable,DatList}) ->
  {noreply, {SelfIndex,Power,PredecessorIndex,Successor,FingerTable,DatList}};

handle_cast({setSuc,SuccessorIndex}, {SelfIndex,Power,Predecessor,Successor,FingerTable,DatList}) ->
  {noreply, {SelfIndex,Power,Predecessor,SuccessorIndex,FingerTable,DatList}};

handle_cast({stabilize}, {SelfIndex,Power,Predecessor,Successor,FingerTable,DatList}) ->
    X = getSuccessorPre(Successor),
  if
    SelfIndex < X and X < Successor ->
      notify(X,SelfIndex),
      {noreply, {SelfIndex,Power,Predecessor,X,FingerTable,DatList}};
    true ->
      {noreply, {SelfIndex,Power,Predecessor,Successor,FingerTable,DatList}}
  end;

handle_cast({notify,NewPredecessor}, {SelfIndex,Power,Predecessor,Successor,FingerTable,DatList}) ->
  if
    Predecessor == -1 ->
      {noreply, {SelfIndex,Power,NewPredecessor,Successor,FingerTable,DatList}};
    Predecessor < NewPredecessor and NewPredecessor < SelfIndex ->
      {noreply, {SelfIndex,Power,NewPredecessor,Successor,FingerTable,DatList}};
    true ->
      {noreply, {SelfIndex,Power,Predecessor,Successor,FingerTable,DatList}}
  end.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

closest_preceding_node(FingerTable,SelfIndex,ID,0)->
  SelfIndex;
closest_preceding_node(FingerTable,SelfIndex,ID,Power)->
  Finger = lists:nth(Power, FingerTable),
  if
    SelfIndex < Finger and Finger < ID ->
      Finger;
    true ->
      closest_preceding_node(FingerTable,SelfIndex,ID,Power-1)
  end.

