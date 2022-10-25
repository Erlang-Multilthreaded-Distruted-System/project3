%%%-------------------------------------------------------------------
%%  Author:  Ruilin Zhu and  Zinan Wei at Unversity of Florida
%%%-------------------------------------------------------------------

-module(chord_server).
-author("Ruilin Zhu, Zinan Wei").
-behaviour(gen_server).
%% API
-export([start_link/1]).
-export([init/1,handle_call/3, handle_cast/2, handle_info/2, code_change/3,terminate/2]).
%%  , handle_cast/2, handle_info/2, code_change/3,terminate/2]).

start_link(NumNodes) ->
  gen_server:start_link({global, ?MODULE}, ?MODULE, [{NumNodes}], []).

init([{NumNodes}]) ->
  Power = trunc(math:log2(NumNodes))+1,
  ChordNodeList = generateChordNodes(NumNodes,Power),

  start_chord(NumNodes,Power,ChordNodeList),

  form_chord(Power,ChordNodeList,NumNodes,1),

  {ok, {NumNodes}}.

%%  APIs
% 1->2, 2->3, 3->4, 4->5, 5->1.....
form_chord(Power,ChordNodeList,1,ACC)->
  % close the ring
  First = lists:nth(1,ChordNodeList),
  Last = lists:nth(ACC,ChordNodeList),
  chord_worker:setPre(First,Last),
  chord_worker:setSuc(Last,First),

  chord_worker:stabilize(First),
  chord_worker:stabilize(Last),
  chord_worker:fix_fingers(First,[],Power),
  chord_worker:fix_fingers(Last,[],Power);

form_chord(Power,ChordNodeList,NumNodes,ACC)->
  Me = lists:nth(ACC,ChordNodeList),
  Target = lists:nth(ACC+1,ChordNodeList),
  chord_worker:join(Me,Target),

  chord_worker:stabilize(Me),
  chord_worker:stabilize(Target),
  chord_worker:fix_fingers(Me,[],Power),
  chord_worker:fix_fingers(Target,[],Power),

  form_chord(Power,ChordNodeList,NumNodes-1,ACC+1).


start_chord(0,Power,ChordNodeList) ->
  ok;
start_chord(N,Power,ChordNodeList) ->
  chord_worker:start_link(lists:nth(N,ChordNodeList),Power),
  start_chord(N-1,Power,ChordNodeList).

generateChordNodes(NumNodes,Power)->
  Maximum = math:pow(2,Power),
  Gap = round(Maximum/NumNodes),
  lists:sort(generateList([],Gap,NumNodes)).

generateList(ResultList,Gap,1) ->
  [get_hash(integer_to_list(1))]++ResultList;
generateList(ResultList,Gap,NumNodes) ->
  SHA1Value = get_hash(integer_to_list(NumNodes+Gap)),
  generateList([SHA1Value]++ResultList,Gap,NumNodes-1) .

get_hash(InputString) ->
  binary:decode_unsigned(crypto:hash(sha, InputString)).

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.