%%%-------------------------------------------------------------------
%%  Author:  Ruilin Zhu and  Zinan Wei at Unversity of Florida
%%%-------------------------------------------------------------------

-module(chord_server).
-author("Ruilin Zhu, Zinan Wei").
-behaviour(gen_server).
%% API
-export([start_link/1]).
-export([init/1, terminate/2]).
%%  , handle_cast/2, handle_info/2, code_change/3,terminate/2]).

start_link(NumNodes) ->
  gen_server:start_link({global, ?MODULE}, ?MODULE, [{NumNodes}], []).

init([{NumNodes}]) ->
  Power = 6,
  ChordNodeList = generateChordNodes(NumNodes,Power),
  generate_chord(NumNodes,Power,ChordNodeList),

  form_chord(ChordNodeList,NumNodes-1),
  finish(),

  {ok, {NumNodes}}.

%%  APIs
% 1->2, 2->3, 3->4,.....
form_chord(ChordNodeList,NumNodes)->
  ok.


% finish the close ring
finish() ->
  ok.


generate_chord(0,Power,ChordNodeList) ->
  ok;
generate_chord(N,Power,ChordNodeList) ->
  chord_worker:start_link(lists:nth(N,ChordNodeList),Power),
  generate_actors(N - 1,Power,ChordNodeList).

generateChordNodes(NumNodes,Power)->
  Maximum = math:pow(2,Power),
  Gap = round(Maximum/NumNodes),
  generateList(NumNodes-1,Gap,Gap,Maximum,[1]).

generateList(1,Gap,Count,Maximum,List) ->
  List++[Gap];
generateList(Length,Gap,Count,Maximum,List) ->
  Nextgap = (Gap+Count) rem Maximum,
  generateList(Length-1,Nextgap,Count,Maximum,List++[Gap]).

terminate(_Reason, State) ->
  ok.

get_hash(InputString) ->
  io_lib:format("~32.32.0b", [binary:decode_unsigned(crypto:hash(sha, InputString))]).