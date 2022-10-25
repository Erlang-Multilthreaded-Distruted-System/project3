-module(chord_supervisor).

-behaviour(supervisor).

-export([start_link/1, init/1]).

start_link(NumNodes) ->
  supervisor:start_link({global, ?MODULE}, ?MODULE, [NumNodes]).

init([NumNodes]) ->
  process_flag(trap_exit, true),
  RestartStrategy = one_for_one,
  MaxRestarts = 3,
  MaxSecondsBetweenRestarts = 5,

  SubFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

  Chord_child =
    {chord_server, {chord_server, start_link, [NumNodes]}, permanent, 2000, worker, [
      chord_server
    ]},

  {ok, {SubFlags, [Chord_child]}}.


