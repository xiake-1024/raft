%%%-------------------------------------------------------------------
%%% @author xiake
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% raft server
%%% @end
%%%-------------------------------------------------------------------
-module(raft_server).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

%%raft state
-define(FOLLOWER,1).
-define(CANDIDATE,2).
-define(LEADER,3).

-record(raft_server_state, {
  state, %%节点状态
  term,  %%当前任期
  ref, %%检测ref定时器
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([Interval]) ->
  true=net_kernel:monitor_nodes(true),
  Interval=erlang:start_timer(Interval,self(),interval),
  {ok, #raft_server_state{state=?FOLLOWER,ref=Interval}}.

handle_call(_Request, _From, State = #raft_server_state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #raft_server_state{}) ->
  {noreply, State}.

handle_info({timeout,_Ref,interval}, State = #raft_server_state{}) ->

  {noreply, State}.

terminate(_Reason, _State = #raft_server_state{}) ->
  ok.

code_change(_OldVsn, State = #raft_server_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
