%%%-------------------------------------------------------------------
%%% @author xiake
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% raft server
%%% @end
%%%-------------------------------------------------------------------
%%https://blog.csdn.net/lifugui001/article/details/81073084
-module(raft_server).

-behaviour(gen_server).

%%EXPORTS
-export([get/1]).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(HIBERNATE_TIMEOUT, 3000).

-define(SERVER, ?MODULE).

-define(RAFT_NODES, 'raft:nodes').

%%raft state
-define(FOLLOWER, 1).
-define(CANDIDATE, 2).
-define(LEADER, 3).

-record(raft_state, {
  term,  %%当前任期
  state, %%节点状态
  ref, %%检测ref定时器
  leader, %%领导
  node_ets, %%raft集群节点数据
  nodes = [],    %%集群节点(可以考虑使用tree维护)
  my_vote,    %%上一个任期内投票的节点
  voted_for,  %% 在当前获得选票的候选人的 Id

  log = [],  %% 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号 可以考虑使用sb_tree实现 目前使用[{index,term,payload}]

  %%所有服务器共同的数据
  commit_index,   %%已知的最大的已经被提交的日志条目的索引值
  last_applied,   %%最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）

  %%leader上独有 （选举后重新初始化）
  nextIndex = [],  %%对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
  matchIndex = [] %% 对于每一个服务器，已经复制给他的日志的最高索引值
}).

%%%===================================================================
%%% External functions
%%%===================================================================
get(Node) when is_atom(Node) ->
  case ets:lookup(?RAFT_NODES, Node) of
    [T] -> T;
    [] -> none
  end.

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([NodeEts, Interval]) ->
  true = net_kernel:monitor_nodes(true),
  Ref = erlang:start_timer(Interval, self(), interval),
  NodeEts = ets:new(NodeEts, [ordered_set, named_table]),
  {ok, #raft_state{term = 0, state = ?FOLLOWER, ref = {Ref, Interval}, node_ets = NodeEts, nodes = nodes()}, ?HIBERNATE_TIMEOUT}.


%%请求同步rpc
handle_call({'sync_log', Leader, Msg}, _From, State = #raft_state{ref = {Ref, Interval}}) -> %%附加日志rpc
  {NState, R} = sync_log_handle(State, Msg),
  NRef = reset_timer(Ref, Interval),
  {reply, R, NState#raft_state{ref = {NRef, Interval}}};
%%心跳rpc
handle_call({'heart', Leader, _Msg}, From, State = #raft_state{term = Term, leader = Leader, ref = {Ref, Interval}}) -> %%leader心跳

  NRef = reset_timer(Ref, Interval),
  {reply, ok, State#raft_state{state = ?FOLLOWER, ref = {NRef, Interval}, votes = []}}.

handle_cast(_Request, State = #raft_state{}) ->
  {noreply, State}.

handle_info({timeout, _Ref, 'heart'}, State = #raft_state{term = Term, nodes = Nodes}) -> %%leader 心跳发送
  %%同步心跳
  Msg = [],
  [gen_server:call({?MODULE, Node}, {'heart', node(), Msg}) || Node <- Nodes],
  {noreply, State#raft_state{state = ?CANDIDATE}};

handle_info({timeout, _Ref, interval}, State = #raft_state{term = Term, ref = {_Ref, Interval}, nodes = Nodes}) -> %%follower 心跳检测
  NTerm = Term + 1,
  [gen_server:cast({?MODULE, Node}, {'req_vote', node(), NTerm}) || Node <- Nodes],

  NRef = erlang:start_timer(Interval, self(), interval),
  {noreply, State#raft_state{state = ?CANDIDATE, ref = {NRef, Interval}}};

%%=====================候选人征集选票
handle_info({'req_vote', Node, NTerm}, State = #raft_state{term = Term}) when NTerm < Term -> %%任期不够
  {noreply, State};
%%handle_info({'req_vote', Node, NTerm}, State = #raft_state{my_vote = {NTerm, _}}) -> %%该任期已经投票给其他人
%%  {noreply, State#raft_state{state = ?CANDIDATE}};
handle_info({'req_vote', Node, NTerm}, State = #raft_state{term = Term}) -> %%请求投票
  %%
  {noreply, State#raft_state{state = ?CANDIDATE}};
handle_info({'res_vote', Node}, State = #raft_state{state = ?CANDIDATE, term = Term}) -> %%请求投票响应
  %%
  {noreply, State#raft_state{state = ?CANDIDATE}};

handle_info({nodedown, Node}, State) -> %%节点关闭消息
  %%ping的通，断定节点假死，不做处理
  NState = case net_adm:ping(Node) of
             pang ->
               State1 = reset_node(State),
               State1;
             _ -> State
           end,

  {noreply, NState};
handle_info({nodeup, Node}, State) -> %%节点启动消息
%%假死重连 不做处理
  NState = case raft_server:get(Node) of
             none -> State;
             _ ->
               State1 = reset_node(State),
               State1
           end,
  {noreply, NState}.

terminate(_Reason, _State = #raft_state{}) ->
  ok.

code_change(_OldVsn, State = #raft_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%%重置节点
reset_node(State#{node_ets = NodeEts}) ->
  NNodes = nodes(),
  [reset_node_(NNodes, NodeEts, Node) || {Node, Args} <- ets:tab2list(NodeEts)],
  ok.
reset_node_([], NodeEts, H) ->
  ets:delete(NodeEts, H);
reset_node_([H | T], _NodeEts, H) ->
  ok.
%%重置心跳timer
reset_timer(Ref, Interval) ->
  if
    Ref =:= none -> ok;
    true -> erlang:cancel_timer(Ref)
  end,
  NRef = erlang:start_timer(Interval, self(), interval),
  NRef.
%%===========================附加日志处理====================================

sync_log_handle(#raft_state{term = CurTerm} = State, {Term, _LeaderId, _PrevLogIndex, _PrevLogTerm, _Entries, _LeaderCommit} = Msg) when Term < CurTerm ->
  {State, false};
sync_log_handle(#raft_state{term = CurTerm, log = Log} = State, {Term, LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit} = Msg) when Term < CurTerm ->
  case get_log(PrevLogIndex, Log) of
    {_, PrevLogTerm, _} ->
      ok;

        none -> {State, false} %% 如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false
  end.

%%日志处理
get_log(Index, Log) ->
  case lists:keyfind(Index, 1, Log) of
    false -> none;
    V -> V
  end.
