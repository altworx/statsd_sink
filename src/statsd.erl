%%%-------------------------------------------------------------------
%%% @copyright (C) 2017, altworx
%%% @doc
%%% StatsD client process and vmstats sink, coudl be improved with pooled Sockets fs
%%% @end
%%%-------------------------------------------------------------------
-module(statsd).

-behaviour(gen_server).
-behaviour(vmstats_sink).

%% API
-export([start_link/0, get_cached/0, gauge/2, counter/2, timing/2, increment/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% vmstats_sink callbacks
-export([collect/3]).

-define(SERVER, ?MODULE).
-define(STATSD_PORT, 8125).
-define(STATSD_HOST, "localhost").

-record(state, {host = ?STATSD_HOST,
                port = ?STATSD_PORT,
                gauges = #{},
                socket}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, Error :: term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec collect(counter | gauge | timing, iodata() , term()) -> ok.
collect(Type,Key,Value) ->
    KeyBinary = key_to_binary(Key),
    gen_server:cast(?SERVER, {Type, KeyBinary, Value}).

-spec gauge(atom() | iolist() | binary(), number()) -> ok.
gauge(Key, Value) ->
    gen_server:cast(?SERVER, {gauge, key_to_binary(Key), Value}).

-spec counter(atom() | iolist() | binary(), number()) -> ok.
counter(Key, Value) ->
    gen_server:cast(?SERVER, {counter, Key, Value}).

-spec increment(atom() | iolist() | binary()) -> ok.
increment(Key) ->
    gen_server:cast(?SERVER, {counter, Key, 1}).

-spec timing(atom() | iolist() | binary(), number()) -> ok.
timing(Key, Value) ->
    gen_server:cast(?SERVER, {timing, Key, Value}).

get_cached() ->
    gen_server:call(?SERVER,get_cached).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    {ok, Socket} = gen_udp:open(0),
    {ok, #state{socket = Socket}}.

handle_call(get_cached, _From, #state{gauges = Gauges} = State) ->
    {reply, #{gauges => Gauges}, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({Type, Key, Value}, #state{port = Port, host = Host, socket = Socket} =State) ->
    Msg = io_lib:format("~s:~w|~s", [Key, Value, type_datagram(Type)]),
    gen_udp:send(Socket, Host, Port, Msg),
    NewState = cache_statsd_to_state(Type, Key, Value, State),
    {noreply, NewState};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{socket = Socket}) ->
    gen_udp:close(Socket),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
type_datagram(counter) -> c;
type_datagram(gauge) -> g;
type_datagram(timing) -> ms.

cache_statsd_to_state(gauge, Key, Value, #state{gauges = Gauges} = State) when is_binary(Key) ->
    State#state{gauges = Gauges#{Key => Value}};
cache_statsd_to_state(_,_,_,State) -> State.

key_to_binary(Key) when is_atom(Key)->
    atom_to_list(Key);
key_to_binary(Key) when is_list(Key)->
    iolist_to_binary(Key);
key_to_binary(Key) when is_binary(Key) ->
    Key.
