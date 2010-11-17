-module(basho_bench_driver_hdfs_thrift).

-export([new/1,
         run/4]).

-include_lib("hdfsbench/include/hadoopfs_types.hrl").
-include("basho_bench.hrl").

-record(state, {client, directory}).

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    Host = basho_bench_config:get(hdfs_host, "localhost"),
    Port = basho_bench_config:get(hdfs_port, 55000),
    Directory = basho_bench_config:get(hdfs_directory, "/bench"),

    ?DEBUG("Connecting to ~p:~p\n", [Host, Port]),

    {ok, HDFSClient} = thrift_client_util:new(Host,
					      Port,
					      thriftHadoopFileSystem_thrift,
					      []),
    {ok, #state{client=HDFSClient, directory=Directory}}.

run(get, KeyGen, _ValueGen, State) ->
    _Key = KeyGen(),
    {ok, State};
run(put, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    Value = ValueGen(),
    Path = #pathname{pathname=State#state.directory ++ "/" ++ Key},
    {C1, R1} = thrift_client:call(State#state.client, create, [Path]),
    case R1 of
	{ok, FileHandle} ->
	    {C2, R2} = thrift_client:call(C1, write, [FileHandle, Value]),
	    case R2 of
		{ok, true} ->
		    {C3, _R3} = thrift_client:call(C2, close, [FileHandle]),
		    {ok, State#state{client=C3}};
		_ ->
		    {error, write_error, State#state{client=C2}}
	    end;
	_ ->
	    {error, filehandle_error, State#state{client=C1}}
    end;
run(delete, KeyGen, _ValueGen, State) ->
    _Key = KeyGen(),
    {ok, State}.
