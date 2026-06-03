%% -*-erlang-*-
%%==============================================================================
%% Copyright 2025 Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%==============================================================================

%%%-------------------------------------------------------------------
%%% @doc
%%%  
%%% @end
%%%
%% @author Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%% @copyright (C) 2025, Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%%%-------------------------------------------------------------------
-module(jhn_s3c_config).
-copyright('Jan Henry Nystrom <JanHenryNystrom@gmail.com>').

%% API
-export([load/0, get/0, get/1]).

%% Includes
-include_lib("jhn_s3c/src/jhn_s3c.hrl").

%% ===================================================================
%% API functions
%% ===================================================================

%%--------------------------------------------------------------------
-spec load() -> ok.
%%--------------------------------------------------------------------
load() ->
    Pool = get_env(hackney_pool),
    HackneyOpts = get_env(hackney_opts),
    Opts = hackney_conf(Pool, HackneyOpts),
    Default = #config{request_type = get_env(request_type),
                      protocol = atom_to_binary(get_env(protocol)),
                      max_tries = get_env(max_tries)},
    Legacy = Default#config{host = get_env(host),
                            port = get_env(port),
                            hackney_opts = Opts,
                            access_key_id = get_env(access_key_id),
                            access_key = get_env(access_key)},
    Config =
        case get_env(servers) of
            undefined -> [{'LEGACY', Legacy}];
            [] -> [{'LEGACY', Legacy}];
            Servers = [_ | _] ->
                [config(S, Default, Pool, HackneyOpts) || S <- Servers]
        end,
    persistent_term:put(?MODULE, Config).


%%--------------------------------------------------------------------
-spec get() -> #config{}.
%%--------------------------------------------------------------------
get() ->
    case persistent_term:get(?MODULE) of
        [] -> erlang:error(no_config);
        [{_, Config} | _] -> Config
    end.


%%--------------------------------------------------------------------
-spec get(_) -> #config{}.
%%--------------------------------------------------------------------
get('DEFAULT') ->
    case persistent_term:get(?MODULE) of
        [{_, Config} | _] -> Config;
        _ -> erlang:error(no_config)
    end;
get(Name) ->
    case jhn_plist:find(Name, persistent_term:get(?MODULE)) of
        undefined -> erlang:error(no_config);
        Config -> Config
    end.

%% ===================================================================
%% Internal functions.
%% ===================================================================

get_env(Key) -> application:get_env(jhn_s3c, Key, undefined).

hackney_conf(Pool, Opts) ->
    lists:ukeymerge(1,
                    lists:sort(Opts),
                    lists:sort([{pool, Pool}, {recv_timeout, 10_000}])).

config({Name, Conf}, Default, Pool, HackneyOpts) ->
    Opts1 = hackney_conf(jhn_plist:find(hackney_pool, Conf, Pool),
                         jhn_plist:find(hackney_opts, Conf, HackneyOpts)),
    #config{request_type = ReqType,
            protocol = Protocol,
            max_tries = MaxRetries} = Default,
    {Name,
     #config{request_type = jhn_plist:find(request_type, Conf, ReqType),
             protocol = jhn_plist:find(protocol, Conf, Protocol),
             host = jhn_plist:find(host, Conf),
             port = jhn_plist:find(port, Conf),
             access_key_id = jhn_plist:find(access_key_id, Conf),
             access_key = jhn_plist:find(access_key, Conf),
             hackney_opts = Opts1,
             max_tries = jhn_plist:find(max_tries, Conf, MaxRetries)
            }
    }.
