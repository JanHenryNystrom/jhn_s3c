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
-module(jhn_s3c_app).
-copyright('Jan Henry Nystrom <JanHenryNystrom@gmail.com>').

-behaviour(application).

%% API
-export([config/0]).

%% Application callbacks
-export([start/2, stop/1]).

%% Includes
-include_lib("jhn_s3c/src/jhn_s3c.hrl").

%% ===================================================================
%% API functions
%% ===================================================================

%%--------------------------------------------------------------------
-spec config() -> #config{}.
%%--------------------------------------------------------------------
config() -> persistent_term:get(?MODULE).

%% ===================================================================
%% Application callbacks
%% ===================================================================

%%--------------------------------------------------------------------
-spec start(normal, no_arg) -> {ok, pid()}.
%%--------------------------------------------------------------------
start(normal, no_arg) ->
    Opts = [with_body, {pool, get_env(hackney_pool)}, {recv_timeout, 10_000}],
    Opts1 = lists:ukeymerge(1,
                            lists:sort(get_env(hackney_opts)),
                            lists:sort(Opts)),
    Config = #config{request_type = get_env(request_type),
                     protocol = atom_to_binary(get_env(protocol)),
                     host = get_env(host),
                     port = integer_to_binary(get_env(port)),
                     access_key_id = get_env(access_key_id),
                     access_key = get_env(access_key),
                     hackney_opts = Opts1,
                     max_tries = get_env(max_tries)},
    persistent_term:put(?MODULE, Config),
    {ok, self()}.

%%--------------------------------------------------------------------
-spec stop(_) -> ok.
%%--------------------------------------------------------------------
stop(_) -> ok.

%% ===================================================================
%% Inernal functions.
%% ===================================================================

get_env(Key) -> application:get_env(jhn_s3c, Key, undefined).

