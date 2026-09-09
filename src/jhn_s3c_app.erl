%% -*-erlang-*-
%%==============================================================================
%% Copyright 2025-2026 Jan Henry Nystrom <JanHenryNystrom@gmail.com>
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
%% @copyright (C) 2025-2026, Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%%%-------------------------------------------------------------------
-module(jhn_s3c_app).
-copyright('Jan Henry Nystrom <JanHenryNystrom@gmail.com>').

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% Includes
-include_lib("jhn_s3c/src/jhn_s3c.hrl").

%% ===================================================================
%% Application callbacks
%% ===================================================================

%%--------------------------------------------------------------------
-spec start(normal, no_arg) -> {ok, pid()}.
%%--------------------------------------------------------------------
start(normal, no_arg) ->
    jhn_s3c_config:load(),
    Opts = [O || {_, #config{hackney_opts = O}} <- jhn_s3c_config:get()],
    Pools = [jhn_plist:find(pool, O) || O <- Opts],
    [hackney_pool:start_pool(Pool, [{timeout, 120_000}]) || Pool <- Pools],
    {ok, self()}.

%%--------------------------------------------------------------------
-spec stop(_) -> ok.
%%--------------------------------------------------------------------
stop(_) -> ok.

%% ===================================================================
%% Internal functions.
%% ===================================================================
