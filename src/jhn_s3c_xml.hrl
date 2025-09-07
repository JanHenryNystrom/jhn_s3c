%% -*-erlang-*-
%%==============================================================================
%% Copyright 2015 Jan Henry Nystrom <JanHenryNystrom@gmail.com>
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

%% Defines

%% Records
-record(xml, {tag :: atom() | binary(),
              attrs = [] :: [attr()],
              children = [] :: [xml() | cdata() | binary()]}).
-record(cdata, {data = <<>> :: binary()}).

%% Types
-type attr() :: {atom() | binary(), undefined | binary() | atom()}.
-type xml() :: #xml{}.
-type cdata() :: #cdata{}.
