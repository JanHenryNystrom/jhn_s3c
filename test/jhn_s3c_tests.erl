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
%%%   A S3 library CRUD library eunit test suite
%%% @end
%%%
%% @author Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%% @copyright (C) 2025, Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%%%-------------------------------------------------------------------
-module(jhn_s3c_tests).
-copyright('Jan Henry Nystrom <JanHenryNystrom@gmail.com>').

%% Includes
-include_lib("eunit/include/eunit.hrl").

%% Defines
-define(BUCKET, ~"test-bucket").

%%------------------------------------------------------------------------------
%% Fixtures
%%------------------------------------------------------------------------------

bucket_test_() ->
    {inorder,
        {setup, setup(bucket), teardown(bucket),
         [{"Create", ?_test(create(bucket))},
          {"List", ?_test(list(buckets))},
          {"Delete", ?_test(delete(bucket))}
         ]}}.

object_test_() ->
    {inorder,
        {setup, setup(object), teardown(object),
         [{"Create", ?_test(create(object))},
          {"Read", ?_test(read(object))},
          {"Update", ?_test(update(object))},
          {"List", ?_test(list(objects))},
          {"Delete", ?_test(delete(object))}
         ]}}.

bucket_path_test_() ->
    {inorder,
        {setup, setup(bucket_path), teardown(bucket),
         [{"Create", ?_test(create(bucket))},
          {"List", ?_test(list(buckets))},
          {"Delete", ?_test(delete(bucket))}
         ]}}.

object_path_test_() ->
    {inorder,
        {setup, setup(object_path), teardown(object),
         [{"Create", ?_test(create(object))},
          {"Read", ?_test(read(object))},
          {"Update", ?_test(update(object))},
          {"List", ?_test(list(objects))},
          {"Delete", ?_test(delete(object))}
         ]}}.

%%------------------------------------------------------------------------------
%% Setup
%%------------------------------------------------------------------------------

setup(bucket) ->
    logger:remove_handler(default),
    fun() ->
            {ok, Started} = application:ensure_all_started(jhn_s3c),
            Started
    end;
setup(object) ->
    logger:remove_handler(default),
    fun() ->
            {ok, Started} = application:ensure_all_started(jhn_s3c),
            ok = jhn_s3c:create_bucket(?BUCKET),
            Started
    end;
setup(bucket_path) ->
    logger:remove_handler(default),
    fun() ->
            application:load(jhn_s3c),
            application:set_env(jhn_s3c, request_type, path),
            {ok, Started} = application:ensure_all_started(jhn_s3c),
            Started
    end;
setup(object_path) ->
    logger:remove_handler(default),
    fun() ->
            application:load(jhn_s3c),
            application:set_env(jhn_s3c, request_type, path),
            {ok, Started} = application:ensure_all_started(jhn_s3c),
            ok = jhn_s3c:create_bucket(?BUCKET),
            Started
    end.

teardown(bucket) ->
    fun(Started) -> [application:stop(App) || App <- Started] end;
teardown(object) ->
    fun(Started) ->
            ok = jhn_s3c:delete_objects(?BUCKET, jhn_s3c:list_objects(?BUCKET)),
            ok = jhn_s3c:delete_bucket(?BUCKET),
            [application:stop(App) || App <- Started]
    end.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

create(bucket) ->
    ?assertMatch(ok, jhn_s3c:create_bucket(?BUCKET));
create(object) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object = jhn_json:encode(#{hallo => goodbye}, [binary]),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET, Key, Object)).

list(buckets) ->
    ?assertMatch([?BUCKET], jhn_s3c:list_buckets());
list(objects) ->
    ?assertMatch([_, _, _], jhn_s3c:list_objects(?BUCKET)).

read(object) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object = jhn_json:encode(#{hallo => goodbye}, [binary]),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET, Key, Object)),
    ?assertMatch(Object, jhn_s3c:get_object(?BUCKET, Key)),
    ?assertMatch(<<_, _/binary>>,
                 jhn_plist:find(~"etag", jhn_s3c:head_object(?BUCKET, Key))).

update(object) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object1 = jhn_json:encode(#{hallo => goodbye}),
    Object2 = jhn_json:encode(#{hallo => <<"tìoraidh"/utf8>>}, [binary]),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET, Key, Object1)),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET, Key, Object2)),
    ?assertMatch(Object2, jhn_s3c:get_object(?BUCKET, Key)).

delete(bucket) ->
    ?assertMatch(ok, jhn_s3c:delete_bucket(?BUCKET));
delete(object) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object = jhn_json:encode(#{hallo => goodbye}, [binary]),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET, Key, Object)),
    ?assertMatch(Object, jhn_s3c:get_object(?BUCKET, Key)),
    ?assertMatch(ok, jhn_s3c:delete_object(?BUCKET, Key)),
    ?assertMatch({error, {404, _, _}}, jhn_s3c:get_object(?BUCKET, Key)).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------
