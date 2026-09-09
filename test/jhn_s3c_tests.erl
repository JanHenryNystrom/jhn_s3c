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
%%%   A S3 CRUD library eunit test suite
%%% @end
%%%
%% @author Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%% @copyright (C) 2025-2026, Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%%%-------------------------------------------------------------------
-module(jhn_s3c_tests).
-copyright('Jan Henry Nystrom <JanHenryNystrom@gmail.com>').

%% Includes
-include_lib("eunit/include/eunit.hrl").

%% Defines
-define(BUCKET, ~"test-bucket").
-define(BUCKET_A, ~"atest-bucket").
-define(BUCKET_B, ~"btest-bucket").

-define(SERVER_A,
        {server_a, [{host, ~"s3.service"}, {port, 9000},
                    {access_key_id, ~"admin"}, {access_key, ~"password"}]}).

-define(SERVER_B,
        {server_b, [{host, ~"s3bis.service"}, {port, 8000},
                    {pool, another_pool},
                    {access_key_id, ~"hugo"}, {access_key, ~"password"}]}).

%%------------------------------------------------------------------------------
%% Fixtures
%%------------------------------------------------------------------------------

bucket_test_() ->
    {inorder,
        {setup, setup(bucket, legacy), teardown(bucket, legacy),
         [{"Create", ?_test(create(bucket))},
          {"List", ?_test(list(buckets))},
          {"Get Versioning", ?_test(versioning(get))},
          {"Put Versioning", ?_test(versioning(put))},
          {"Delete", ?_test(delete(bucket))}
         ]}}.

bucket_default_test_() ->
    {inorder,
        {setup, setup(bucket, default), teardown(bucket, default),
         [{"Create", ?_test(create(bucket))},
          {"List", ?_test(list(buckets))},
          {"Get Versioning", ?_test(versioning(get))},
          {"Put Versioning", ?_test(versioning(put))},
          {"Delete", ?_test(delete(bucket))}
         ]}}.

bucket_two_test_() ->
    {inorder,
        {setup, setup(bucket, default), teardown(bucket, default),
         [{"Create A", ?_test(create(bucket_a))},
          {"List A", ?_test(list(buckets_a))},
          {"Create B", ?_test(create(bucket_b))},
          {"List B", ?_test(list(buckets_b))},
          {"Get Versioning", ?_test(versioning(get_b))},
          {"Put Versioning", ?_test(versioning(put_b))},
          {"Delete B", ?_test(delete(bucket_b))},
          {"Delete A", ?_test(delete(bucket_a))}
         ]}}.

object_test_() ->
    {inorder,
        {setup, setup(object, legacy), teardown(object, legacy),
         [{"Create", ?_test(create(object))},
          {"Read", ?_test(read(object))},
          {"Update", ?_test(update(object))},
          {"List", ?_test(list(objects))},
          {"Delete", ?_test(delete(object))}
         ]}}.

object_two_test_() ->
    {inorder,
        {setup, setup(object, two), teardown(object, two),
         [{"Create", ?_test(create(object_a))},
          {"Create", ?_test(create(object_b))},
          {"Read", ?_test(read(object_a))},
          {"Read", ?_test(read(object_b))},
          {"Update", ?_test(update(object_a))},
          {"Update", ?_test(update(object_b))},
          {"List", ?_test(list(objects_a))},
          {"List", ?_test(list(objects_b))},
          {"Delete", ?_test(delete(object_a))},
          {"Delete", ?_test(delete(object_b))}
         ]}}.

bucket_path_test_() ->
    {inorder,
        {setup, setup(bucket_path, legacy), teardown(bucket, legacy),
         [{"Create", ?_test(create(bucket))},
          {"List", ?_test(list(buckets))},
          {"Get Versioning", ?_test(versioning(get))},
          {"Put Versioning", ?_test(versioning(put))},
          {"Delete", ?_test(delete(bucket))}
         ]}}.

object_path_test_() ->
    {inorder,
        {setup, setup(object_path, legacy), teardown(object, legacy),
         [{"Create", ?_test(create(object))},
          {"Read", ?_test(read(object))},
          {"Update", ?_test(update(object))},
          {"List", ?_test(list(objects))},
          {"Delete", ?_test(delete(object))}
         ]}}.

object_listing_test_() ->
    {inorder,
        {setup, setup(object, legacy), teardown(object, legacy),
         [{"List max_keys", ?_test(list(max_keys))},
          {"List token", ?_test(list(continuation_token))},
          {"count objects", ?_test(list(count))}
         ]}}.

versioning_listing_test_() ->
    {inorder,
        {setup, setup(versioning, legacy), teardown(versioning, legacy),
         [{"Versioning list", ?_test(versioning(list))}
         ]}}.

%%------------------------------------------------------------------------------
%% Setup
%%------------------------------------------------------------------------------

setup(bucket, legacy) ->
    logger:remove_handler(default),
    fun() ->
            {ok, Started} = application:ensure_all_started(jhn_s3c),
            Started
    end;
setup(bucket, default) ->
    logger:remove_handler(default),
    fun() ->
            application:load(jhn_s3c),
            application:set_env(jhn_s3c, servers, [?SERVER_B, ?SERVER_A]),
            {ok, Started} = application:ensure_all_started(jhn_s3c),
            Started
    end;
setup(object, two) ->
    logger:remove_handler(default),
    fun() ->
            application:load(jhn_s3c),
            application:set_env(jhn_s3c, servers, [?SERVER_B, ?SERVER_A]),
            {ok, Started} = application:ensure_all_started(jhn_s3c),
            ok = jhn_s3c:create_bucket(?BUCKET_A, [{server, server_a}]),
            ok = jhn_s3c:create_bucket(?BUCKET_B, [{server, server_b}]),
            Started
    end;
setup(object, _) ->
    logger:remove_handler(default),
    fun() ->
            {ok, Started} = application:ensure_all_started(jhn_s3c),
            ok = jhn_s3c:create_bucket(?BUCKET),
            Started
    end;
setup(bucket_path, _) ->
    logger:remove_handler(default),
    fun() ->
            application:load(jhn_s3c),
            application:set_env(jhn_s3c, request_type, path),
            {ok, Started} = application:ensure_all_started(jhn_s3c),
            Started
    end;
setup(object_path, _) ->
    logger:remove_handler(default),
    fun() ->
            application:load(jhn_s3c),
            application:set_env(jhn_s3c, request_type, path),
            {ok, Started} = application:ensure_all_started(jhn_s3c),
            ok = jhn_s3c:create_bucket(?BUCKET),
            Started
    end;
setup(versioning, _) ->
    logger:remove_handler(default),
    fun() ->
            application:load(jhn_s3c),
            application:set_env(jhn_s3c, request_type, virtual_host),
            {ok, Started} = application:ensure_all_started(jhn_s3c),
            ok = jhn_s3c:create_bucket(?BUCKET),
            ok = jhn_s3c:put_bucket_versioning(?BUCKET, enabled),
            Started
    end.

teardown(bucket, legacy) ->
    fun(Started) -> [application:stop(App) || App <- Started],
                    application:unload(s3c)
    end;
teardown(bucket, default) ->
    fun(Started) ->
            [application:stop(App) || App <- Started],
            application:unload(s3c)
    end;
teardown(object, two) ->
    fun(Started) ->
            ok = jhn_s3c:delete_objects(
                   ?BUCKET_A,
                   jhn_s3c:list_objects(?BUCKET_A, [{server, server_a}]),
                   [{server, server_a}]),
            ok = jhn_s3c:delete_bucket(?BUCKET_A, [{server, server_a}]),
            ok = jhn_s3c:delete_objects(
                   ?BUCKET_B,
                   jhn_s3c:list_objects(?BUCKET_B, [{server, server_b}]),
                   [{server, server_b}]),
            ok = jhn_s3c:delete_bucket(?BUCKET_B, [{server, server_b}]),
            [application:stop(App) || App <- Started],
            application:unload(s3c)
    end;
teardown(object, _) ->
    fun(Started) ->
            ok = jhn_s3c:delete_objects(?BUCKET, jhn_s3c:list_objects(?BUCKET)),
            ok = jhn_s3c:delete_bucket(?BUCKET),
            [application:stop(App) || App <- Started],
            application:unload(s3c)
    end;
teardown(versioning, _) ->
    %% ok = jhn_s3c:put_bucket_versioning(?BUCKET, suspended),
    %% ok = jhn_s3c:delete_bucket(?BUCKET),
    fun(Started) ->
            [application:stop(App) || App <- Started],
            application:unload(s3c)
    end.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

create(bucket) -> ?assertMatch(ok, jhn_s3c:create_bucket(?BUCKET));
create(bucket_a) ->
    ?assertMatch(ok, jhn_s3c:create_bucket(?BUCKET_A, [{server, server_a}]));
create(bucket_b) ->
    ?assertMatch(ok, jhn_s3c:create_bucket(?BUCKET_B, [{server, server_b}]));
create(object) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object = jhn_json:encode(#{hallo => goodbye}, [binary]),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET, Key, Object));
create(object_a) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object = jhn_json:encode(#{hallo => goodbye_a}, [binary]),
    ?assertMatch(ok,
                 jhn_s3c:put_object(?BUCKET_A,
                                    Key,
                                    Object,
                                    [{server, server_a}]));
create(object_b) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object = jhn_json:encode(#{hallo => goodbye_b}, [binary]),
    ?assertMatch(ok,
                 jhn_s3c:put_object(?BUCKET_B,
                                    Key,
                                    Object,
                                    [{server, server_b}])).

list(buckets) -> ?assertMatch([?BUCKET], jhn_s3c:list_buckets());
list(buckets_a) ->
    ?assertMatch([?BUCKET_A], jhn_s3c:list_buckets([{server, server_a}]));
list(buckets_b) ->
    ?assertMatch([?BUCKET_B], jhn_s3c:list_buckets([{server, server_b}]));
list(objects) -> ?assertMatch([_, _, _], jhn_s3c:list_objects(?BUCKET));
list(objects_a) ->
    ?assertMatch([_, _, _],
                 jhn_s3c:list_objects(?BUCKET_A, [{server, server_a}]));
list(objects_b) ->
    ?assertMatch([_, _, _],
                 jhn_s3c:list_objects(?BUCKET_B, [{server, server_b}]));
list(max_keys) ->
    put_n(10),
    ?assertMatch(#{token := _, keys := [_, _, _, _, _]},
                 jhn_s3c:list_objects(?BUCKET, [{max_keys, 5}])),
    ?assertMatch(10, length(jhn_s3c:list_objects(?BUCKET, []))),
    ok = jhn_s3c:delete_objects(?BUCKET, jhn_s3c:list_objects(?BUCKET));
list(continuation_token) ->
    put_n(10),
    #{token := T, keys := Ks} = jhn_s3c:list_objects(?BUCKET, [{max_keys, 5}]),
    ok = jhn_s3c:delete_objects(?BUCKET, Ks),
    ?assertMatch([_, _, _, _, _],
                 jhn_s3c:list_objects(?BUCKET, [{max_keys, 5}, {token, T}])),
    ok = jhn_s3c:delete_objects(?BUCKET, jhn_s3c:list_objects(?BUCKET));
list(count) ->
    put_n(213),
    ?assertMatch(213, jhn_s3c:count_objects(?BUCKET, 3)),
    #{keys := Keys} = jhn_s3c:list_objects(?BUCKET, [{max_keys, 14}]),
    ok = jhn_s3c:delete_objects(?BUCKET, Keys),
    ?assertMatch(199, jhn_s3c:count_objects(?BUCKET)),
    ok = jhn_s3c:delete_objects(?BUCKET, jhn_s3c:list_objects(?BUCKET)).

versioning(get) ->
    ?assertMatch(#{status := suspended},
                 jhn_s3c:get_bucket_versioning(?BUCKET));
versioning(get_b) ->
    ?assertMatch(#{status := suspended},
                 jhn_s3c:get_bucket_versioning(?BUCKET_B));
versioning(put) ->
    ?assertMatch(ok, jhn_s3c:put_bucket_versioning(?BUCKET, enabled)),
    ?assertMatch(#{status := enabled},
                 jhn_s3c:get_bucket_versioning(?BUCKET)),
    ?assertMatch(ok, jhn_s3c:put_bucket_versioning(?BUCKET, suspended)),
    ?assertMatch(#{status := suspended},
                 jhn_s3c:get_bucket_versioning(?BUCKET));
versioning(put_b) ->
    ?assertMatch(ok, jhn_s3c:put_bucket_versioning(?BUCKET_B, enabled)),
    ?assertMatch(#{status := enabled},
                 jhn_s3c:get_bucket_versioning(?BUCKET_B)),
    ?assertMatch(ok, jhn_s3c:put_bucket_versioning(?BUCKET_B, suspended)),
    ?assertMatch(#{status := suspended},
                 jhn_s3c:get_bucket_versioning(?BUCKET_B));
versioning(list) ->
    put_n(1, ~"1"),
    put_n(1, ~"2"),
    ?assertMatch([#{key := Key, is_latest := true, version_id := _},
                  #{key := Key, is_latest := false, version_id := _}],
                 jhn_s3c:list_object_versions(?BUCKET)),
    VKs = [#{version_id := V1}, #{version_id := V2}] =
        jhn_s3c:list_object_versions(?BUCKET),
    ?assertMatch(true, V1 /= V2),
    ?assertMatch(ok, jhn_s3c:delete_objects(?BUCKET, VKs)).

read(object) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object = jhn_json:encode(#{hallo => goodbye}, [binary]),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET, Key, Object)),
    ?assertMatch(Object, jhn_s3c:get_object(?BUCKET, Key)),
    ?assertMatch(<<_, _/binary>>,
                 jhn_plist:find(~"etag", jhn_s3c:head_object(?BUCKET, Key)));
read(object_a) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object = jhn_json:encode(#{hallo => goodbye_a}, [binary]),
    ?assertMatch(ok,
                 jhn_s3c:put_object(?BUCKET_A,
                                    Key,
                                    Object,
                                    [{server, server_a}])),
    ?assertMatch(Object, jhn_s3c:get_object(?BUCKET_A,Key,[{server,server_a}])),
    ?assertMatch(<<_, _/binary>>,
                 jhn_plist:find(~"etag",
                                jhn_s3c:head_object(?BUCKET_A,
                                                    Key,
                                                    [{server, server_a}])));
read(object_b) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object = jhn_json:encode(#{hallo => goodbye_b}, [binary]),
    ?assertMatch(ok,
                 jhn_s3c:put_object(?BUCKET_B,
                                    Key,
                                    Object,
                                    [{server, server_b}])),
    ?assertMatch(Object, jhn_s3c:get_object(?BUCKET_B,Key,[{server,server_b}])),
    ?assertMatch(<<_, _/binary>>,
                 jhn_plist:find(~"etag",
                                jhn_s3c:head_object(?BUCKET_B,
                                                    Key,
                                                    [{server, server_b}]))).

update(object) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object1 = jhn_json:encode(#{hallo => goodbye}),
    Object2 = jhn_json:encode(#{hallo => <<"tìoraidh"/utf8>>}, [binary]),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET, Key, Object1)),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET, Key, Object2)),
    ?assertMatch(Object2, jhn_s3c:get_object(?BUCKET, Key));
update(object_a) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object1 = jhn_json:encode(#{hallo => goodbye_a}),
    Object2 = jhn_json:encode(#{hallo => <<"tìoraidh_a"/utf8>>}, [binary]),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET_A,
                                        Key,
                                        Object1,
                                        [{server, server_a}])),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET_A,
                                        Key,
                                        Object2,
                                        [{server, server_a}])),
    ?assertMatch(Object2, jhn_s3c:get_object(?BUCKET_A,
                                             Key,
                                             [{server, server_a}]));
update(object_b) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object1 = jhn_json:encode(#{hallo => goodbye_b}),
    Object2 = jhn_json:encode(#{hallo => <<"tìoraidh_b"/utf8>>}, [binary]),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET_B,
                                        Key,
                                        Object1,
                                        [{server, server_b}])),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET_B,
                                        Key,
                                        Object2,
                                        [{server, server_b}])),
    ?assertMatch(Object2, jhn_s3c:get_object(?BUCKET_B,
                                             Key,
                                             [{server, server_b}])).

delete(bucket) ->
    ?assertMatch(ok, jhn_s3c:delete_bucket(?BUCKET));
delete(bucket_a) ->
    ?assertMatch(ok, jhn_s3c:delete_bucket(?BUCKET_A, [{server, server_a}]));
delete(bucket_b) ->
    ?assertMatch(ok, jhn_s3c:delete_bucket(?BUCKET_B, [{server, server_b}]));
delete(object) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object = jhn_json:encode(#{hallo => goodbye}, [binary]),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET, Key, Object)),
    ?assertMatch(Object, jhn_s3c:get_object(?BUCKET, Key)),
    ?assertMatch(ok, jhn_s3c:delete_object(?BUCKET, Key)),
    ?assertMatch({error, {404, _, _}}, jhn_s3c:get_object(?BUCKET, Key));
delete(object_a) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object = jhn_json:encode(#{hallo => goodbye_a}, [binary]),
    ?assertMatch(ok,
                 jhn_s3c:put_object(?BUCKET_A,
                                    Key,
                                    Object,
                                    [{server, server_a}])),
    ?assertMatch(Object,
                 jhn_s3c:get_object(?BUCKET_A, Key, [{server, server_a}])),
    ?assertMatch(ok,
                 jhn_s3c:delete_object(?BUCKET_A, Key, [{server, server_a}])),
    ?assertMatch({error, {404, _, _}},
                 jhn_s3c:get_object(?BUCKET_A, Key, [{server, server_a}]));
delete(object_b) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object = jhn_json:encode(#{hallo => goodbye_b}, [binary]),
    ?assertMatch(ok,
                 jhn_s3c:put_object(?BUCKET_B,
                                    Key,
                                    Object,
                                    [{server, server_b}])),
    ?assertMatch(Object,
                 jhn_s3c:get_object(?BUCKET_B, Key, [{server, server_b}])),
    ?assertMatch(ok,
                 jhn_s3c:delete_object(?BUCKET_B, Key, [{server, server_b}])),
    ?assertMatch({error, {404, _, _}},
                 jhn_s3c:get_object(?BUCKET_B, Key, [{server, server_b}])).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

put_n(N) ->
    [jhn_s3c:put_object(?BUCKET, key(I), object(I)) || I <- lists:seq(1, N)].

put_n(N, V) ->
    [jhn_s3c:put_object(?BUCKET, key(I), object(I, V)) || I <- lists:seq(1, N)].

key(N) -> <<"Key_", (integer_to_binary(N))/binary>>.
object(N) -> <<"Object_", (integer_to_binary(N))/binary>>.
object(N, V) -> <<"Object_", (integer_to_binary(N))/binary, "_", V/binary>>.
