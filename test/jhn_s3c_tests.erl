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
          {"Get Versioning", ?_test(versioning(get))},
          {"Put Versioning", ?_test(versioning(put))},
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
          {"Get Versioning", ?_test(versioning(get))},
          {"Put Versioning", ?_test(versioning(put))},
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

object_listing_test_() ->
    {inorder,
        {setup, setup(object), teardown(object),
         [{"List max_keys", ?_test(list(max_keys))},
          {"List token", ?_test(list(continuation_token))},
          {"count objects", ?_test(list(count))}
         ]}}.

versioning_listing_test_() ->
    {inorder,
        {setup, setup(versioning), teardown(versioning),
         [{"Versioning list", ?_test(versioning(list))}
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
    end;
setup(versioning) ->
    logger:remove_handler(default),
    fun() ->
            application:load(jhn_s3c),
            application:set_env(jhn_s3c, request_type, virtual_host),
            {ok, Started} = application:ensure_all_started(jhn_s3c),
            ok = jhn_s3c:create_bucket(?BUCKET),
            ok = jhn_s3c:put_bucket_versioning(?BUCKET, enabled),
            Started
    end.

teardown(bucket) ->
    fun(Started) -> [application:stop(App) || App <- Started] end;
teardown(object) ->
    fun(Started) ->
            ok = jhn_s3c:delete_objects(?BUCKET, jhn_s3c:list_objects(?BUCKET)),
            ok = jhn_s3c:delete_bucket(?BUCKET),
            [application:stop(App) || App <- Started]
    end;
teardown(versioning) ->
    %% ok = jhn_s3c:put_bucket_versioning(?BUCKET, suspended),
    %% ok = jhn_s3c:delete_bucket(?BUCKET),
    fun(Started) -> [application:stop(App) || App <- Started] end.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

create(bucket) -> ?assertMatch(ok, jhn_s3c:create_bucket(?BUCKET));
create(object) ->
    Key = jhn_uuid:gen(v7, [binary]),
    Object = jhn_json:encode(#{hallo => goodbye}, [binary]),
    ?assertMatch(ok, jhn_s3c:put_object(?BUCKET, Key, Object)).

list(buckets) -> ?assertMatch([?BUCKET], jhn_s3c:list_buckets());
list(objects) -> ?assertMatch([_, _, _], jhn_s3c:list_objects(?BUCKET));
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
versioning(put) ->
    ?assertMatch(ok, jhn_s3c:put_bucket_versioning(?BUCKET, enabled)),
    ?assertMatch(#{status := enabled},
                 jhn_s3c:get_bucket_versioning(?BUCKET)),
    ?assertMatch(ok, jhn_s3c:put_bucket_versioning(?BUCKET, suspended)),
    ?assertMatch(#{status := suspended},
                 jhn_s3c:get_bucket_versioning(?BUCKET));
versioning(list) ->
    put_n(1, ~"1"),
    put_n(1, ~"2"),
    ?assertMatch([#{key := Key, is_latest := true, version_id := _},
                  #{key := Key, is_latest := false, version_id := _}],
                 jhn_s3c:list_object_versions(?BUCKET)),
    VKs = [#{key := Key, version_id := V1}, #{version_id := V2}] =
        jhn_s3c:list_object_versions(?BUCKET),
    ?assertMatch(true, V1 /= V2),

    dbg:tracer(process, {fun(A, _) -> ?debugFmt("~nCall: ~p~n", [A]) end, x}),
    dbg:p(all, c),
    dbg:tp(hackney, request, x),
    dbg:tp(gen_tcp, send, x),
    dbg:tpl(jhn_s3c, aws_auth, x),
    dbg:tp(crypto, mac, x),

    ?assertMatch(ok, jhn_s3c:delete_object(?BUCKET, Key, [{version_id, V1}])),
    ?assertMatch(ok, jhn_s3c:delete_object(?BUCKET, Key, [{version_id, V2}])).
%%    ?assertMatch(ok, jhn_s3c:delete_objects(?BUCKET, VKs)).

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

put_n(N) ->
    [jhn_s3c:put_object(?BUCKET, key(I), object(I)) || I <- lists:seq(1, N)].

put_n(N, V) ->
    [jhn_s3c:put_object(?BUCKET, key(I), object(I, V)) || I <- lists:seq(1, N)].

key(N) -> <<"Key_", (integer_to_binary(N))/binary>>.
object(N) -> <<"Object_", (integer_to_binary(N))/binary>>.
object(N, V) -> <<"Object_", (integer_to_binary(N))/binary, "_", V/binary>>.
