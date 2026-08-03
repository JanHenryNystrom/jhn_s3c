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
-module(jhn_s3c).
-copyright('Jan Henry Nystrom <JanHenryNystrom@gmail.com>').

%% API functions
-export([%% Bucket
         create_bucket/1, create_bucket/2, list_buckets/0, list_buckets/1,
         delete_bucket/1, delete_bucket/2,
         get_bucket_versioning/1, get_bucket_versioning/2,
         put_bucket_versioning/2, put_bucket_versioning/3,
         %% Object
         count_objects/1, count_objects/2,
         put_object/3, put_object/4,
         list_objects/1, list_objects/2,
         list_object_versions/1, list_object_versions/2,
         get_object/2, get_object/3, head_object/2, head_object/3,
         delete_object/2,  delete_object/3,
         delete_objects/2, delete_objects/3]).

%% Includes
-include_lib("jhn_s3c/src/jhn_s3c.hrl").
-include_lib("jhn_s3c/src/jhn_s3c_xml.hrl").
-include_lib("kernel/include/logger.hrl").

%% Exported types
-export_type([bucket/0, key/0, value/0, error/0]).

%% Types
-type bucket()       :: binary().
-type key()          :: binary().
-type version_key()  :: #{key := key(),
                          is_latest := boolean(),
                          version_id := binary()}.
-type sub_resource() :: binary().
-type value()        :: iodata().
-type opts()         :: [opt()].

-type versioning() :: #{status := status(), mfa_delete => mfa_delete()}.
-type status()     :: enabled | suspended.
-type mfa_delete() :: enabled | disabled.

-type opt()           :: {server, _} |
                         {max_keys, integer()} | {token, binary()} |
                         {key_marker, binary()} |
                         {version_id_marker, binary()} |
                         {version_id, binary()}.
-type hackney_error() :: _.
-type http_error()    :: {http_status(), headers(), body()}.
-type exception()     :: {class(), reason(),  [tuple()]}.
-type class()         :: exit | error | throw.
-type reason()        :: atom() | {atom(), _}.
-type error()         :: {error, http_error() | exception() | hackney_error()}.
-type http_status()   :: integer().
-type headers()       :: [{binary(), binary()}].
-type body()          :: binary().

-type method() :: head | get | put | post | delete.

%% Defines
-define(SUCCESS(Status), Status >= 200, Status =< 299).

%% Records
-record(req, {server         :: _,
              method  = get  :: method(),
              bucket  = ~""  :: bucket(),
              key     = ~""  :: key(),
              sub     = ~""  :: sub_resource(),
              object  = <<>> :: value(),
              kvs     = []   :: [{binary(), binary()}] | binary()
             }).

-record(state, {method       :: method(),
                uri          :: binary(),
                headers      :: [{_, _}],
                object       :: value(),
                hackney_opts :: [{_, _}]
               }).

%% ===================================================================
%% API functions.
%% ===================================================================

%%--------------------------------------------------------------------
-spec create_bucket(bucket()) -> ok | error().
%%--------------------------------------------------------------------
create_bucket(Bucket) -> create_bucket(Bucket, []).

%%--------------------------------------------------------------------
-spec create_bucket(bucket(), opts()) -> ok | error().
%%--------------------------------------------------------------------
create_bucket(Bucket, Opts) ->
    case exec(#req{method = put, bucket = Bucket, server = server(Opts)}) of
        {ok, _, _} -> ok;
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec list_buckets() -> [bucket()] | error().
%%--------------------------------------------------------------------
list_buckets() -> list_buckets([]).

%%--------------------------------------------------------------------
-spec list_buckets(opts()) -> [bucket()] | error().
%%--------------------------------------------------------------------
list_buckets(Opts) ->
    case exec(#req{server = server(Opts)}) of
        {ok, _, Body} ->
            select([~"Buckets", {~"Bucket"}, ~"Name", child], Body);
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec delete_bucket(bucket()) -> ok | error().
%%--------------------------------------------------------------------
delete_bucket(Bucket) -> delete_bucket(Bucket, []).

%%--------------------------------------------------------------------
-spec delete_bucket(bucket(), opts()) -> ok | error().
%%--------------------------------------------------------------------
delete_bucket(Bucket, Opts) ->
    case exec(#req{method = delete, bucket = Bucket, server = server(Opts)}) of
        {ok, _, _} -> ok;
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec get_bucket_versioning(bucket()) -> versioning() | error().
%%--------------------------------------------------------------------
get_bucket_versioning(Bucket) -> get_bucket_versioning(Bucket, []).

%%--------------------------------------------------------------------
-spec get_bucket_versioning(bucket(), opts()) -> versioning() | error().
%%--------------------------------------------------------------------
get_bucket_versioning(Bucket, Opts) ->
    case exec(#req{bucket = Bucket, sub = ~"versioning",server=server(Opts)}) of
        {ok, _, Body} ->
            XML = case Body of
                      <<"<?xml version=", _/binary>> -> Body;
                      _ -> <<"<?xml version=\"1.0\"?>", Body/binary>>
                  end,
            case select([[~"Status", ~"MfaDelete"], child], XML) of
                #{~"Status" := S, ~"MfaDelete" := M} ->
                    #{status => low_atom(S), mfa_delete => low_atom(M)};
                #{~"Status" := S} -> #{status => low_atom(S)};
                #{~"MfaDelete" := M} -> #{mfa_delete => low_atom(M)};
                _ -> #{status => suspended}
            end;
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec put_bucket_versioning(bucket(), status()) -> versioning() | error().
%%--------------------------------------------------------------------
put_bucket_versioning(Bucket, Status) ->
    put_bucket_versioning(Bucket, Status, []).

%%--------------------------------------------------------------------
-spec put_bucket_versioning(bucket(),status(),opts()) -> versioning() | error().
%%--------------------------------------------------------------------
put_bucket_versioning(Bucket, Status, Opts) ->
    Status1 = case Status of
                  enabled -> ~"Enabled";
                  suspended -> ~"Suspended"
              end,
    XML = #xml{tag = ~"VersioningConfiguration",
               attrs = [{xmlns, ~"http://s3.amazonaws.com/doc/2006-03-01/"}],
               children = [#xml{tag = ~"Status", children = [Status1]}]},
    Req = #req{method = put,
               bucket = Bucket,
               sub = ~"versioning",
               object = jhn_s3c_xml:encode(XML, binary),
               server = server(Opts)},
    case exec(Req) of
        {ok, _, _} -> ok;
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec count_objects(bucket()) -> non_neg_integer() | error().
%%--------------------------------------------------------------------
count_objects(Bucket) -> count_objects(Bucket, 1000).

%%--------------------------------------------------------------------
-spec count_objects(bucket(), pos_integer()) -> non_neg_integer() | error().
%%--------------------------------------------------------------------
count_objects(Bucket, NoOfKeysAtTheTime) ->
    count_objects(Bucket, NoOfKeysAtTheTime, []).

%%--------------------------------------------------------------------
-spec count_objects(bucket(),pos_integer(),opts()) -> non_neg_integer()|error().
%%--------------------------------------------------------------------
count_objects(Bucket, NoOfKeysAtTheTime, Opts) ->
    count_objects(list_objects(Bucket, Opts), Bucket, NoOfKeysAtTheTime, 0).

%%--------------------------------------------------------------------
-spec put_object(bucket(), key(), value()) -> ok | error().
%%--------------------------------------------------------------------
put_object(Bucket, Key, Value) -> put_object(Bucket, Key, Value, []).

%%--------------------------------------------------------------------
-spec put_object(bucket(), key(), value(), opts()) -> ok | error().
%%--------------------------------------------------------------------
put_object(Bucket, Key, Value, Opts) ->
    Req = #req{method = put,
               bucket = Bucket,
               key = Key,
               object = Value,
               server = server(Opts)},
    case exec(Req) of
        {ok, _, _} -> ok;
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec list_objects(bucket()) ->
          [key()] | #{token := _, keys := [key()]} | error().
%%--------------------------------------------------------------------
list_objects(Bucket) -> list_objects(Bucket, []).

%%--------------------------------------------------------------------
-spec list_objects(bucket(), opts()) ->
          [key()] | #{token := _, keys := [key()]} | error().
%%--------------------------------------------------------------------
list_objects(Bucket, Opts) ->
    KVs = parse_opts(?FUNCTION_NAME, Opts),
    Req = #req{bucket = Bucket,
               kvs = [{~"list-type", ~"2"} | KVs],
               server = server(Opts)},
    case exec(Req) of
        {ok, _, Body} ->
            Keys = select([{~"Contents"}, ~"Key", child], Body),
            case select([~"IsTruncated", child], Body) of
                ~"false" -> Keys;
                ~"true" ->
                    #{token => select([~"NextContinuationToken", child], Body),
                      keys => Keys}
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
-spec list_object_versions(bucket()) ->
          [version_key()] |
          #{key_marker := _,
            version_id_marker := _,
            keys := [version_key()]} | error().
%%--------------------------------------------------------------------
list_object_versions(Bucket) -> list_object_versions(Bucket, []).

%%--------------------------------------------------------------------
-spec list_object_versions(bucket(), opts()) ->
          binary() |
          [version_key()] |
          #{key_marker := _,
            version_id_marker := _,
            keys := [version_key()]} |
          error().
%%--------------------------------------------------------------------
list_object_versions(Bucket, Opts) ->
    KVs = parse_opts(?FUNCTION_NAME, Opts),
    Req = #req{bucket = Bucket,sub = ~"versions",kvs = KVs,server=server(Opts)},
    case exec(Req) of
        {ok, _, Body} ->
            VersionKeys =
                [#{key => Key,
                   is_latest => binary_to_existing_atom(IsLatest),
                   version_id => VersionId} ||
                    #{~"Key" := Key,
                      ~"IsLatest" := IsLatest,
                      ~"VersionId" := VersionId} <-
                        select([{~"Version"},
                                [~"Key", ~"IsLatest", ~"VersionId"],
                                child],
                               Body)],
            case select([~"IsTruncated", child], Body) of
                ~"false" -> VersionKeys;
                ~"true" ->
                    #{key_marker => select([~"NextKeyMarker", child], Body),
                      version_id_marker =>
                          select([~"NextVersionIdMarker", child], Body),
                      keys => VersionKeys}
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
-spec get_object(bucket(), key()) -> value() | error().
%%--------------------------------------------------------------------
get_object(Bucket, Key) -> get_object(Bucket, Key, []).

%%--------------------------------------------------------------------
-spec get_object(bucket(), key(), opts()) -> value() | error().
%%--------------------------------------------------------------------
get_object(Bucket, Key, Opts) ->
    case exec(#req{bucket = Bucket, key = Key, server = server(Opts)}) of
        {ok, _, Body} -> Body;
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec head_object(bucket(), key()) -> [{binary(), binary()}] | error().
%%--------------------------------------------------------------------
head_object(Bucket, Key) -> head_object(Bucket, Key, []).

%%--------------------------------------------------------------------
-spec head_object(bucket(), key(), opts()) -> [{binary(), binary()}] | error().
%%--------------------------------------------------------------------
head_object(Bucket, Key, Opts) ->
    Req = #req{method = head, bucket = Bucket, key = Key,server = server(Opts)},
    case exec(Req) of
        {ok, Headers, _} ->
            [{jhn_bstring:to_lower(H), V} || {H, V} <- Headers];
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
-spec delete_object(bucket(), key()) -> ok | error().
%%--------------------------------------------------------------------
delete_object(Bucket, Key) -> delete_object(Bucket, Key, []).

%%--------------------------------------------------------------------
-spec delete_object(bucket(), key(), opts()) -> ok | error().
%%--------------------------------------------------------------------
%%
%% N.B. specifying version_id does not currently work, use delete_objects/2
%%      instead.
%%
delete_object(Bucket, Key, Opts) ->
    Sub = case parse_opts(?FUNCTION_NAME, Opts) of
              [] -> ~"";
              [{<<"versionId">>, Id}] -> <<"VersionId=", Id/binary>>
          end,
    Req = #req{method = delete,
               bucket = Bucket,
               key = Key,
               sub = Sub,
               server = server(Opts)},
    case exec(Req) of
        {ok, _, _} -> ok;
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec delete_objects(bucket(), [key() | version_key()]) -> ok | error().
%%--------------------------------------------------------------------
delete_objects(Bucket, Keys) -> delete_objects(Bucket, Keys, []).

%%--------------------------------------------------------------------
-spec delete_objects(bucket(), [key() | version_key()], opts()) -> ok | error().
%%--------------------------------------------------------------------
delete_objects(_, [], _) -> ok;
delete_objects(Bucket, Keys, Opts) ->
    RegularKeys =
        [#xml{tag = ~"Object",
              children = [#xml{tag = ~"Key", children = [Key]}]} ||
            Key <- Keys, is_binary(Key)],
    VersionKeys =
        [#xml{tag = ~"Object",
              children = [#xml{tag = ~"Key", children = [Key]},
                          #xml{tag = ~"VersionId", children = [VersionId]}]} ||
            #{key := Key, version_id := VersionId} <- Keys],
    XML = #xml{tag = ~"Delete",
               attrs = [{xmlns, ~"http://s3.amazonaws.com/doc/2006-03-01/"}],
               children = RegularKeys ++ VersionKeys},
    Body = jhn_s3c_xml:encode(XML, binary),
    Req = #req{method = post,
               bucket = Bucket,
               sub = ~"delete",
               object = Body,
               server = server(Opts)},
    case exec(Req) of
        {ok, _, _} -> ok;
        Error -> Error
    end.

%% ===================================================================
%% Internal functions.
%% ===================================================================

server(Opts) -> jhn_plist:find(server, Opts, 'DEFAULT').

count_objects(#{token := T}, Bucket, Step, Acc) ->
    Opts = [{token, T}, {max_keys, Step}],
    count_objects(list_objects(Bucket, Opts), Bucket, Step, Acc + Step);
count_objects([], _, Acc, _) -> Acc;
count_objects(List = [_|_], _, _, Acc) ->
    Acc + length(List);
count_objects(Error, _, _, _) ->
    Error.

parse_opts(Function, Opts) ->
    jhn_plist:delete(server, [parse_opt(Function, Opt) || Opt <- Opts]).

parse_opt(_, {server, Server}) -> {server, Server};
parse_opt(_, {max_keys, N}) when is_integer(N) ->
    {~"max-keys", integer_to_binary(N)};
parse_opt(list_objects, {token, Token}) ->
    {~"continuation-token", Token};
parse_opt(list_object_versions, {key_marker, KeyMarker}) ->
    {~"key-marker", KeyMarker};
parse_opt(list_object_versions, {version_id, VersionIdMarker}) ->
    {~"version-id-marker", VersionIdMarker};
parse_opt(delete_object, {version_id, VersionsId}) ->
    {~"versionId", VersionsId}.


exec(Req) ->
    {State, Max, Count} = state(Req),
    try do_exec(State, Max, 0, Count + 1, 0)
    catch C:R:BT -> {error, {C, R, BT}}
    end.

do_exec(State, Max, Tries, Count, Closed) ->
    #state{method = M, uri = URI, headers = Headers, object = Object,
           hackney_opts = Opts} = State,
    Opts1 = case proplists:is_defined(with_body, Opts) of
                true -> Opts;
                false -> [with_body | Opts]
            end,
    case result(hackney:request(M, URI, Headers, Object, Opts1), Max, Tries) of
        retry ->
            timer:sleep((49 + rand:uniform(51)) * Tries),
            do_exec(State, Max, Tries + 1, Count, Closed);
        closed when Closed > Count ->
            {error, closed};
        closed ->
            do_exec(State, Max, Tries, Count, Closed + 1);
        Result ->
            Result
    end.

state(Req) ->
    #req{method = M,
         bucket = B,
         key = K,
         sub = Sub,
         object = O,
         kvs = KVs,
         server = Server} = Req,
    #config{request_type = ReqType,
            protocol = Proto, host = Host, port = Port,
            hackney_opts = Opts, max_tries = Max,
            access_key_id = Id,
            access_key = SecretKey} = jhn_s3c_config:get(Server),
    {_, Pool} = proplists:lookup(pool, Opts),
    Count = hackney_pool:count(Pool),
    Method = normalize(M),
    {ContentType, MD5} =
        case O of
            <<>> -> {~"", ~""};
            <<"<?xml version=\"1.0\"?>", _/binary>> ->
                {~"application/xml", base64:encode(erlang:md5(O))};
            _ ->
                {~"application/octet_stream", base64:encode(erlang:md5(O))}
        end,
    Date = jhn_timestamp:gen([binary, rfc7231]),
    SPath = make_url(~"", ~"", ~"", sign_path(B, K), Sub),
    Auth = aws_auth(Method, MD5, ContentType, Date, SPath, Id, SecretKey),
    Headers = [{~"Date", Date},
               {~"Authorization", Auth},
               {~"Content-Type", ContentType}],
    Headers1 = case MD5 of
                   ~"" -> Headers;
                   _ -> [{~"Content-MD5", MD5} | Headers]
               end,
    Headers2 = case {ReqType, B} of
                   {path, _} -> [{~"Host", Host} | Headers1];
                   {_, ~""} -> [{~"Host", Host} | Headers1];
                   _ -> [{~"Host", <<B/binary, ".", Host/binary>>} | Headers1]
               end,
    KVs1 = case Sub of
               ~"" -> KVs;
               _ -> Sub
           end,
    Path = case {ReqType, B} of
              {virtual_host, _} -> [K];
              {path, ~""} -> [K];
              {path, _} -> [B, K]
          end,
    URI = make_url(Proto, Host, Port, Path, KVs1),
    State = #state{method = M,
                   uri = URI,
                   headers = Headers2,
                   object = O,
                   hackney_opts = Opts},
    {State, Max, Count}.

normalize(get) -> ~"GET";
normalize(head) -> ~"HEAD";
normalize(post) -> ~"POST";
normalize(put) -> ~"PUT";
normalize(delete) -> ~"DELETE".

sign_path(~"", ~"") -> [~""];
sign_path(Bucket, Key) -> [Bucket, Key].

make_url(Protocol, Host, Port, Path, Query) ->
    Set = fun({_, ~""}, M) -> M;
             ({K, V}, M) -> M#{K => V}
          end,
    URI = lists:foldl(Set,
                      #{path => [$/, lists:join($/, Path)]},
                      [{host, Host}, {scheme, Protocol}, {port, Port}]),
    URI1 = case Query of
               ~"" -> URI;
               [] -> URI;
               _ when is_binary(Query) -> URI#{query => Query};
               _ -> URI#{query => uri_string:compose_query(Query)}
           end,
    uri_string:normalize(URI1).

aws_auth(Method, MD5, ContentType, Date, Path, Id, SecretKey) ->
    StringToSign = [Method, $\n, MD5, $\n, ContentType, $\n, Date, $\n, Path],
    Signature = base64:encode(crypto:mac(hmac, sha, SecretKey, StringToSign)),
    iolist_to_binary(["AWS ", Id, $:, Signature]).

result({ok, S, Headers}, _, _) when ?SUCCESS(S) -> {ok, Headers, ~""};
result({ok, S, Headers, Body}, _, _) when ?SUCCESS(S) -> {ok, Headers, Body};
result({ok, 429, _}, Max, Try) when Try < Max -> retry;
result({ok, 429, _, _}, Max, Try) when Try < Max -> retry;
%% This is to deal with scality S3
result({ok, 500, Headers, Body}, Max, Try) when Try < Max ->
    case binary:match(Body, ~"Please try again.") of
        nomatch -> {error, {500, Headers, Body}};
        _ -> retry
    end;
result({ok, Status, Headers}, _, _) -> {error, {Status, Headers, ~""}};
result({ok, Status, Headers, Body}, _, _) -> {error, {Status, Headers, Body}};
result({error, closed}, _, _) -> closed;
result({error, timeout}, Max, Try) when Try < Max -> retry;
result(Error = {error, _}, _, _) -> Error.

select(Pick, XML) -> jhn_s3c_xml:select(Pick, jhn_s3c_xml:decode(XML)).

low_atom(S) -> binary_to_existing_atom(jhn_bstring:to_lower(S)).
