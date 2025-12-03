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
-module(jhn_s3c).
-copyright('Jan Henry Nystrom <JanHenryNystrom@gmail.com>').

%% API functions
-export([%% Bucket
         create_bucket/1, list_buckets/0, delete_bucket/1,
         get_bucket_versioning/1, put_bucket_versioning/2,
         %% Object
         count_objects/1, count_objects/2,
         put_object/3,
         list_objects/1, list_objects/2,
         list_object_versions/1, list_object_versions/2,
         get_object/2, head_object/2,
         delete_object/2,  delete_object/3, delete_objects/2]).

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

-type opt()           :: {max_keys, integer()} | {token, binary()} |
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
-record(req, {method  = get  :: method(),
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
create_bucket(Bucket) ->
    case exec(#req{method = put, bucket = Bucket}) of
        {ok, _, _} -> ok;
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec list_buckets() -> [bucket()] | error().
%%--------------------------------------------------------------------
list_buckets() ->
    case exec(#req{}) of
        {ok, _, Body} ->
            select([~"Buckets", {~"Bucket"}, ~"Name", child], Body);
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec delete_bucket(bucket()) -> ok | error().
%%--------------------------------------------------------------------
delete_bucket(Bucket) ->
    case exec(#req{method = delete, bucket = Bucket}) of
        {ok, _, _} -> ok;
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec get_bucket_versioning(bucket()) -> versioning() | error().
%%--------------------------------------------------------------------
get_bucket_versioning(Bucket) ->
    case exec(#req{bucket = Bucket, sub = ~"versioning"}) of
        {ok, _, Body} ->
            XML = <<"<?xml version=\"1.0\"?>", Body/binary>>,
            case select([[~"Status", ~"MfaDelete"]], XML) of
                [] -> #{status => suspended};
                Children ->
                    #{low_atom(Tag) => low_atom(Val) ||
                        #xml{tag = Tag, children = [Val]} <- Children}
            end;
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec put_bucket_versioning(bucket(), status()) -> versioning() | error().
%%--------------------------------------------------------------------
put_bucket_versioning(Bucket, Status) ->
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
               object = jhn_s3c_xml:encode(XML, binary)},
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
    count_objects(list_objects(Bucket), Bucket, NoOfKeysAtTheTime, 0).

%%--------------------------------------------------------------------
-spec put_object(bucket(), key(), value()) -> ok | error().
%%--------------------------------------------------------------------
put_object(Bucket, Key, Value) ->
    case exec(#req{method = put, bucket = Bucket, key = Key, object = Value}) of
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
    case exec(#req{bucket = Bucket, kvs = [{~"list-type", ~"2"} | KVs]}) of
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
          [version_key()] |
          #{key_marker := _,
            version_id_marker := _,
            keys := [version_key()]} | error().
%%--------------------------------------------------------------------
list_object_versions(Bucket, Opts) ->
    KVs = parse_opts(?FUNCTION_NAME, Opts),
    case exec(#req{bucket = Bucket, sub = ~"versions", kvs = KVs}) of
        {ok, _, Body} ->
            VersionKeys =
                [#{key => Key,
                   is_latest => binary_to_existing_atom(IsLatest),
                   version_id => VersionId} ||
                    [Key, IsLatest, VersionId] <-
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
get_object(Bucket, Key) ->
    case exec(#req{bucket = Bucket, key = Key}) of
        {ok, _, Body} -> Body;
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec head_object(bucket(), key()) -> [{binary(), binary()}] | error().
%%--------------------------------------------------------------------
head_object(Bucket, Key) ->
    case exec(#req{method = head, bucket = Bucket, key = Key}) of
        {ok, Headers, _} ->
            [{hackney_bstr:to_lower(H), V} || {H, V} <- Headers];
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
delete_object(Bucket, Key, Opts) ->
    Sub = case parse_opts(?FUNCTION_NAME, Opts) of
              [] -> ~"";
              [{<<"versionId">>, Id}] -> <<"VersionId=", Id/binary>>
          end,
    case exec(#req{method = delete, bucket = Bucket, key = Key, sub = Sub}) of
        {ok, _, _} -> ok;
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec delete_objects(bucket(), [key() | version_key()]) -> ok | error().
%%--------------------------------------------------------------------
delete_objects(_, []) -> ok;
delete_objects(Bucket, Keys) ->
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
    Req = #req{method = post, bucket = Bucket, sub = ~"delete", object = Body},
    case exec(Req) of
        {ok, _, _} -> ok;
        Error -> Error
    end.

%% ===================================================================
%% Internal functions.
%% ===================================================================

count_objects(#{token := T}, Bucket, Step, Acc) ->
    Opts = [{token, T}, {max_keys, Step}],
    count_objects(list_objects(Bucket, Opts), Bucket, Step, Acc + Step);
count_objects([], _, Acc, _) -> Acc;
count_objects(List = [_|_], _, _, Acc) ->
    Acc + length(List);
count_objects(Error, _, _, _) ->
    Error.

parse_opts(Function, Opts) -> [parse_opt(Function, Opt) || Opt <- Opts].

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
    case result(hackney:request(M, URI, Headers, Object, Opts), Max, Tries) of
        retry ->
            timer:sleep((49 + rand:uniform(51)) * Tries, Count, Closed),
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
         bucket = B, key = K, sub = Sub, object = O, kvs = KVs} = Req,
    #config{request_type = ReqType,
            protocol = Proto, host = Host, port = Port,
            hackney_opts = Opts, max_tries = Max,
            access_key_id = Id, access_key = SecretKey} = jhn_s3c_config:get(),
    {_, Pool} = proplists:lookup(pool, Opts),
    Count = hackney_pool:count(Pool),
    URL = <<Proto/binary, "://", Host/binary, ":", Port/binary>>,
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
    SPath = hackney_url:make_url(~"", sign_path(B, K), Sub),
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
    URI = case {ReqType, B} of
              {virtual_host, _} -> hackney_url:make_url(URL, [K], KVs1);
              {path, ~""} -> hackney_url:make_url(URL, [K], KVs1);
              {path, _} -> hackney_url:make_url(URL, [B, K], KVs1)
          end,
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
