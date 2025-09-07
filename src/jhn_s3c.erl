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
         %% Object
         put_object/3,
         list_objects/1,
         get_object/2, head_object/2,
         delete_object/2, delete_objects/2]).

%% Includes
-include_lib("jhn_s3c/src/jhn_s3c.hrl").
-include_lib("jhn_s3c/src/jhn_s3c_xml.hrl").
-include_lib("kernel/include/logger.hrl").

%% Exported types
-export_type([bucket/0, key/0, value/0, error/0]).

%% Types
-type bucket()       :: binary().
-type key()          :: binary().
-type sub_resource() :: binary().
-type value()        :: iodata().

-type hackney_error() :: _.
-type http_error()    :: {status(), headers(), body()}.
-type exception()     :: {class(), reason(),  [tuple()]}.
-type class()         :: exit | error | throw.
-type reason()        :: atom() | {atom(), _}.
-type error()         :: {error, http_error() | exception() | hackney_error()}.
-type status()        :: integer().
-type headers()       :: [{binary(), binary()}].
-type body()          :: binary().

-type method() :: head | get | put | post | delete.

%% Defines
-define(SUCCESS(Status), Status >= 200, Status =< 299).

%% Records
-record(req, {method         :: method(),
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
                hackney_opts :: [{_, _}]}).

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
    case exec(#req{method = get}) of
        {ok, _, Body} ->
            jhn_s3c_xml:select([~"Buckets", [~"Bucket"], ~"Name", child],
                               jhn_s3c_xml:decode(Body));
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
-spec put_object(bucket(), key(), value()) -> ok | error().
%%--------------------------------------------------------------------
put_object(Bucket, Key, Value) ->
    Req = #req{method = put, bucket = Bucket, key = Key, object = Value},
    case exec(Req) of
        {ok, _, _} -> ok;
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec list_objects(bucket()) -> [key()] | error().
%%--------------------------------------------------------------------
list_objects(Bucket) ->
    Req = #req{method = get, bucket = Bucket, kvs = [{~"list-type", ~"2"}]},
    case exec(Req) of
        {ok, _, Body} ->
            jhn_s3c_xml:select([[~"Contents"], ~"Key", child],
                               jhn_s3c_xml:decode(Body));
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec get_object(bucket(), key()) -> value() | error().
%%--------------------------------------------------------------------
get_object(Bucket, Key) ->
    case exec(#req{method = get, bucket = Bucket, key = Key}) of
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
delete_object(Bucket, Key) ->
    case exec(#req{method = delete, bucket = Bucket, key = Key}) of
        {ok, _, _} -> ok;
        Error -> Error
    end.

%%--------------------------------------------------------------------
-spec delete_objects(bucket(), [key()]) -> ok | error().
%%--------------------------------------------------------------------
delete_objects(Bucket, Keys) ->
    XML = #xml{tag = ~"Delete",
               attrs = [{xmlns, ~"http://s3.amazonaws.com/doc/2006-03-01/"}],
               children =
                   [#xml{tag = ~"Object",
                         children = [#xml{tag = ~"Key",
                                          children = [Key]}]} || Key <- Keys]},
    Body = jhn_s3c_xml:encode(XML, binary),
    Req = #req{method = post, bucket = Bucket, sub = ~"delete", object = Body},
    case exec(Req) of
        {ok, _, _} -> ok;
        Error -> Error
    end.

%% ===================================================================
%% Inernal functions.
%% ===================================================================

exec(Req) ->
    {State, Max} = state(Req),
    try do_exec(State, Max, 0)
    catch C:R:BT -> {error, {C, R, BT}}
    end.

do_exec(State, Max, Tries) ->
    #state{method = M, uri = URI, headers = Headers, object = Object,
           hackney_opts = Opts} = State,
    case result(hackney:request(M, URI, Headers, Object, Opts), Max, Tries) of
        retry ->
            timer:sleep((49 + rand:uniform(51)) * Tries),
            do_exec(State, Max, Tries + 1);
        Result ->
            Result
    end.

state(Req) ->
    #req{method = M,
         bucket = B, key = K, sub = Sub, object = O, kvs = KVs} = Req,
    #config{protocol = Proto, host = Host, port = Port,
            hackney_opts = Opts, max_tries = Max,
            access_key_id = Id, access_key = SecretKey} = jhn_s3c_app:config(),
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
    Headers2 = case B of
                   ~"" -> [{~"Host", Host} | Headers1];
                   _ -> [{~"Host", <<B/binary, ".", Host/binary>>} | Headers1]
               end,
    KVs1 = case Sub of
               ~"" -> KVs;
               _ -> Sub
           end,
    State = #state{method = M,
                   uri = hackney_url:make_url(URL, [K], KVs1),
                   headers = Headers2,
                   object = O,
                   hackney_opts = Opts},
    {State, Max}.

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
result({error, closed}, Max, Try) when Try < Max -> retry;
result({error, timeout}, Max, Try) when Try < Max -> retry;
result(Error = {error, _}, _, _) -> Error.
