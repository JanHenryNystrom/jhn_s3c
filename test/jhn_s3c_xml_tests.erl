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
%%%   eunit unit tests for the S3C XML library module.
%%% @end
%%%
%% @author Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%% @copyright (C) 2025, Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%%%-------------------------------------------------------------------
-module(jhn_s3c_xml_tests).
-copyright('Jan Henry Nystrom <JanHenryNystrom@gmail.com>').

%% Includes
-include_lib("eunit/include/eunit.hrl").
-include_lib("jhn_s3c/src/jhn_s3c_xml.hrl").

%%--------
%% Defines
%%--------

%% Escape
-define(ESCAPE_XML,
        #xml{tag = <<"test">>,
             attrs = [{<<"attr">>, <<"\"'<&>'\"">>}],
             children = [<<"Ha \"'<&>'\"">>]}).

%% Presence
-define(PRESENCE_SUBSCRIBE_XML,
        #xml{tag = <<"presence">>,
             attrs = [{<<"from">>, <<"alice@wonderland.lit">>},
                      {<<"to">>, <<"sister@realworld.lit">>},
                      {<<"type">>, <<"subscribe">>}
                     ]}).

-define(PRESENCE_SUBSCRIBED_XML,
        #xml{tag = <<"presence">>,
             attrs = [{<<"from">>, <<"sister@realworld.lit">>},
                      {<<"to">>, <<"alice@wonderland.lit">>},
                      {<<"type">>, <<"subscribed">>}
                     ]}).

-define(PRESENCE_NOTIFICATION_XML,
        #xml{tag = <<"presence">>,
             attrs = [{<<"from">>, <<"alice@wonderland.lit/rabbithole">>},
                      {<<"to">>, <<"sister@realworld.lit">>}]}).

-define(PRESENCE_PROBE_XML,
        #xml{tag = <<"presence">>,
             attrs = [{<<"from">>, <<"alice@wonderland.lit/rabbithole">>},
                      {<<"to">>, <<"sister@realworld.lit">>},
                      {<<"type">>, <<"probe">>}]}).

%%
%% "Delayed Delivery" XEP-0203
%%
-define(PRESENCE_DELAY_XML,
        #xml{tag = <<"presence">>,
             attrs = [{<<"from">>, <<"sister@realworld.lit/home">>},
                      {<<"to">>, <<"alice@wonderland.lit/rabbithole">>},
                      {<<"type">>, <<"unavailable">>}],
             children = [#xml{tag = <<"delay">>,
                              attrs = [{<<"xmlns">>, <<"urn:xmpp:delay">>},
                                       {<<"stamp">>,<<"2008-11-26T15:59:09Z">>}]
                             }
                        ]
            }).

-define(PRESENCE_SHOW_XML,
        #xml{tag = <<"presence">>,
             attrs = [{<<"from">>, <<"alice@wonderland.lit/rabbithole">>},
                      {<<"to">>, <<"sister@realworld.lit">>}],
             children = [#xml{tag = <<"show">>,
                              children = [<<"xa">>]},
                         #xml{tag = <<"status">>,
                              children = [<<"down the rabbit hole!">>]}
                        ]}).

%% Escape

-define(ESCAPE_BIN,
        <<"<?xml version=\"1.0\"?>"
          "<test attr='&quot;&apos;&lt;&amp;&gt;&apos;&quot;'>"
          "Ha &quot;&apos;&lt;&amp;&gt;&apos;&quot;"
          "</test>">>).

%% Presence
-define(PRESENCE_SUBSCRIBE_BIN,
        <<"<?xml version=\"1.0\"?>"
          "<presence from='alice@wonderland.lit' "
                     "to='sister@realworld.lit' "
                     "type='subscribe'/>">>).

-define(PRESENCE_SUBSCRIBED_BIN,
        <<"<?xml version=\"1.0\"?>"
          "<presence from='sister@realworld.lit' "
                     "to='alice@wonderland.lit' "
                     "type='subscribed'/>">>).

-define(PRESENCE_NOTIFICATION_BIN,
        <<"<?xml version=\"1.0\"?>"
          "<presence from='alice@wonderland.lit/rabbithole' "
                    "to='sister@realworld.lit'/>">>).

-define(PRESENCE_PROBE_BIN,
        <<"<?xml version=\"1.0\"?>"
          "<presence from='alice@wonderland.lit/rabbithole' "
                    "to='sister@realworld.lit' "
                    "type='probe'/>">>).

%%
%% "Delayed Delivery" XEP-0203
%%
-define(PRESENCE_DELAY_BIN,
        <<"<?xml version=\"1.0\"?>"
          "<presence from='sister@realworld.lit/home' "
                    "to='alice@wonderland.lit/rabbithole' "
                    "type='unavailable'>"
            "<delay xmlns='urn:xmpp:delay' "
                   "stamp='2008-11-26T15:59:09Z'/>"
          "</presence>">>).

-define(PRESENCE_SHOW_BIN,
        <<"<?xml version=\"1.0\"?>"
          "<presence from='alice@wonderland.lit/rabbithole' "
                    "to='sister@realworld.lit'>"
            "<show>xa</show>"
            "<status>down the rabbit hole!</status>"
          "</presence>">>).

%% ===================================================================
%% Tests.
%% ===================================================================

%% ===================================================================
%% Encoding
%% ===================================================================

%%--------------------------------------------------------------------
%% Escape
%%--------------------------------------------------------------------

encode_escape_test_() ->
    [{"Simple",
      ?_test(
         ?assertEqual(
            ?ESCAPE_BIN,
            iolist_to_binary(jhn_s3c_xml:encode(?ESCAPE_XML))))}
    ].

%%--------------------------------------------------------------------
%% Presence
%%--------------------------------------------------------------------
encode_presence_test_() ->
    [{"Subscribe",
      ?_test(
         ?assertEqual(
            ?PRESENCE_SUBSCRIBE_BIN,
            iolist_to_binary(jhn_s3c_xml:encode(?PRESENCE_SUBSCRIBE_XML))))},
     {"Subscribed",
      ?_test(
         ?assertEqual(
            ?PRESENCE_SUBSCRIBED_BIN,
            iolist_to_binary(jhn_s3c_xml:encode(?PRESENCE_SUBSCRIBED_XML))))},
     {"Notification",
      ?_test(
         ?assertEqual(
            ?PRESENCE_NOTIFICATION_BIN,
            iolist_to_binary(jhn_s3c_xml:encode(?PRESENCE_NOTIFICATION_XML))))},
     {"Probe",
      ?_test(
         ?assertEqual(
            ?PRESENCE_PROBE_BIN,
            iolist_to_binary(jhn_s3c_xml:encode(?PRESENCE_PROBE_XML))))},
     {"Delay",
      ?_test(
         ?assertEqual(
            ?PRESENCE_DELAY_BIN,
            iolist_to_binary(jhn_s3c_xml:encode(?PRESENCE_DELAY_XML))))},
     {"Show",
      ?_test(
         ?assertEqual(
            ?PRESENCE_SHOW_BIN,
            iolist_to_binary(jhn_s3c_xml:encode(?PRESENCE_SHOW_XML))))}
    ].

%% ===================================================================
%% Decoding
%% ===================================================================

%%--------------------------------------------------------------------
%% Escape
%%--------------------------------------------------------------------
decode_escape_test_() ->
    [{"Simple", ?_test(equal(?ESCAPE_XML, jhn_s3c_xml:decode(?ESCAPE_BIN)))}
    ].

%%--------------------------------------------------------------------
%% Presence
%%--------------------------------------------------------------------
decode_presence_test_() ->
    [{"Subscribe",
      ?_test(equal(?PRESENCE_SUBSCRIBE_XML,
                    jhn_s3c_xml:decode(?PRESENCE_SUBSCRIBE_BIN)))},
     {"Subscribed",
      ?_test(equal(?PRESENCE_SUBSCRIBED_XML,
                   jhn_s3c_xml:decode(?PRESENCE_SUBSCRIBED_BIN)))},
     {"Notification",
      ?_test(equal(?PRESENCE_NOTIFICATION_XML,
                   jhn_s3c_xml:decode(?PRESENCE_NOTIFICATION_BIN)))},
     {"Probe",
      ?_test(equal(?PRESENCE_PROBE_XML,
                   jhn_s3c_xml:decode(?PRESENCE_PROBE_BIN)))},
     {"Delay",
      ?_test(equal(?PRESENCE_DELAY_XML,
                   jhn_s3c_xml:decode(?PRESENCE_DELAY_BIN)))},
     {"Show",
      ?_test(equal(?PRESENCE_SHOW_XML,
                    jhn_s3c_xml:decode(?PRESENCE_SHOW_BIN)))}
    ].

%% ===================================================================
%% Common functions.
%% ===================================================================

equal(#xml{tag = Tag, attrs = Attrs1, children = Children1},
      #xml{tag = Tag, attrs = Attrs2, children = Children2}) ->
    equal_attrs(lists:sort(Attrs1), lists:sort(Attrs2)),
    equal_children(Children1, Children2);
equal(#xml{tag = Tag1}, #xml{tag = Tag2}) ->
    erlang:error({different_tags, Tag1, Tag2});
equal(Text, Text) when is_binary(Text) ->
    true.

equal_attrs([], []) -> true;
equal_attrs([], Attrs) -> erlang:error({missing_atributes, Attrs});
equal_attrs(Attrs, []) -> erlang:error({excessive_atributes, Attrs});
equal_attrs([H1 | T1], [H2 | T2]) ->
    equal_attr(H1, H2),
    equal_attrs(T1, T2).

equal_attr({N, V}, Attr) when is_atom(N) ->
    equal_attr({atom_to_binary(N, utf8), V}, Attr);
equal_attr(Attr, {N, V}) when is_atom(N) ->
    equal_attr(Attr, {atom_to_binary(N, utf8), V});
equal_attr({N, V}, {N, V}) ->
    true;
equal_attr({N, V1}, {N, V2}) ->
    erlang:error({attribute_values, V1, V2});
equal_attr({N1, V}, {N2, V}) ->
    erlang:error({missaligned_attribute, N1, N2});
equal_attr({N1, V1}, {N2, V2}) ->
    erlang:error({missaligned_attribute, {N1, V1}, {N2, V2}}).

equal_children([], []) -> true;
equal_children([], Children) -> erlang:error({missing_children, Children});
equal_children(Children, []) -> erlang:error({excessive_children, Children});
equal_children([H1 |T1], [H2 | T2]) ->
    equal(H1, H2),
    equal_children(T1, T2).
