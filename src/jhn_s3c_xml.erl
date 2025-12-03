%==============================================================================
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
%%%   The xml encoding/decoding for S3C.
%%% @end
%%%
%% @author Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%% @copyright (C) 2025, Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%%%-------------------------------------------------------------------
-module(jhn_s3c_xml).
-copyright('Jan Henry Nystrom <JanHenryNystrom@gmail.com>').

%% Library functions
-export([encode/1, encode/2,
         decode/1,
         select/2
        ]).

%% Includes
-include_lib("jhn_s3c/src/jhn_s3c_xml.hrl").

%% Exported Types
-export_types([]).

%% Records
-record(state, {current = undefined :: undefined | xml() | cdata() | binary(),
                stack   = [] :: [xml()]
               }).

%% Types

%% Defines
-define(START_CHAR(C),
            C =:= 16#3A; %% :
            C >= 16#41, C =< 16#5A; %% A-Z
            C =:= 16#5F; %% _
            C >= 16#61, C =< 16#7A; %% a-z
            C >= 16#C0, C =< 16#D6; %% À-Ö
            C >= 16#D8, C =< 16#F6; %% Ø-ö
            C >= 16#F8, C =< 16#2FF; %% ø-
            C >= 16#370, C =< 16#37D;
            C >= 16#37F, C =< 16#1FFF;
            C >= 16#200C, C =< 16#200D;
            C >= 16#2070, C =< 16#218F;
            C >= 16#2C00, C =< 16#2FEF;
            C >= 16#3001, C =< 16#D7FF;
            C >= 16#F900, C =< 16#FDCF;
            C >= 16#FDF0, C =< 16#FFFD;
            C >= 16#10000, C =< 16#EFFFF).

-define(NAME_CHAR(C),
            C >= 16#2D, C =< 16#2E; %% --.
            C >= 16#30, C =< 16#39; %% 0-9
            C =:= 16#3A; %% :
            C >= 16#41, C =< 16#5A; %% A-Z
            C =:= 16#5F; %% _
            C >= 16#61, C =< 16#7A; %% a-z
            C =:= 16#B7; %% ·
            C >= 16#C0, C =< 16#D6; %% À-Ö
            C >= 16#D8, C =< 16#F6; %% Ø-ö
            C >= 16#F8, C =< 16#2FF; %% ø-
            C >= 16#300, C =< 16#37D;
            C >= 16#37F, C =< 16#1FFF;
            C >= 16#200C, C =< 16#200D;
            C >= 16#203F, C =< 16#2040;
            C >= 16#2070, C =< 16#218F;
            C >= 16#2C00, C =< 16#2FEF;
            C >= 16#3001, C =< 16#D7FF;
            C >= 16#F900, C =< 16#FDCF;
            C >= 16#FDF0, C =< 16#FFFD;
            C >= 16#10000, C =< 16#EFFFF).

-define(CHAR(C),
        C >= 16#9, C =< 16#A;
        C =:= 16#D;
        C >= 16#20, C =< 16#D7FF;
        C >= 16#E000, C =< 16#FFFD;
        C >= 16#10000, C =< 16#10FFFF).

-define(WS(C), C =< $\s).

-define(DECL, ~"<?xml version=\"1.0\"?>").

%% ===================================================================
%% Library functions.
%% ===================================================================

%%--------------------------------------------------------------------
%% Function: encode() -> .
%% @doc
%%   
%% @end
%%--------------------------------------------------------------------
-spec encode(xml()) -> iodata().
%%--------------------------------------------------------------------
encode(XML) -> encode(XML, iolist).

%%--------------------------------------------------------------------
%% Function: encode() -> .
%% @doc
%%   
%% @end
%%--------------------------------------------------------------------
-spec encode(xml(), iolist | binary) -> iodata().
%%--------------------------------------------------------------------
encode(XML, Return) ->
    case Return of
        iolist -> [?DECL, do_encode(XML)];
        binary -> iolist_to_binary([?DECL, do_encode(XML)])
    end.

%%--------------------------------------------------------------------
%% Function: decode() -> .
%% @doc
%%   
%% @end
%%--------------------------------------------------------------------
-spec decode(binary()) -> xml() | {error, _}.
%%--------------------------------------------------------------------
decode(Binary) ->
    try do_decode(Binary)
    catch _:Error:BT -> {error, Error, BT}
    end.

%%--------------------------------------------------------------------
%% Function: decode() -> .
%% @doc
%%   
%% @end
%%--------------------------------------------------------------------
-spec select(_, #xml{}) -> binary() | [binary()] | {error, _}.
%%--------------------------------------------------------------------
select(Selection, XML) ->
    try do_select(Selection, XML)
    catch _:Error:BT -> {error, Error, BT}
    end.

%% ===================================================================
%% Internal functions.
%% ===================================================================

%% ===================================================================
%% Encoding
%% ===================================================================

do_encode(Text) when is_binary(Text) -> escape(Text);
do_encode(#cdata{data = <<>>}) -> [];
do_encode(#cdata{data = Data}) -> [<<"<![CDATA[">>, Data, <<"]]>">>];
do_encode(#xml{tag = Tag, attrs = Attrs, children = []}) ->
    [<<"<">>, encode_tag(Tag), encode_attrs(Attrs), <<"/>">>];
do_encode(#xml{tag = Tag, attrs = Attrs, children = Children}) ->
    Tag1 = encode_tag(Tag),
    [<<"<">>, Tag1, encode_attrs(Attrs), <<">">>,
     [do_encode(Child) || Child <- Children],
     <<"</">>, Tag1, <<">">>].

encode_attrs([]) -> <<>>;
encode_attrs(Attrs) -> [encode_attr(Attr) || Attr <- Attrs].

encode_attr({_, undefined}) -> [];
encode_attr({Name, Value}) when is_atom(Name) ->
    encode_attr({atom_to_binary(Name, utf8), Value});
encode_attr({Name, Value}) when is_atom(Value) ->
    encode_attr({Name, atom_to_binary(Value, utf8)});
encode_attr({Name, Value}) ->
    [<<"\s">>, Name, <<"='">>, escape(Value), <<"'">>].

encode_tag(Tag) when is_atom(Tag) -> atom_to_binary(Tag, utf8);
encode_tag(Tag) -> Tag.

escape(Value) ->
    case needs_escaping(Value) of
        true -> escape(Value, <<>>);
        false -> Value
    end.

needs_escaping(<<>>) -> false;
needs_escaping(<<$<, _/binary>>) -> true;
needs_escaping(<<$>, _/binary>>) -> true;
needs_escaping(<<$", _/binary>>) -> true;
needs_escaping(<<$', _/binary>>) -> true;
needs_escaping(<<$&, _/binary>>) -> true;
needs_escaping(<<_, T/binary>>) -> needs_escaping(T).

escape(<<>>, Acc) -> Acc;
escape(<<$<, T/binary>>, Acc) -> escape(T, <<Acc/binary, "&lt;">>);
escape(<<$>, T/binary>>, Acc) -> escape(T, <<Acc/binary, "&gt;">>);
escape(<<$", T/binary>>, Acc) -> escape(T, <<Acc/binary, "&quot;">>);
escape(<<$', T/binary>>, Acc) -> escape(T, <<Acc/binary, "&apos;">>);
escape(<<$&, T/binary>>, Acc) -> escape(T, <<Acc/binary, "&amp;">>);
escape(<<H, T/binary>>, Acc) -> escape(T, <<Acc/binary, H>>).

%% ===================================================================
%% Decoding
%% ===================================================================

do_decode(<<"<?xml", T/binary>>) ->
    do_decode(decode_decl_attr(T, <<"version=">>, version), #state{}).

decode_decl_attr(<<H/utf8, T/binary>>, Check, Phase) when ?WS(H) ->
    decode_decl_attr(T, Check, Phase);
decode_decl_attr(<<H, T/binary>>, <<H, T1/binary>>, Phase) ->
    decode_decl_attr(T, T1, Phase);
decode_decl_attr(T, <<>>, version) ->
    decode_decl_version_val(T, undefined, <<"1.0">>);
decode_decl_attr(T, <<>>, encoding) ->
    decode_decl_enc_val(T, undefined, <<"UTF-8">>);
decode_decl_attr(T, <<>>, standalone) ->
    decode_decl_alone_val(T, undefined, <<>>).

decode_decl_version_val(<<$', T/binary>>, undefined, Check) ->
    decode_decl_version_val(T, $', Check);
decode_decl_version_val(<<$", T/binary>>, undefined, Check) ->
    decode_decl_version_val(T, $", Check);
decode_decl_version_val(<<Quote, T/binary>>, Quote, <<>>) ->
    decode_decl_rest(T, encoding);
decode_decl_version_val(<<H, T/binary>>, Quote, <<H, T1/binary>>) ->
    decode_decl_version_val(T, Quote, T1).

decode_decl_enc_val(<<$', T/binary>>, undefined, Check) ->
    decode_decl_enc_val(T, $', Check);
decode_decl_enc_val(<<$", T/binary>>, undefined, Check) ->
    decode_decl_enc_val(T, $", Check);
decode_decl_enc_val(<<H, T/binary>>, Quote, <<H, T1/binary>>) ->
    decode_decl_enc_val(T, Quote, T1);
decode_decl_enc_val(<<Quote, T/binary>>, Quote, <<>>) ->
    decode_decl_rest(T, standalone).

decode_decl_alone_val(<<$', T/binary>>, undefined, Acc) ->
    decode_decl_alone_val(T, $', Acc);
decode_decl_alone_val(<<$", T/binary>>, undefined, Acc) ->
    decode_decl_alone_val(T, $", Acc);
decode_decl_alone_val(<<Quote, T/binary>>, Quote, Acc) ->
    decode_decl_end(T, <<"?>">>, Acc);
decode_decl_alone_val(<<H, T/binary>>, Quote, Acc) ->
    decode_decl_alone_val(T, Quote, <<Acc/binary, H>>).

decode_decl_rest(<<H, T/binary>>, Phase) when ?WS(H) ->
    decode_decl_rest(T, Phase);
decode_decl_rest(<<$e, T/binary>>, encoding) ->
    decode_decl_attr(T, <<"ncoding=">>, encoding);
decode_decl_rest(<<$s, T/binary>>, encoding) ->
    decode_decl_attr(T, <<"tandalone=">>, standalone);
decode_decl_rest(<<$s, T/binary>>, standalone) ->
    decode_decl_attr(T, <<"tandalone=">>, standalone);
decode_decl_rest(<<$?, T/binary>>, _) ->
    decode_decl_end(T, <<$>>>, undefined).

decode_decl_end(T, <<>>, undefined) -> T;
decode_decl_end(T, <<>>, <<"yes">>) -> T;
decode_decl_end(T, <<>>, <<"no">>) -> T;
decode_decl_end(<<H, T/binary>>, Check, Alone) when ?WS(H) ->
    decode_decl_end(T, Check, Alone);
decode_decl_end(<<H, T/binary>>, <<H, T1/binary>>, Alone) ->
    decode_decl_end(T, T1, Alone).

do_decode(<<$<, T/binary>>, State = #state{}) ->
    decode_start(T, <<>>, State);
do_decode(<<H, T/binary>>, State = #state{}) when ?WS(H) ->
    do_decode(T, State);
do_decode(<<$&, T/binary>>, State = #state{}) ->
    decode_text_escape(T, <<>>, <<>>, State);
do_decode(<<H/utf8, T/binary>>, State = #state{}) ->
    decode_text(T, <<H/utf8>>, State).

decode_start(<<"![CDATA[", T/binary>>, <<>>, State) ->
    decode_cdata(T, <<>>, State);
decode_start(<<$/, T/binary>>, Acc, State) ->
    decode_empty(T, State#state{current = #xml{tag = Acc}});
decode_start(<<$>, T/binary>>, Acc, State) -> 
    decode_child(T, <<>>, State#state{current = #xml{tag = Acc}});
decode_start(<<H, T/binary>>, Acc, State) when ?WS(H) ->
    decode_name(T, <<>>, State#state{current = #xml{tag = Acc}});
decode_start(<<H/utf8, T/binary>>, <<>>, State) when ?START_CHAR(H) ->
    decode_start(T, <<H/utf8>>, State);
decode_start(<<H/utf8, T/binary>>, Acc = <<_, _/binary>>, State)
  when ?NAME_CHAR(H) ->
    decode_start(T, <<Acc/binary, H/utf8>>, State).

decode_cdata(<<$], T/binary>>, Acc, State) ->
    decode_cdata_end(T, Acc, <<$]>>, State);
decode_cdata(<<H/utf8, T/binary>>, Acc, State) ->
    decode_cdata(T, <<Acc/binary, H/utf8>>, State).

decode_cdata_end(<<$], T/binary>>, Acc, <<$]>>, State) ->
    decode_cdata_end(T, Acc, <<"]]">>, State);
decode_cdata_end(<<$>, T/binary>>, Acc, <<"]]">>, State) ->
    pop(T, State#state{current = #cdata{data = Acc}});
decode_cdata_end(T, Acc, Term, State) ->
    decode_cdata(T, <<Acc/binary, Term/binary>>, State).

decode_name(<<$/, T/binary>>, <<>>, State) -> decode_empty(T, State);
decode_name(<<$>, T/binary>>, <<>>, State) -> decode_child(T, <<>>, State);
decode_name(<<$=, T/binary>>, Acc, State) -> decode_value_start(T, Acc, State);
decode_name(<<H, T/binary>>, <<>>, State) when ?WS(H) ->
    decode_name(T, <<>>, State);
decode_name(<<H, T/binary>>, Acc, State) when ?WS(H) ->
    decode_assign(T, Acc, State);
decode_name(<<H/utf8, T/binary>>, <<>>, State) when ?START_CHAR(H) ->
    decode_name(T, <<H/utf8>>, State);
decode_name(<<H/utf8, T/binary>>, Acc = <<_, _/binary>>, State)
  when ?NAME_CHAR(H) ->
    decode_name(T, <<Acc/binary, H/utf8>>, State).

decode_assign(<<$=, T/binary>>, Name, State) ->
    decode_value_start(T, Name, State);
decode_assign(<<H, T/binary>>, Name, State) when ?WS(H) ->
    decode_assign(T, Name, State).

decode_value_start(<<$', T/binary>>, Name, State) ->
    decode_value(T, <<>>, $', Name, State);
decode_value_start(<<$", T/binary>>, Name, State) ->
    decode_value(T, <<>>, $", Name, State);
decode_value_start(<<H, T/binary>>, Name, State) when ?WS(H) ->
    decode_value_start(T, Name, State).

decode_value(<<$&, T/binary>>, Acc, Del, Name, State) ->
    decode_value_escape(T, <<>>, Acc, Del, Name, State);
decode_value(<<Del, T/binary>>, Acc, Del, Name, State) ->
    #state{current = XML = #xml{attrs = Attrs}} = State,
    State1 = State#state{current = XML#xml{attrs = [{Name, Acc} | Attrs]}},
    decode_name(T, <<>>, State1);
decode_value(<<H, T/binary>>, Acc, Del, Name, State) ->
    decode_value(T, <<Acc/binary, H>>, Del, Name, State).

decode_value_escape(<<";", T/binary>>, Acc, AccValue, Del, Name, State) ->
    C = unescape(Acc),
    decode_value(T, <<AccValue/binary, C>>, Del, Name, State);
decode_value_escape(<<H, T/binary>>, Acc, AccValue, Del, Name, State) ->
    decode_value_escape(T, <<Acc/binary, H>>, AccValue, Del, Name, State).

decode_child(<<$<, T/binary>>, <<>>, State) ->
    decode_child(T, <<$<>>, State);
decode_child(<<$/, T/binary>>, <<$<>>, State) ->
    decode_end_tag(T, <<>>, State);
decode_child(<<H, T/binary>>, <<>>, State) when ?WS(H) ->
    decode_child(T, <<>>, State);
decode_child(T, Acc, State) ->
    do_decode(<<Acc/binary, T/binary>>, push(State)).

decode_end_tag(<<$>, T/binary>>, A, State = #state{current = #xml{tag = A}}) ->
    pop(T, reverse_children(State));
decode_end_tag(<<H, T/binary>>, Acc, State) when ?WS(H) ->
    decode_end(T, Acc, State);
decode_end_tag(<<H, T/binary>>, Acc, State) ->
    decode_end_tag(T, <<Acc/binary, H>>, State).

decode_end(<<$>, T/binary>>, A, State = #state{current = #xml{tag = A}}) ->
    pop(T, reverse_children(State));
decode_end(<<H, T/binary>>, Acc, State) when ?WS(H) ->
    decode_end(T, Acc, State).

decode_empty(<<$>, T/binary>>, State) -> pop(T, State).

decode_text(<<H, T/binary>>, Acc, State) when ?WS(H) ->
    decode_text_ws(T, Acc, <<H>>, State);
decode_text(T = <<$<, _/binary>>, Acc, State) ->
    pop(T, State#state{current = Acc});
decode_text(<<$&, T/binary>>, Acc, State) ->
    decode_text_escape(T, <<>>, Acc, State);
decode_text(<<H/utf8, T/binary>>, Acc, State) ->
    decode_text(T, <<Acc/binary, H/utf8>>, State).

decode_text_escape(<<";", T/binary>>, Acc, TextValue, State) ->
    C = unescape(Acc),
    decode_text(T, <<TextValue/binary, C>>, State);
decode_text_escape(<<H, T/binary>>, Acc, TextValue, State) ->
    decode_text_escape(T, <<Acc/binary, H>>, TextValue, State).

decode_text_ws(<<H, T/binary>>, Acc, WS, State) when ?WS(H) ->
    decode_text_ws(T, Acc, <<WS/binary, H>>, State);
decode_text_ws(T = <<$<, _/binary>>, Acc, _, State) ->
    pop(T, State#state{current = Acc});
decode_text_ws(<<$&, T/binary>>, Acc, WS, State) ->
    decode_text_escape(T, <<>>, <<Acc/binary, WS/binary>>, State);
decode_text_ws(<<H/utf8, T/binary>>, Acc, WS, State) ->
    decode_text(T, <<Acc/binary, WS/binary, H/utf8>>, State).

push(State = #state{current = C, stack = Stack}) ->
    State#state{current = undefined, stack = [C | Stack]}.

pop(_, #state{stack = [], current = C}) -> C;
pop(Binary, State) ->
    #state{current = C, stack = [H = #xml{children = Cs} | T]} = State,
    State1 = State#state{current = H#xml{children = [C | Cs]}, stack = T},
    decode_child(Binary, <<>>, State1).

reverse_children(State = #state{current = XML = #xml{children = Cs}}) ->
    State#state{current = XML#xml{children = lists:reverse(Cs)}}.

unescape(<<"lt">>) -> $<;
unescape(<<"gt">>) -> $>;
unescape(<<"quot">>) -> $"; %% "
unescape(<<"apos">>) -> $'; %% '
unescape(<<"amp">>) -> $&;
unescape(<<"#x", T/binary>>) -> binary_to_integer(T, 16);
unescape(<<"#", T/binary>>) -> binary_to_integer(T).

%% ===================================================================
%% Selection
%% ===================================================================

do_select([], XML) -> XML;
do_select([child | T], #xml{children = [Child]}) ->
    do_select(T, Child);
do_select([{H} | T], #xml{children = Children}) ->
    [do_select(T, Child) || Child <- Children, Child#xml.tag == H];
do_select([H = [_ | _] | T], #xml{children = Children}) ->
    [do_select(T, Child) || Child <- Children, lists:member(Child#xml.tag, H)];
do_select([H | T], #xml{children = Children}) ->
    case lists:keyfind(H, #xml.tag, Children) of
        false -> [];
        XML = #xml{} -> do_select(T, XML)
    end.
