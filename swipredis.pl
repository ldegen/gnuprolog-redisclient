%%
% some glue to make gnuprolog-redisclient run with swi-prolog
%
:- module(redis,[
          redis_connect/1,
          redis_connect/3,
          redis_disconnect/1,
          redis_do/3,
          redis/1,
          redis_print/1,
          redis_subscribe/2,
          redis_subscribe_only/2,
          redis_next_msg/2]).


redis_subscribe(Topic,Msg):-
  call_cleanup(
      ( redis_connect(R),
        redis_subscribe(R,Topic,Msg)
      ),
      ( writeln(disconnecting),
        redis_disconnect(R)
      )
  ).

redis_subscribe(R,Topic,Msg):-
  redis_subscribe_only(R,Topic),
  repeat,
  redis_next_msg(R,Msg).

redis_next_msg(redis(SI,_,_),Msg):-
  get_msg(SI,Msg).


redis_subscribe_only(redis(_,SO,_),Topic):-
  gpredis_build_cmd(subscribe(Topic),Cmd),
  gpredis_write(SO,Cmd).

get_msg(SI,Out):-
  get_byte(SI, ReplyMode),
  char_code(ReplyMode2, ReplyMode),
  once(gpredis_parse_reply(ReplyMode2, SI, Out)).

socket(_,S-_-_):-
  tcp_socket(S).

socket_connect(S-SI-SO, 'AF_INET'(Host, Port), SI, SO):-
  tcp_connect(S,Host:Port,SI,SO).

socket_close(_-SI-SO):-
  close(SI),close(SO).

format_to_atom(Atom,Format,Data):-
  format(atom(Atom),Format,Data).

format_to_codes(Codes,Format,Data):-
  format(codes(Codes),Format,Data).

set_stream_type(S,T):-
  set_stream(S,type(T)).


:- include(gpredis).
