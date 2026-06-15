jhn_s3c [(γ)][5]
==========

A partial S3 Erlang client lib

  * [Introduction](#introduction)
  * [Configuration](#configuration)
  * [Build](#build)
  * [Contribute](#contribute) - Read if you're planning to submit patches

<a name='introduction'/>

Introduction
------------

A partial S3 Erlang client lib that only supports a few methods and
AWS Signature Version 2. Will be expanded.

<a name='contribute'/>

Configuration
--------

New (Multiple servers):

If the servers is config is undefined or the empty list the old (legacy)
configuation is used. If at least one server is defined any definitions
of `host`, `port`, `access_key_id`, or `access_key` are ignored.

The first server config is always the default server so if one of the functions
are called without the server option defined that is the one called.

All the config values except `servers` acts as default values if defined but
will be overridden by the server config if that is defined. So for example the
`request_type` if not defined at the top level or for a the server will have the
default value of `path`, but if set to say `virtual_host` it will have that
value for all servers not defining it, and if a server defines it to `path`
well then that particular server will use the path` request type.

The default for `request_type` is `path` in the new configuration unlike the
old.

* servers (`[]`): a list of server configurations
* request_type (`path`): connection mode either `virtual_host` or `path`
* protocol (`http`):  either `http` or `https`
* hackney_opts (`[]`): additional options provided to the hackney request
* hackney_pool (`jhn_s3`): which hackney pool to use
* max_tries (`3`): the max number of request tries including inital and retries

Server configuration:
* request_type (`path`): connection mode either `virtual_host` or `path`
* protocol (`http`):  either `http` or `https`
* host (`~"localhost"`): either a hostname  or ip-address
* port (`80`): the port number as an integer
* hackney_opts (`[]`): additional options provided to the hackney request
* hackney_pool (`jhn_s3`): which hackney pool to use
* max_tries (`3`): the max number of request tries including inital and retries
* access_key_id: the user/access_key_id for S3
* access_key: password/access_key for S3


Example:
From the test suite where the protocol definition being strictly speeking
redundant but harmless and  acts a default for `server_a` and `server_b`.

```erlang
[{kernel, [{inetrc, "test/inetrc"}]},
 {jhn_s3c,
  [{servers,
    [{server_a, [{host, ~"s3.service"}, {port, 9000},
                 {access_key_id, ~"admin"}, {access_key, ~"password"}]},
     {server_b, [{host, ~"s3bis.service"}, {port, 8000},
                 {access_key_id, ~"hugo"}, {access_key, ~"password"}]}]},
   {protocol, http}
  ]
 }
].
 ```

Old (legacy will be discontinued with theh next major release):

* request_type (`path`): connection mode either `virtual_host` or `path`
* protocol (`http`):  either `http` or `https`
* host (`~"localhost"`): either a hostname  or ip-address
* port (`80`): the port number as an integer
* hackney_opts (`[]`): additional options provided to the hackney request
* hackney_pool (`jhn_s3`): which hackney pool to use
* max_tries (`3`): the max number of request tries including inital and retries
* access_key_id: the user/access_key_id for S3
* access_key: password/access_key for S3

```erlang
[{kernel, [{inetrc, "test/inetrc"}]},
 {jhn_s3c,
  [{protocol, http},
   {host, ~"s3.service"},
   {port, 9000},
   {access_key_id, "admin"},
   {access_key, "password"}]}].
```

<a name='build'/>

Build
-----

jhn_s3c requires [rebar3][1] to build, but provides make support to download
and install rebar. To build jhn_s3c, go to the jhn_s3c directory and type:

```sh
make
```

To make sure jhn_s3c works on your platform, run the tests (reuquires docker):

```sh
make ci
```

<a name='contribute'/>

Contribute
----------

Should you find yourself using jhn_s3c and have issues, comments or
feedback please [create an issue here on GitHub.] [2]

Patches are greatly appreciated, but since these libraries reflect my
learning process and I have rather peculiar notions of code hygiene

For a much nicer history, please [write good commit messages][4].
I know I really should.

  [1]: https://github.com/erlang/rebar3 "Rebar3 - A build tool for Erlang"
  [2]: http://github.com/JanHenryNystrom/jhn_s3c/issues "jhn_s3c issues"
  [4]: http://github.com/erlang/otp/wiki/Writing-good-commit-messages "Erlang/OTP commit messages"
  [5]: http://en.wikipedia.org/wiki/Software_release_life_cycle "Software release life cycle"
