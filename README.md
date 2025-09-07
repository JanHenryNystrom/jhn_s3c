jhn_s3c [(α)][5]
==========

A partial S3 Erlang client lib

  * [Introduction](#introduction)
  * [Features/Modules](#features)
  * [Build](#build)
  * [Contribute](#contribute) - Read if you're planning to submit patches

<a name='introduction'>

Introduction
------------

A partial S3 Erlang client lib that only supports a few methods and
AWS Signature Version 2. Will be expanded.

<a name='behaviours'>

<a name='features'>

Features/Modules
--------

TBD.

<a name='build'>

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

<a name='contribute'>

Contribute
----------

Should you find yourself using jhn_s3c and have issues, comments or
feedback please [create an issue here on GitHub.] [2]

Patches are greatly appreciated, but since these libraries reflect my
learning process and I have rather peculiar notions of code hygiene

For a much nicer history, please [write good commit messages][4].
I know I really should.

  [1]: https://github.com/erlang/rebar3
       "Rebar3 - A build tool for Erlang"
  [2]: http://github.com/JanHenryNystrom/jhn_s3c/issues
       "jhn_s3c issues"
  [4]: http://github.com/erlang/otp/wiki/Writing-good-commit-messages
       "Erlang/OTP commit messages"
  [5]: http://en.wikipedia.org/wiki/Software_release_life_cycle
       "Software release life cycle"
