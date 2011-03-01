#!/usr/bin/env perl
use warnings;
use strict;

use Test::More tests => 9;
use Data::Dump qw(dump);
use lib 'lib';

use_ok 'MojoX::Gearman';

my $g = new_ok 'MojoX::Gearman';

ok( my $echo = $g->req( 'ECHO_REQ', "foobar" ), 'ECHO' );
cmp_ok $echo, 'eq', "foobar";

ok( my $ping = $g->req( 'SUBMIT_JOB', 'ping', '', 'bla' ), 'SUBMIT_JOB' );
like $ping, qr/pong/, 'got pong';
diag dump $ping;

ok( $g->req( 'ECHO_REQ', "alive" ), 'ECHO - still alive - still alive?' );

cmp_ok( $g->req( 'SUBMIT_JOB', 'mojo_g', '', 42 ), '==', 43, 'mojo_g' );

cmp_ok( $g->req( 'SUBMIT_JOB', 'mojo_rev', '', "foobar" ), 'eq', "raboof", 'mojo_rev' );

