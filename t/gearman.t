#!/usr/bin/env perl
use warnings;
use strict;

use Test::More tests => 5;
use Data::Dump qw(dump);
use lib 'lib';

use_ok 'MojoX::Gearman';

my $g = new_ok 'MojoX::Gearman';

ok( my $echo = $g->req( 16, "foobar" ), 'ECHO' );
cmp_ok $echo, 'eq', "foobar";

ok( my $ping = $g->req( 7, 'ping', '', 'bla' ), 'SUBMIT_JOB' );
diag dump $ping;

