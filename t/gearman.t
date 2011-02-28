#!/usr/bin/env perl
use warnings;
use strict;

use Test::More tests => 4;
use lib 'lib';

use_ok 'MojoX::Gearman';

my $g = new_ok 'MojoX::Gearman';

ok( my $echo = $g->req( 16, "foobar" ), 'echo' );
cmp_ok $echo, 'eq', "foobar";
