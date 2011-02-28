#!/usr/bin/env perl
use warnings;
use strict;

use Test::More tests => 3;
use lib 'lib';

use_ok 'MojoX::Gearman';

my $g = new_ok 'MojoX::Gearman';

ok( $g->req( 16, "foobar" ), 'echo' );
