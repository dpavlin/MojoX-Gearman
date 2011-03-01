#!/usr/bin/env perl
use warnings;
use strict;

use Test::More tests => 5;
use Data::Dump qw(dump);
use lib 'lib';

use_ok 'MojoX::Gearman';

my $g = new_ok 'MojoX::Gearman';

my $name = "mojo_g";
ok( my $echo = $g->req( 'CAN_DO', $name, sub {
	my $payload = shift;
	warn "DO $name ", dump($payload), $/;
	return $payload + 1;
}), "CAN_DO $name" );

diag "start loop";
ok( $g->req( 'GRAB_JOB' ), 'GRAB_JOB' );
ok( $g->start, 'start' );

