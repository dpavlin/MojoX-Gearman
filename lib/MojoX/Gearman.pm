package MojoX::Gearman;

use strict;
use warnings;

our $VERSION = 0.1;
use base 'Mojo::Base';

use Mojo::IOLoop;
use List::Util	  ();
use Mojo::Util	  ();
use Scalar::Util	();
use Data::Dump qw(dump);
require Carp;

__PACKAGE__->attr(server   => '127.0.0.1:4730');
__PACKAGE__->attr(ioloop   => sub { Mojo::IOLoop->new });
__PACKAGE__->attr(error	=> undef);
__PACKAGE__->attr(timeout  => 300);
__PACKAGE__->attr(encoding => 'UTF-8');
__PACKAGE__->attr(
	on_error => sub {
		sub {
			my $gearman = shift;
			warn "Gearman error: ", $gearman->error, "\n";
		  }
	}
);
__PACKAGE__->attr(res => undef);

sub DESTROY {
	my $self = shift;

	# Loop
	return unless my $loop = $self->ioloop;

	# Cleanup connection
	$loop->drop($self->{_connection})
	  if $self->{_connection};
}

sub connect {
	my $self = shift;

	# drop old connection
	if ($self->connected) {
		$self->ioloop->drop($self->{_connection});
	}

	$self->server =~ m{^([^:]+)(:(\d+))?};
	my $address = $1;
	my $port = $3 || 4730;

	Scalar::Util::weaken $self;

	# connect
	$self->{_connecting} = 1;
	$self->{_connection} = $self->ioloop->connect(
		{   address	=> $address,
			port	   => $port,
			on_connect => sub { $self->_on_connect(@_) },
			on_read	=> sub { $self->_on_read(@_) },
			on_error   => sub { $self->_on_error(@_) },
			on_hup	 => sub { $self->_on_hup(@_) },
		}
	);

	return $self;
}

sub connected {
	my $self = shift;

	return $self->{_connection};
}

my $packet_type = {
	CAN_DO => 1,

	PRE_SLEEP => 4,

	NOOP => 6,

	SUBMIT_JOB => 7,
	JOB_CREATED => 8,

	GRAB_JOB => 9,
	NO_JOB => 10,
	JOB_ASSIGN => 11,

	WORK_COMPLETE => 13,

	ECHO_REQ => 16,
	ECHO_RES => 17,

	ERROR => 19,
};

my $nr2type;
$nr2type->{ $packet_type->{$_} } = $_ foreach keys %$packet_type;


sub parse_packet {
	my ($self,$data) = @_;
	die "no data in packet" unless $data;
	my ($magic, $type, $len) = unpack( "a4NN", $data );
	die "wrong magic [$magic]" unless $magic eq "\0RES";
	die "unsupported type [$type]" unless exists $nr2type->{$type};
	die "ERROR" if $type == $packet_type->{ERROR};
	return ( $type, split("\0", substr($data,12,$len)) );
}

sub req {
	my $self = shift;
warn "XXX req ",dump(@_);
	my $type = shift;
	my $callback = pop @_ if ref $_[$#_] eq 'CODE';
	my $data = join("\0", @_);

	die "can't find packet type $type in ", dump $packet_type unless exists $packet_type->{$type};
	Mojo::Util::encode($self->encoding, $data) if $self->encoding;

	$self->{_res} = undef;

	my $response;
	my $cb = sub {
		my ( $self, $data ) = @_;
		my ( $type, @data ) = $self->parse_packet($data);
		warn "# <<<< ", $nr2type->{$type}, " ",dump(@data);

		if ( $type == $packet_type->{JOB_CREATED} ) {
			push @{ $self->{_cb_queue} }, sub {
				my ( $self,$data ) = @_;
warn "# WORK_COMPLETE ",dump $data;
				my ( $type, $handle, $out ) = $self->parse_packet($data);
				die "not WORK_COMPLETE" unless $type == $packet_type->{WORK_COMPLETE};
				$self->res( $out );
				$self->stop;
			};
		} elsif ( $type == $packet_type->{NO_JOB} ) {
			$self->req( 'PRE_SLEEP' );
		} elsif ( $type == $packet_type->{JOB_ASSIGN} ) {
			my ( $handle, $function, $workload ) = @data;
			my $callback = $self->{_function}->{$function};
			die "no $function callback" unless ref $callback eq 'CODE';
			warn "# calling $data callback $callback";
			my $out = $callback->( $workload );
			warn "# === ",dump $out;
			$self->req( 'WORK_COMPLETE', $handle, $out );
			$self->req( 'GRAB_JOB' );
		} elsif ( $type == $packet_type->{NOOP} ) {
			$self->req( 'GRAB_JOB' );
		} else {
			$self->stop;
		}

		my $out = $#data == 0 ? $data[0] : [ @data ];
		$self->res( $out );

	};

#	$data .= "\0" if $data;
	my $len = length($data);
	my $message = pack("a4NN", "\0REQ", $packet_type->{$type}, length $data ) . $data;
	warn "# >>>> $type ",dump($message);

	my $mqueue = $self->{_message_queue} ||= [];
	my $cqueue = $self->{_cb_queue}	  ||= [];

	push @$mqueue, $message;
	push @$cqueue, $cb;

	$self->connect unless $self->{_connection};
	$self->_send_next_message;

	if ( $type eq 'CAN_DO' ) {
		$self->{_function}->{$data} = $callback;
		warn "# installed $data callback $callback";
		$self->req( 'GRAB_JOB' );
	}
		

#	$self->start;

	$self->res;
}

sub start {
	my ($self) = @_;
	warn "# start";
	$self->ioloop->start;
	return $self;
}

sub stop {
	my ($self) = @_;
	warn "# stop";
	$self->ioloop->stop;
	return $self;
}

sub _send_next_message {
	my ($self) = @_;

	if ((my $c = $self->{_connection}) && !$self->{_connecting}) {
		while (my $message = shift @{$self->{_message_queue}}) {
			warn "# write ",dump($message);
			$self->ioloop->write($c, $message);
		}
	}
}

sub _on_connect {
	my ($self, $ioloop, $id) = @_;
	delete $self->{_connecting};

	$ioloop->connection_timeout($id => $self->timeout);

	$self->_send_next_message;
}

sub _on_error {
	my ($self, $ioloop, $id, $error) = @_;

	warn "ERROR: $error";

	$self->error($error);
	$self->_inform_queue;

	$self->on_error->($self);

	$ioloop->drop($id);
}

sub _on_hup {
	my ($self, $ioloop, $id) = @_;

	$self->{error} ||= 'disconnected';
	$self->_inform_queue;

	delete $self->{_message_queue};

	delete $self->{_connecting};
	delete $self->{_connection};
}

sub _inform_queue {
	my ($self) = @_;

	for my $cb (@{$self->{_cb_queue}}) {
		$cb->($self) if $cb;
	}
	$self->{_queue} = [];
}

sub _on_read {
	my ($self, $ioloop, $id, $data) = @_;

	my $cb = shift @{$self->{_cb_queue}};
	if ($cb) {
		Mojo::Util::decode($self->encoding, $data) if $data;
		warn "# on read callback with ", dump($data);
		$cb->($self, $data);
	} else {
		warn "no callback";
	}

	# Reset error after callback dispatching
	$self->error(undef);
}

1;
__END__

=head1 NAME

MojoX::Gearman - asynchronous Gearman client for L<Mojolicious>.

=head1 SYNOPSIS

	use MojoX::Gearman;

	my $gearman = MojoX::Gearman->new(server => '127.0.0.1:4730');

=head1 DESCRIPTION

L<MojoX::Gearman> is an asynchronous client to Gearman for Mojo.

=head1 ATTRIBUTES

L<MojoX::Gearman> implements the following attributes.

=head2 C<server>

	my $server = $gearman->server;
	$gearman	 = $gearman->server('127.0.0.1:4730');

C<Gearman> server connection string, defaults to '127.0.0.1:4730'.

=head2 C<ioloop>

	my $ioloop = $gearman->ioloop;
	$gearman	 = $gearman->ioloop(Mojo::IOLoop->new);

Loop object to use for io operations, by default a L<Mojo::IOLoop> singleton
object will be used.

=head2 C<timeout>

	my $timeout = $gearman->timeout;
	$gearman	  = $gearman->timeout(100);

Maximum amount of time in seconds a connection can be inactive before being
dropped, defaults to C<300>.

=head2 C<encoding>

	my $encoding = $gearman->encoding;
	$gearman	   = $gearman->encoding('UTF-8');

Encoding used for stored data, defaults to C<UTF-8>.

=head1 METHODS

=head2 C<req>

	$gearman->req( $type, $data, ..., sub { # callback } );

=head2 C<error>

	$gearman->execute("ping" => sub {
		my ($gearman, $result) = @_;
		die $gearman->error unless defined $result;
	}

Returns error occured during command execution.
Note that this method returns error code just from current command and
can be used just in callback.

=head2 C<on_error>

	$gearman->on_error(sub{
		my $gearman = shift;
		warn 'Gearman error ', $gearman->error, "\n";
	});

Executes if error occured. Called before commands callbacks.

=head2 C<start>

	$gearman->start;

Starts IOLoop. Shortcut for $gearman->ioloop->start;

=head1 SEE ALSO

L<Gearman::Client>, L<Mojolicious>, L<Mojo::IOLoop>

=head1 SUPPORT

=head1 DEVELOPMENT

=head2 Repository

	https://github.com/dpavlin/mojox-gearman

=head1 AUTHOR

Dobrica Pavlinusic, C<dpavlin@rot13.org>.

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2010, Dobrica Pavlinusic

This program is free software, you can gearmantribute it and/or modify it under
the terms of the Artistic License version 2.0.

=cut
