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
__PACKAGE__->attr(ioloop   => sub { Mojo::IOLoop->singleton });
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

sub req {
	my $self = shift;
	my $type = shift;
	my $data = join("\0", @_);

	Mojo::Util::encode($self->encoding, $data) if $self->encoding;

	my $ret;

	my $cb = sub {
		my ( $self, $data ) = @_;
		$self->ioloop->stop;
		warn "# <<<< ",dump($data);
		my ($magic, $type, $len) = unpack( "a4NN", $data );
		die "wrong magic [$magic]" unless $magic eq "\0RES";
		die "ERROR" if $type == 19;
		$ret = substr($data,12,$len);
	};

	my $len = length($data);
	my $message = pack("a4NN", "\0REQ", $type, length $data ) . $data;
	warn "# >>>> ",dump($data);

	my $mqueue = $self->{_message_queue} ||= [];
	my $cqueue = $self->{_cb_queue}	  ||= [];


	push @$mqueue, $message;
	push @$cqueue, $cb;

	$self->connect unless $self->{_connection};
	$self->_send_next_message;

	$self->ioloop->start;

	return $ret;
}

sub start {
	my ($self) = @_;

	$self->ioloop->start;
	return $self;
}

sub stop {
	my ($self) = @_;

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

	$gearman->req( $type, $data );

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
