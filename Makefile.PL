use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
	NAME         => 'MojoX::Gearman',
	VERSION_FROM => 'lib/MojoX/Gearman.pm', # finds $VERSION
	PREREQ_PM    => {
		'Test::Simple' => 0.44,
		'Mojolicious'  => 0.9003,
	},
	AUTHOR       => 'Dobrica Pavlinusic <dpavlin@rot13.org>',
	ABSTRACT_FROM => 'lib/MojoX/Gearman.pm',
);

