use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'DBIx::DBO',
    license             => 'perl',
    dist_author         => 'Vernon Lyon <vlyon@cpan.org>',
    dist_version_from   => 'lib/DBIx/DBO.pm',
    build_requires => {
        'Test::More' => 0.82,
    },
    requires => {
        'perl' => 5.010,
        'DBI' => 1.53,
    },
    add_to_cleanup      => [ 'DBIx-DBO-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
