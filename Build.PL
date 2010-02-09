use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'DBIx::DBO',
    license             => 'perl',
    dist_author         => 'Vernon Lyon <vlyon@cpan.org>',
    dist_version_from   => 'lib/DBIx/DBO.pm',
    build_requires => {
        'Test::More' => 0.62,
        'File::Temp' => 0,
    },
    requires => {
        'perl' => 5.008,
        'DBI' => 1.50,
    },
    recommends => {
        'MRO::Compat' => 0,
    },
    add_to_cleanup      => [ 'DBIx-DBO-*' ],
    meta_add => {
        bugtracker => 'http://rt.cpan.org/NoAuth/Bugs.html?Dist=DBIx-DBO',
        repository => 'http://vlyon.svn.beanstalkapp.com/src/dbo',
    },
);

$builder->create_build_script();
