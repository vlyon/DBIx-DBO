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
        'perl' => 5.008_001,
        'DBI' => 1.50,
    },
    recommends => {
        $] < 5.009_005 ? ('MRO::Compat' => 0) : (),
    },
    meta_merge => {
        resources => {
            bugtracker => 'http://rt.cpan.org/Public/Dist/Display.html?Name=DBIx-DBO',
            repository => 'http://vlyon.svn.beanstalkapp.com/src/dbo',
        },
    },
    add_to_cleanup      => [ 'DBIx-DBO-*' ],
);

$builder->create_build_script();
