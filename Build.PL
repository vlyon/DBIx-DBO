use strict;
use warnings;
use Module::Build;

my %build = (
    module_name         => 'DBIx::DBO',
    license             => 'perl',
    dist_author         => 'Vernon Lyon <vlyon@cpan.org>',
    dist_version_from   => 'lib/DBIx/DBO.pm',
    dynamic_config      => 1,
    configure_requires => {
        'Module::Build' => 0.28,
    },
    build_requires => {
        'Module::Build' => 0.28,
        'Test::More' => 0.62,
        'File::Temp' => 0,
    },
    requires => {
        'perl' => 5.008_001,
        'DBI' => 1.50,
    },
    meta_merge => {
        resources => {
            bugtracker => 'http://rt.cpan.org/Public/Dist/Display.html?Name=DBIx-DBO',
            repository => 'https://vlyon.svn.beanstalkapp.com/src/dbo',
        },
    },
);

if ($] < 5.008_009) {
    # Perl prior to 5.8.9 didn't have hv_store, so we'll need to build it
    $build{needs_compiler} = 1;
    $build{build_requires}{'ExtUtils::CBuilder'} = 0;
    $build{xs_files}{'QueryXS.xs'} = 'lib/DBIx/DBO/Query.xs';
}

if ($] < 5.009_005) {
    # Perl prior to 5.9.5 didn't have mro, so we'll need to use Class::C3
    $build{requires}{'MRO::Compat'} = 0.09;
}

unless (exists &Scalar::Util::weaken) {
    # The pure-perl Scalar::Util doesn't contain weaken(), Task::Weaken will try to fix it or give an error
    $build{requires}{'Task::Weaken'} = 0;
}

Module::Build->new(%build)->create_build_script();
