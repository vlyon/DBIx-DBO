use strict;
use warnings;
use Module::Build;

# Require MRO::Compat if using perl < 5.9.5
my $class = $] < 5.009_005 ? 'Module::Build' : Module::Build->subclass(code => q/
    sub write_metafile {
        my $self = shift;
        $self->{properties}{requires}{'MRO::Compat'} = 0;
        $self->SUPER::write_metafile(@_);
    }
/);

my $needs_compiler = $] < 5.008_009;

my $builder = $class->new(
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
        $needs_compiler ? ('ExtUtils::CBuilder' => 0) : (),
    },
    requires => {
        'perl' => 5.008_001,
        'DBI' => 1.50,
        $] < 5.009_005 ? ('MRO::Compat' => 0) : (),
    },
    needs_compiler => $needs_compiler,
    xs_files => {
        $needs_compiler ? ('QueryXS.xs' => 'lib/DBIx/DBO/Query.xs') : (),
    },
    meta_merge => {
        resources => {
            bugtracker => 'http://rt.cpan.org/Public/Dist/Display.html?Name=DBIx-DBO',
            repository => 'http://vlyon.svn.beanstalkapp.com/src/dbo',
        },
    },
);

$builder->create_build_script();
