use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'DBIx::DBO',
    AUTHOR              => 'Vernon Lyon <vlyon@cpan.org>',
    VERSION_FROM        => 'lib/DBIx/DBO.pm',
    ABSTRACT_FROM       => 'lib/DBIx/DBO.pm',
    MIN_PERL_VERSION    => '5.010',
    PL_FILES            => {},
    PREREQ_PM => {
        'DBI' => 0,
        'Test::More' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'DBIx-DBO-*' },
);
