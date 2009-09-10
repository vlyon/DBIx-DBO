use strict;
use warnings;
use 5.010_000;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'DBIx::DBO',
    AUTHOR              => 'Vernon Lyon <vlyon@cpan.org>',
    VERSION_FROM        => 'lib/DBIx/DBO.pm',
    ABSTRACT_FROM       => 'lib/DBIx/DBO.pm',
    PL_FILES            => {},
    PREREQ_PM => {
        'DBI' => 1.53,
        'Test::More' => 0.82,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'DBIx-DBO-*' },
);
