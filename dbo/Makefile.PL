use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'DBIx::DBO',
    AUTHOR              => 'Vernon Lyon <vlyon@cpan.org>',
    VERSION_FROM        => 'lib/DBIx/DBO.pm',
    ABSTRACT_FROM       => 'lib/DBIx/DBO.pm',
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'DBIx-DBO-*' },
);
