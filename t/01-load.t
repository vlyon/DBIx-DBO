#!perl -T

use strict;
use warnings;
use Test::More tests => 4;

BEGIN {
    $Test::More::VERSION >= 0.82
        or BAIL_OUT "Test::More 0.82 required, this is only $Test::More::VERSION!";
	use_ok 'DBIx::DBO'
        or BAIL_OUT 'DBIx::DBO failed!';
}

diag "DBIx::DBO $DBIx::DBO::VERSION, Perl $], $^X";
note 'Available DBI drivers: '.join(', ', DBI->available_drivers);

ok $DBIx::DBO::Config{QuoteIdentifier}, 'QuoteIdentifier setting is ON by default';
import DBIx::DBO QuoteIdentifier => 123;
is $DBIx::DBO::Config{QuoteIdentifier}, 123, "Check 'QuoteIdentifier' import option";
DBIx::DBO->config(QuoteIdentifier => 456);
is +DBIx::DBO->config('QuoteIdentifier'), 456, 'Method DBIx::DBO->config';

