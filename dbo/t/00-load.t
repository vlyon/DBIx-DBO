#!perl -T

use strict;
use warnings;
use Test::More tests => 4;

BEGIN {
	use_ok 'DBIx::DBO' or BAIL_OUT 'DBIx::DBO failed!';
}

diag "Testing DBIx::DBO $DBIx::DBO::VERSION, Perl $], $^X";
note 'Available DBI drivers: '.join(', ', DBI->available_drivers);

is $DBIx::DBO::QuoteIdentifier, 1, 'QuoteIdentifier setting is ON by default';
import DBIx::DBO 'NoQuoteIdentifier';
is $DBIx::DBO::QuoteIdentifier, 0, "Check 'NoQuoteIdentifier' import option";
import DBIx::DBO 'QuoteIdentifier';
is $DBIx::DBO::QuoteIdentifier, 1, "Check 'QuoteIdentifier' import option";

