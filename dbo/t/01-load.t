use strict;
use warnings;
use Test::More tests => 4;

BEGIN {
    if ($Test::More::VERSION < 0.84) {
        diag "Test::More 0.84 is recommended, this is only $Test::More::VERSION!";
        unless (exists $::{note}) {
            eval q#
                sub Test::More::note {
                    local $Test::Builder::{_print_diag} = $Test::Builder::{_print};
                    Test::More->builder->diag(@_);
                }
                *note = \&Test::More::note;
            #;
            die $@ if $@;
        }
    }
	use_ok 'DBIx::DBO' or BAIL_OUT 'DBIx::DBO failed!';
}

diag "DBIx::DBO $DBIx::DBO::VERSION, Perl $], $^X";
note 'Available DBI drivers: '.join(', ', DBI->available_drivers);

ok $DBIx::DBO::Config{QuoteIdentifier}, 'QuoteIdentifier setting is ON by default';
import DBIx::DBO QuoteIdentifier => 123;
is $DBIx::DBO::Config{QuoteIdentifier}, 123, "Check 'QuoteIdentifier' import option";
DBIx::DBO->config(QuoteIdentifier => 456);
is +DBIx::DBO->config('QuoteIdentifier'), 456, 'Method DBIx::DBO->config';

