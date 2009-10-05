#!perl -T

use strict;
use warnings;
use Test::DBO DBM => 'DBM', tests => 21, tempdir => 1;

# Create the DBO (2 tests)
my $dbo = Test::DBO::connect_ok();
#my $dbo = Test::DBO::connect_ok("mldbm=Storable");

my $test_tbl = $Test::DBO::prefix.'_tbl';

# Make sure QuoteIdentifier is OFF for DBM (1 test)
is $dbo->config('QuoteIdentifier'), 0, 'Method $dbo->config';

# Table methods: do, select* (9 tests)
my $t = Test::DBO::basic_methods($dbo, undef, $test_tbl);

# Skip... (No tests)
Test::DBO::skip_advanced_table_methods($dbo, $t);

# Query methods: (9 tests)
my $q = Test::DBO::query_methods($dbo, $t);

# Skip... (No tests)
Test::DBO::skip_advanced_query_methods($dbo, $t, $q);

# Cleanup
Test::DBO::cleanup($dbo);

