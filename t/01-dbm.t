#!perl -T

use strict;
use warnings;
use Test::DBO DBM => 21, tempdir => 1;

# Create the DBO
my $dbo = Test::DBO::connect_ok();
#my $dbo = Test::DBO::connect_ok("mldbm=Storable");

my $test_tbl = $Test::DBO::prefix.'_tbl';

# Make sure QuoteIdentifier is OFF for DBM
is $dbo->config('QuoteIdentifier'), 0, 'Method $dbo->config';

# Test methods: do, select* (4 tests)
my $t = Test::DBO::basic_methods($dbo, undef, $test_tbl);

# Skip... (No tests)
Test::DBO::skip_advanced_table_methods($dbo, $t);

# Query methods: (2 tests)
Test::DBO::query_methods($dbo, $t);

# Cleanup
Test::DBO::cleanup($dbo);

