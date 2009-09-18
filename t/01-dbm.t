#!perl -T

use strict;
use warnings;
use Test::DBO DBM => 11, tempdir => 1;

# Create the DBO
my $dbo = Test::DBO::connect_ok();
#my $dbo = Test::DBO::connect_ok("mldbm=Storable");

my $test_tbl = $Test::DBO::prefix.'_tbl';

# Make sure QuoteIdentifier is OFF for DBM
is $dbo->config('QuoteIdentifier'), 0, 'Method $dbo->config';

# Test methods: do, select* (4 tests)
Test::DBO::basic_methods($dbo, undef, $test_tbl);

