use strict;
use warnings;

# Create the DBO (2 tests)
my $dbo;
use Test::DBO DBM => 'DBM', tests => 41, tempdir => 1, connect_ok => [\$dbo];

my $test_tbl = $Test::DBO::prefix.'_tbl';

# Make sure QuoteIdentifier is OFF for DBM (1 test)
is $dbo->config('QuoteIdentifier'), 0, 'Method $dbo->config';

# Table methods: do, select* (15 tests)
my $t = Test::DBO::basic_methods($dbo, undef, $test_tbl) or die;

# Skip... (No tests)
Test::DBO::skip_advanced_table_methods($dbo, $t);

# Row methods: (10 tests)
Test::DBO::row_methods($dbo, $t);

# Query methods: (13 tests)
my $q = Test::DBO::query_methods($dbo, $t);

# Skip... (No tests)
Test::DBO::skip_advanced_query_methods($dbo, $t, $q);

# Cleanup
Test::DBO::cleanup($dbo);

