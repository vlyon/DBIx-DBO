#!perl -T

use strict;
use warnings;
use File::Temp;
my $dbo;
use Test::DBO SQLite => 'SQLite', tests => 22, tempdir => 1, connect_ok => [\$dbo];

my $test_tbl = $Test::DBO::prefix.'_tbl';

# Test methods: do, select* (9 tests)
my $t = Test::DBO::basic_methods($dbo, undef, $test_tbl);

# Test methods: do, select* (2 tests)
Test::DBO::advanced_table_methods($dbo, $t);

# Query methods: (9 tests)
Test::DBO::query_methods($dbo, $t);

# Cleanup
Test::DBO::cleanup($dbo);

