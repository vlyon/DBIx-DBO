use strict;
use warnings;

# Create the DBO (2 tests)
my $dbo;
use Test::DBO SQLite => 'SQLite', tests => 60, tempdir => 1, connect_ok => [\$dbo];

my $test_tbl = $Test::DBO::prefix.'_tbl';

# Table methods: do, select* (15 tests)
my $t = Test::DBO::basic_methods($dbo, undef, $test_tbl);

# Advanced table methods: insert, update, delete (2 tests)
Test::DBO::advanced_table_methods($dbo, $t);

# Row methods: (10 tests)
Test::DBO::row_methods($dbo, $t);

# Query methods: (13 tests)
my $q = Test::DBO::query_methods($dbo, $t);

# Advanced query methods: (9 tests)
Test::DBO::advanced_query_methods($dbo, $t, $q);

# Join methods: (9 tests)
Test::DBO::join_methods($dbo, $t->{Name}, 1);

# Cleanup
Test::DBO::cleanup($dbo);

