use strict;
use warnings;

# Create the DBO (2 tests)
my $dbo;
use Test::DBO SQLite => 'SQLite', tests => 87, tempdir => 1, connect_ok => [\$dbo];

# In SQLite there is no Schema
undef $Test::DBO::test_db;
undef $Test::DBO::test_sch;
$Test::DBO::can{collate} = 'BINARY';
$Test::DBO::can{auto_increment_id} = 'INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT';

# Table methods: do, select* (22 tests)
my $t = Test::DBO::basic_methods($dbo);

# Advanced table methods: insert, update, delete (2 tests)
Test::DBO::advanced_table_methods($dbo, $t);

# Row methods: (14 tests)
Test::DBO::row_methods($dbo, $t);

# Query methods: (17 tests)
my $q = Test::DBO::query_methods($dbo, $t);

# Advanced query methods: (11 tests)
Test::DBO::advanced_query_methods($dbo, $t, $q);

# Join methods: (10 tests)
Test::DBO::join_methods($dbo, $t->{Name}, 1);

END {
    # Cleanup (1 test)
    Test::DBO::cleanup($dbo) if $dbo;
}

