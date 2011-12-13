use strict;
use warnings;

# Allow the use of ORACLE_USERID or DBO_TEST_ORACLE_USER
BEGIN {
    $ENV{DBO_TEST_ORACLE_USER} = $ENV{ORACLE_USERID}
        if exists $ENV{ORACLE_USERID} and not exists $ENV{DBO_TEST_ORACLE_USER};
}
# Create the DBO (2 tests)
my $dbo;
use Test::DBO Oracle => 'Oracle', tests => 87, connect_ok => [\$dbo];

# Use the default Schema
undef $Test::DBO::test_db;
undef $Test::DBO::test_sch;
$Test::DBO::case_sensitivity_sql = 'SELECT COUNT(*) FROM DUAL WHERE ? LIKE ?';

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
Test::DBO::join_methods($dbo, $t->{Name});

END {
    # Cleanup (1 test)
    Test::DBO::cleanup($dbo) if $dbo;
}

