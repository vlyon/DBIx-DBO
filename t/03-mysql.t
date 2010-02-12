use strict;
use warnings;

# Create the DBO (3 tests)
my $dbo;
use Test::DBO mysql => 'MySQL', tests => 67, connect_ok => [\$dbo];
ok $dbo->do('SET NAMES utf8'), 'SET NAMES utf8' or diag sql_err($dbo);

my $test_db = $Test::DBO::prefix.'_db';
my $test_tbl = $Test::DBO::prefix.'_tbl';
my $quoted_db = $dbo->_qi($test_db);

# Create a test database (3 tests)
ok $dbo->do("CREATE DATABASE $quoted_db CHARACTER SET utf8"), "Create database $quoted_db" or die sql_err($dbo);
my $drop_db = 1;
ok $dbo->do("USE $quoted_db"), "USE $quoted_db" or diag sql_err($dbo);
is $dbo->selectrow_array('SELECT DATABASE()'), $test_db, 'Correct DB selected' or die sql_err($dbo);

# Table methods: do, select* (15 tests)
my $t = Test::DBO::basic_methods($dbo, $test_db, $test_tbl);

# Advanced table methods: insert, update, delete (2 tests)
Test::DBO::advanced_table_methods($dbo, $t);

# Row methods: (10 tests)
Test::DBO::row_methods($dbo, $t);

# Query methods: (15 tests)
my $q = Test::DBO::query_methods($dbo, $t);

# Advanced query methods: (9 tests)
Test::DBO::advanced_query_methods($dbo, $t, $q);

# Join methods: (9 tests)
Test::DBO::join_methods($dbo, $t->{Name});

# Cleanup (1 test)
Test::DBO::cleanup($dbo);

undef $drop_db;
ok $dbo->do("DROP DATABASE $quoted_db"), "Drop database $quoted_db" or die sql_err($dbo);

END {
    $dbo->do("DROP DATABASE $quoted_db") or diag sql_err($dbo) if $drop_db;
}

