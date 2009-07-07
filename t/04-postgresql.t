#!perl -T

use strict;
use warnings;
use Test::DBO Pg => 11;

# Create the DBO
my $dbo = Test::DBO::connect_dbo('dbname=postgres', $user, $pswd);
ok $dbo->do('SET NAMES utf8'), 'SET NAMES utf8' or diag sql_err($dbo);

exit;
my $test_db = $ENV{DBO_TEST_PG_DB} || $Test::DBO::prefix.'_db';
my $test_tbl = $Test::DBO::prefix.'_tbl';
my $quoted_db = $dbo->_qi($test_db);
my $quoted_tbl = $dbo->_qi($test_tbl);

# Create a test database
ok $dbo->do("CREATE DATABASE $quoted_db CHARACTER SET utf8"), "Create database $quoted_db" or die sql_err($dbo);
my $drop_db = 1;
ok $dbo->do("USE $quoted_db"), "USE $quoted_db" or diag sql_err($dbo);

SKIP: {
    is $dbo->selectrow_array('SELECT DATABASE()'), $test_db, 'Correct DB selected'
        or diag sql_err($dbo) && skip 'Incorrect DB selected!', 21;

    # Test methods: do, select* (4 tests)
    Test::DBO::basic_methods($dbo, $quoted_tbl);
}

undef $drop_db;
ok $dbo->do("DROP DATABASE $quoted_db"), "Drop database $quoted_db" or die sql_err($dbo);

END {
    $dbo->do("DROP DATABASE $quoted_db") or diag sql_err($dbo) if $drop_db;
}

