#!perl -T

use strict;
use warnings;

use Test::DBO 'Pg';

my $test_db = $ENV{DBO_TEST_PG_DB} || $Test::DBO::prefix.'_db';
my $test_tbl = $Test::DBO::prefix.'_tbl';
my $quoted_db;
my $quoted_tbl;
my $drop_db;

# Try to connect to the default DB
my $dsn = $ENV{DBO_TEST_PG_DB} ? "dbname=\"$ENV{DBO_TEST_PG_DB}\"" : '';
# Create the DBO
my $dbo = Test::DBO::connect_dbo($dsn) or note "Can't connect: $DBI::errstr";
if ($dbo) {
    $quoted_db = $dbo->_qi($test_db);
    $quoted_tbl = $dbo->_qi($test_tbl);
} else {
    # Try to connect to the postgres or template1 DB and create the test DB
    if (defined $ENV{DBO_TEST_PG_USER}) {
        $dbo = Test::DBO::connect_dbo('dbname=postgres') or note "Can't connect: $DBI::errstr";
    }
    $dbo ||= Test::DBO::connect_dbo('dbname=postgres', 'postgres') or note "Can't connect: $DBI::errstr";
    $dbo ||= Test::DBO::connect_dbo('dbname=template1') or note "Can't connect: $DBI::errstr";
    plan skip_all => "Can't connect: $DBI::errstr" unless $dbo;

    # Create a test database
    $quoted_db = $dbo->_qi($test_db);
    $quoted_tbl = $dbo->_qi($test_tbl);
    unless ($dbo->do("CREATE DATABASE $quoted_db")) {
        note sql_err($dbo);
        plan skip_all => "Can't create test database";
    }
    note "Created test database: $quoted_db";
    $drop_db = $dbo;
    $dbo = Test::DBO::connect_dbo("dbname=$quoted_db") or note "Can't connect: $DBI::errstr";
    plan skip_all => "Can't connect to newly created test database: $DBI::errstr" unless $dbo;
}

plan tests => 6;
pass "Connect to Pg";
isa_ok $dbo, 'DBIx::DBO::Pg', '$dbo';

# Test methods: do, select* (4 tests)
Test::DBO::basic_methods($dbo, $quoted_tbl);

END {
    if ($drop_db) {
        undef $dbo; # Make sure we're no longer connected
        $drop_db->do("DROP DATABASE $quoted_db") or diag sql_err($dbo);
        note "Dropped test database: $quoted_db";
    }
}

