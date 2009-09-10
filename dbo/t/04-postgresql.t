#!perl -T

use strict;
use warnings;

use Test::DBO 'Pg';

my $dbo;
my $drop_db;
my $test_db = $ENV{DBO_TEST_PG_DB} || $Test::DBO::prefix.'_db';
my $test_tbl = $Test::DBO::prefix.'_tbl';
my $quoted_db;
my $quoted_tbl;

sub connect_and_create {
    if (my $dbo = Test::DBO::connect_dbo(@_)) {
        # Create a test database
        $quoted_db = $dbo->_qi($test_db);
        $quoted_tbl = $dbo->_qi($test_tbl);
        if ($dbo->do("CREATE DATABASE $quoted_db")) {
            note "Created $quoted_db test database";
            return $dbo;
        }
        note sql_err($dbo);
        return;
    }
    my $msg = $_[0] =~ /dbname=(.*)/ ? " to $1 database" : '';
    note "Can't connect$msg: $DBI::errstr";
    return;
}

# Try to connect to the default DB
if ($ENV{DBO_TEST_PG_DB}) {
    unless ($dbo = Test::DBO::connect_dbo("dbname=\"$ENV{DBO_TEST_PG_DB}\"")) {
        note "Can't connect: $DBI::errstr";
        plan skip_all => "Can't connect to \"$ENV{DBO_TEST_PG_DB}\" database";
    }
} else {
    $drop_db = connect_and_create('dbname=postgres')
        || connect_and_create('dbname=template1')
        || connect_and_create('dbname=postgres', 'postgres');
    plan skip_all => "Can't create test database from \"postgres\" or \"template1\" databases" unless $drop_db;

    # Connect to the created database
    $dbo = Test::DBO::connect_dbo("dbname=$quoted_db") or note "Can't connect: $DBI::errstr";
    plan skip_all => "Can't connect to newly created test database: $DBI::errstr" unless $dbo;
}

plan tests => 6;
pass "Connect to PostgreSQL $quoted_db database";
isa_ok $dbo, 'DBIx::DBO::Pg', '$dbo';

# Test methods: do, select* (4 tests)
Test::DBO::basic_methods($dbo, $quoted_tbl);

END {
    if ($drop_db) {
        undef $dbo; # Make sure we're no longer connected
        if ($drop_db->do("DROP DATABASE $quoted_db")) {
            note "Dropped test database: $quoted_db";
        } else {
            diag sql_err($dbo);
        }
    }
}

