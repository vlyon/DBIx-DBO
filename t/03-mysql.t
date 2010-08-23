use strict;
use warnings;

my $dbo;
use Test::DBO mysql => 'MySQL', try_connect => \$dbo;

# Try to ensure a connection by guessing
$dbo ||= Test::DBO::connect_dbo('test', 'root') || Test::DBO::connect_dbo('test')
    || Test::DBO::connect_dbo('', 'root') || Test::DBO::connect_dbo('')
        or plan skip_all => "Can't connect: $DBI::errstr";

my $quoted_db = $dbo->_qi($Test::DBO::test_db);
if ($dbo->do("CREATE DATABASE $quoted_db CHARACTER SET utf8")) {
    Test::DBO::todo_cleanup("DROP DATABASE $quoted_db");
    $dbo->do("USE $quoted_db");
} else {
    my $msg = "Can't create the test database: $DBI::errstr";
    unless ($Test::DBO::test_db = $dbo->selectrow_array('SELECT DATABASE()')) {
        undef $dbo;
        plan skip_all => $msg;
    }
    $quoted_db = $dbo->_qi($Test::DBO::test_db);
}

plan tests => 67;

# Create the DBO (3 tests)
pass "Connect to MySQL $quoted_db database";
isa_ok $dbo, 'DBIx::DBO::DBD::mysql', '$dbo';
ok $dbo->do('SET NAMES utf8'), 'SET NAMES utf8' or diag sql_err($dbo);

# In MySQL the Schema is the DB
$Test::DBO::test_sch = $Test::DBO::test_db;
$Test::DBO::can{collate} = 'BINARY';
$Test::DBO::can{multi_table_update} = 1;

# Table methods: do, select* (15 tests)
my $t = Test::DBO::basic_methods($dbo);

# Advanced table methods: insert, update, delete (2 tests)
Test::DBO::advanced_table_methods($dbo, $t);

# Row methods: (10 tests)
Test::DBO::row_methods($dbo, $t);

# Query methods: (16 tests)
my $q = Test::DBO::query_methods($dbo, $t);

# Advanced query methods: (10 tests)
Test::DBO::advanced_query_methods($dbo, $t, $q);

# Join methods: (9 tests)
Test::DBO::join_methods($dbo, $t->{Name});

END {
    # Cleanup (1 test)
    Test::DBO::cleanup($dbo) if $dbo;
}

