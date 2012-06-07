use strict;
use warnings;

my $dbo;
use Test::DBO mysql => 'MySQL', try_connect => \$dbo;

# Try to ensure a connection by guessing
$dbo ||= Test::DBO::connect_dbo('test', 'root') || Test::DBO::connect_dbo('test')
    || Test::DBO::connect_dbo('', 'root') || Test::DBO::connect_dbo('')
        or plan skip_all => "Can't connect: $DBI::errstr";

my $quoted_db = $dbo->{dbd_class}->_qi($dbo, $Test::DBO::test_db);
if ($dbo->do("CREATE DATABASE $quoted_db CHARACTER SET utf8")) {
    Test::DBO::todo_cleanup("DROP DATABASE $quoted_db");
    $dbo->do("USE $quoted_db");
} else {
    my $msg = "Can't create the test database: $DBI::errstr";
    unless ($Test::DBO::test_db = $dbo->selectrow_array('SELECT DATABASE()')) {
        undef $dbo;
        plan skip_all => $msg;
    }
    $quoted_db = $dbo->{dbd_class}->_qi($dbo, $Test::DBO::test_db);
}

plan tests => 97;

# Create the DBO (3 tests)
pass "Connect to MySQL $quoted_db database";
isa_ok $dbo, 'DBIx::DBO', '$dbo';
ok $dbo->do('SET NAMES utf8'), 'SET NAMES utf8' or diag sql_err($dbo);

# In MySQL the Schema is the DB
$Test::DBO::test_sch = $Test::DBO::test_db;
$Test::DBO::can{collate} = 'BINARY';
$Test::DBO::can{multi_table_update} = 1;
$Test::DBO::can{auto_increment_id} = 'INT NOT NULL AUTO_INCREMENT PRIMARY KEY';

# Table methods: do, select* (22 tests)
my $t = Test::DBO::basic_methods($dbo);

# Advanced table methods: insert, update, delete (2 tests)
Test::DBO::advanced_table_methods($dbo, $t);

# Row methods: (15 tests)
Test::DBO::row_methods($dbo, $t);

# Query methods: (23 tests)
my $q = Test::DBO::query_methods($dbo, $t);

# MySQL CalcFoundRows: (2 tests)
$q->limit(2);
$q->config(CalcFoundRows => 1);
like $q->sql, qr/ SQL_CALC_FOUND_ROWS /, 'Use SQL_CALC_FOUND_ROWS in MySQL';
$q->found_rows;
is $q->{LastSQL}[1], 'SELECT FOUND_ROWS()', 'Use FOUND_ROWS() in MySQL';

# Advanced query methods: (11 tests)
Test::DBO::advanced_query_methods($dbo, $t, $q);

# MySQL CalcFoundRows: (2 tests)
$q->limit(2);
$q->config(CalcFoundRows => 1);
like $q->sql, qr/ SQL_CALC_FOUND_ROWS /, 'Use SQL_CALC_FOUND_ROWS in MySQL';
$q->found_rows;
is $q->{LastSQL}[1], 'SELECT FOUND_ROWS()', 'Use FOUND_ROWS() in MySQL';

# Join methods: (10 tests)
Test::DBO::join_methods($dbo, $t->{Name});

END {
    # Cleanup (1 test)
    Test::DBO::cleanup($dbo) if $dbo;
}

