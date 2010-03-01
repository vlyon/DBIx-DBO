use strict;
use warnings;

my $dbo;
use Test::DBO Pg => 'PostgreSQL', try_connect => \$dbo;

my $drop_db;
my $quoted_db;

sub connect_and_create {
    if (my $dbo = Test::DBO::connect_dbo(@_)) {
        # Create a test database
        $quoted_db = $dbo->_qi($Test::DBO::test_db);
        if ($dbo->do("CREATE DATABASE $quoted_db")) {
            note "Created $quoted_db test database";
            return $dbo;
        }
        undef $quoted_db;
        note sql_err($dbo);
        return;
    }
    my $msg = $_[0] =~ /dbname=(.*)/ ? " to $1 database" : '';
    note "Can't connect$msg: $DBI::errstr";
    return;
}

unless ($dbo) {
    # Try to connect to the default DB
    $drop_db = connect_and_create('dbname=postgres')
        || connect_and_create('dbname=template1')
        || connect_and_create('dbname=postgres', 'postgres')
        || connect_and_create('');
    if ($drop_db) {
        # Connect to the created database
        $dbo = Test::DBO::connect_dbo("dbname=$quoted_db") or note "Can't connect: $DBI::errstr";
        plan skip_all => "Can't connect to test database: $DBI::errstr" unless $dbo;
    } else {
        $dbo = Test::DBO::connect_dbo('dbname=test')
            or plan skip_all => "Can't connect: $DBI::errstr";
    }
}
unless ($quoted_db) {
    $Test::DBO::test_db = $dbo->selectrow_array('SELECT current_database()');
    $quoted_db = $dbo->_qi($Test::DBO::test_db);
}

plan tests => 65;
pass "Connect to PostgreSQL $quoted_db database";
isa_ok $dbo, 'DBIx::DBO::DBD::Pg', '$dbo';

# Create the schema
my $drop_sch;
my $quoted_sch = $dbo->_qi($Test::DBO::test_sch);
if (ok $dbo->do("CREATE SCHEMA $quoted_sch"), "Create $quoted_sch test schema") {
    Test::DBO::todo_cleanup("DROP SCHEMA $quoted_sch");
} else {
    note sql_err($dbo);
}

# Table methods: do, select*, ... (15 tests)
my $t = Test::DBO::basic_methods($dbo);

# Advanced table methods: insert, update, delete (2 tests)
Test::DBO::advanced_table_methods($dbo, $t);

# Row methods: (10 tests)
Test::DBO::row_methods($dbo, $t);

# Query methods: (16 tests)
my $q = Test::DBO::query_methods($dbo, $t);

# Advanced query methods: (9 tests)
Test::DBO::advanced_query_methods($dbo, $t, $q);

# Join methods: (9 tests)
Test::DBO::join_methods($dbo, $t->{Name}, 1);

END {
    # Cleanup (1 test)
    Test::DBO::cleanup($dbo) if $dbo;

    if ($drop_db) {
        undef $dbo; # Make sure we're no longer connected
        if ($drop_db->do("DROP DATABASE $quoted_db")) {
            note "Dropped $quoted_db test database";
        } else {
            diag sql_err($drop_db);
        }
    }
}

