#!perl -T

use Test::More;
use DBIx::DBO;

if (grep $_ eq 'mysql', DBI->available_drivers) {
    plan tests => 20;
} else {
    plan skip_all => 'No mysql driver available!';
}

sub err($) {
    return 1 unless defined $DBI::errstr;
    diag $DBI::errstr;
    my ($cmd, $sql, @bind) = @{$_[0]->_last_sql};
    diag $sql;
    diag '('.join(', ', map $_[0]->rdbh->quote($_), @bind).')';
    1;
}

# Create the DBO
ok my $dbo = DBIx::DBO->connect('DBI:mysql:', '', '', {RaiseError => 0}), 'Connect to MySQL' or die $DBI::errstr;
isa_ok $dbo, 'DBIx::DBO::mysql';
ok $dbo->do('SET NAMES utf8'), 'SET NAMES utf8' or err($dbo);

(my $prefix = 'DBO_'.$DBIx::DBO::VERSION) =~ s/\W/_/g;
my $test_db = $ENV{DBO_TEST_MYSQL_DB} || $prefix.'_test_db';
my $test_tbl = $prefix.'tbl';
my $quoted_db = $dbo->_qi($test_db);
my $quoted_tbl = $dbo->_qi($test_tbl);

# Create a test database
ok $dbo->do("CREATE DATABASE $quoted_db CHARACTER SET utf8"), "Create database $quoted_db" or die $DBI::errstr;
ok $dbo->do("USE $quoted_db"), "USE $quoted_db" or err($dbo);

SKIP: {

    is $dbo->selectrow_array('SELECT DATABASE()'), $test_db, 'Correct DB selected'
        or err($dbo) && skip 'Incorrect DB selected!', 21;

    ok $dbo->do(qq{
            CREATE TABLE $quoted_tbl (
                `id` int(11) NOT NULL auto_increment,
                `Name` varchar(20) default NULL,
                `Status` enum('One','Two','Three','Four') NOT NULL,
                `Boss` int(11) NOT NULL,
                PRIMARY KEY  (`id`),
                UNIQUE KEY `Name` (`Name`)
            )
        }), "Create table $quoted_tbl";
#warn $dbo->dbh->{Statement};

    ok $dbo->do("DROP TABLE $quoted_tbl"), "Drop table $quoted_tbl" or err($dbo);
}

ok $dbo->do("DROP DATABASE $quoted_db"), "Drop database $quoted_db" or die $DBI::errstr;
exit;
# Create a test table
ok $dbo->do("CREATE TABLE $prefix (id INT, name TEXT)"), 'DBIx::DBO->do succeeded';
$dbo->do("INSERT INTO $prefix VALUES (1, 'John Doe')");
$dbo->do("INSERT INTO $prefix VALUES (2, 'Jane Smith')");

# Check the DBO select* methods
ok my @rv = $dbo->selectrow_array("SELECT * FROM $prefix"), 'DBIx::DBO->selectrow_array succeeded';
is_deeply \@rv, [1,'John Doe'], 'DBIx::DBO->selectrow_array returned correct data';

ok my $rv = $dbo->selectrow_arrayref("SELECT * FROM $prefix"), 'DBIx::DBO->selectrow_arrayref succeeded';
is_deeply $rv, [1,'John Doe'], 'DBIx::DBO->selectrow_arrayref returned correct data';

ok $rv = $dbo->selectall_arrayref("SELECT * FROM $prefix"), 'DBIx::DBO->selectall_arrayref succeeded';
is_deeply $rv, [[1,'John Doe'],[2,'Jane Smith']], 'DBIx::DBO->selectall_arrayref returned correct data';

# Remove the created table
$dbo->do("DROP TABLE $prefix");

