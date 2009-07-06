#!perl -T

use Test::More;
use DBIx::DBO;

if (grep $_ eq 'SQLite', DBI->available_drivers) {
    plan tests => 9;
} else {
    plan skip_all => 'No SQLite driver available!';
}

(my $prefix = 'DBO_'.$DBIx::DBO::VERSION) =~ s/\W/_/g;

# Create the DBO
ok my $dbo = DBIx::DBO->connect('DBI:SQLite:', '', '', {RaiseError => 1}), 'Connect to SQLite' or die $DBI::errstr;
isa_ok $dbo, 'DBIx::DBO::SQLite';

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

