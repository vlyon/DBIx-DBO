#!perl -T

use Test::More tests => 7;

BEGIN {
	use_ok 'DBIx::DBO' or BAIL_OUT 'DBIx::DBO failed!';
}

diag "Testing DBIx::DBO $DBIx::DBO::VERSION, Perl $], $^X";
diag 'Available DBI drivers: '.join(', ', DBI->available_drivers);

is $DBIx::DBO::QuoteIdentifier, 1, 'QuoteIdentifier setting is ON by default';
import DBIx::DBO 'NoQuoteIdentifier';
is $DBIx::DBO::QuoteIdentifier, 0, "Check 'NoQuoteIdentifier' import option";
import DBIx::DBO 'QuoteIdentifier';
is $DBIx::DBO::QuoteIdentifier, 1, "Check 'QuoteIdentifier' import option";

my $dbo = DBIx::DBO->connect('DBI:DBM:', '', '', {RaiseError => 1});
ok $dbo->do(q{CREATE TABLE user (id INT, name TEXT)}), 'DBIx::DBO->do succeeded';
$dbo->do(q{INSERT INTO user VALUES (1, 'John Doe'),(2, 'Jane Smith')});
$dbo->do(q{INSERT INTO user VALUES (2, 'Jane Smith')});
ok my $rv = $dbo->selectall_arrayref('SELECT * FROM user'), 'DBIx::DBO->selectall_arrayref succeeded';
is_deeply $rv, [[1,'John Doe'],[2,'Jane Smith']], 'DBIx::DBO->selectall_arrayref returned correct data';
$dbo->do(q{DROP TABLE user});

