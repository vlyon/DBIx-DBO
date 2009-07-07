use Test::More;

package Test::DBO;

use strict;
use warnings;
use Test::More;
use DBIx::DBO;

our $dbd;
(our $prefix = "DBO_${DBIx::DBO::VERSION}_test") =~ s/\W/_/g;

sub import {
    my $class = shift;
    $dbd = shift;
    my $tests = shift;

    if (grep $_ eq $dbd, DBI->available_drivers) {
        plan tests => $tests;
    } else {
        plan skip_all => "No $dbd driver available!";
    }

    no strict 'refs';
    *{caller().'::sql_err'} = \&sql_err;
}

sub sql_err {
    my $obj = shift;

    my $errstr = $DBI::errstr;
    my ($cmd, $sql, @bind) = @{$obj->_last_sql};
    $sql =~ s/^/  /mg;
    my @err = ('SQL command failed:', $sql.';');
    push @err, 'Bind Values: ('.join(', ', map $obj->rdbh->quote($_), @bind).')' if @bind;
    push @err, $errstr || '???';
    $err[-1] =~ s/ at line \d+$//;
    join "\n", @err;
}

sub connect_dbo {
    ok my $dbo = DBIx::DBO->connect("DBI:$dbd:", '', '', {RaiseError => 0}), "Connect to $dbd" or die $DBI::errstr;
    isa_ok $dbo, "DBIx::DBO::$dbd", '$dbo';
    $dbo;
}

sub basic_methods {
    my $dbo = shift;
    my $quoted_tbl = shift;

    SKIP: {
        # Create a test table
        ok $dbo->do("CREATE TABLE $quoted_tbl (id INT, name TEXT)"), 'Method DBIx::DBO->do'
            or diag sql_err($dbo) or skip "Can't create test table $quoted_tbl", 3;

        # Insert data
        $dbo->do("INSERT INTO $quoted_tbl VALUES (1, 'John Doe')") or diag sql_err($dbo);
#        $dbo->do("INSERT INTO $quoted_tbl VALUES (2, 'Jane Smith')") or diag sql_err($dbo);
        $dbo->do("INSERT INTO $quoted_tbl VALUES (?, ?)", undef, 2, 'Jane Smith') or diag sql_err($dbo);

        # Check the DBO select* methods
        my $rv = [];
        @$rv = $dbo->selectrow_array("SELECT * FROM $quoted_tbl") or diag sql_err($dbo);
        is_deeply $rv, [1,'John Doe'], 'Method DBIx::DBO->selectrow_array';

        $rv = $dbo->selectrow_arrayref("SELECT * FROM $quoted_tbl") or diag sql_err($dbo);
        is_deeply $rv, [1,'John Doe'], 'Method DBIx::DBO->selectrow_arrayref';

        $rv = $dbo->selectall_arrayref("SELECT * FROM $quoted_tbl") or diag sql_err($dbo);
        is_deeply $rv, [[1,'John Doe'],[2,'Jane Smith']], 'Method DBIx::DBO->selectall_arrayref';

        # Remove the created table
        $dbo->do("DROP TABLE $quoted_tbl") or diag sql_err($dbo);
    }
}

1;
