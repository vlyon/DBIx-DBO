use Test::More 0.82;

package Test::DBO;

use 5.010_000;
use strict;
use warnings;

use Test::More;
BEGIN {
    # Set up _Debug_SQL if requested
    require DBIx::DBO::Common;

    if ($ENV{DBO_DEBUG_SQL}) {
        diag "DBO_DEBUG_SQL=$ENV{DBO_DEBUG_SQL}";
        package DBIx::DBO::Common;

        $DBIx::DBO::Common::_Debug_SQL = $ENV{DBO_DEBUG_SQL};
        no warnings 'redefine';
        *DBIx::DBO::Common::_carp_last_sql = sub {
            my $me = shift;
            my ($cmd, $sql, @bind) = @{$me->_last_sql};
            local $Carp::Verbose = 1 if $DBIx::DBO::Common::_Debug_SQL > 1;
            my @mess = split /\n/, Carp::shortmess("\t$cmd called");
            splice @mess, 0, 3 if $Carp::Verbose;
            Test::More::diag join "\n", "DEBUG_SQL: $sql", 'DEBUG_SQL: ('.join(', ', map $me->rdbh->quote($_), @bind).')', @mess;
        };
    }
}
use DBIx::DBO;

our $dbd;
(our $prefix = "DBO_${DBIx::DBO::VERSION}_test") =~ s/\W/_/g;

sub import {
    my $class = shift;
    $dbd = shift;
    my $tests = shift;
    my %opt = splice @_;

    grep $_ eq $dbd, DBI->available_drivers or
        plan skip_all => "No $dbd driver available!";

    {
        no strict 'refs';
        *{caller().'::sql_err'} = \&sql_err;
    }

    return unless $tests;

    if (exists $opt{connect_ok}) {
        my $dbo_ref = shift @{$opt{connect_ok}};
        $$dbo_ref = connect_dbo(@{$opt{connect_ok}}) or plan skip_all => "Can't connect: $DBI::errstr";

        plan tests => $tests;
        pass "Connect to $dbd";
        isa_ok $$dbo_ref, "DBIx::DBO::$dbd", '$dbo';
    } else {
        plan tests => $tests;
    }
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

sub connect_ok {
    ok my $dbo = connect_dbo(@_), "Connect to $dbd" or die $DBI::errstr;
    isa_ok $dbo, "DBIx::DBO::$dbd", '$dbo';
    return $dbo;
}

sub connect_dbo {
    my $dsn = shift // $ENV{'DBO_TEST_'.uc($dbd).'_DB'} // '';
    my $user = shift // $ENV{'DBO_TEST_'.uc($dbd).'_USER'};
    my $pass = shift // $ENV{'DBO_TEST_'.uc($dbd).'_PASS'};

    DBIx::DBO->connect("DBI:$dbd:$dsn", $user, $pass, {RaiseError => 0});
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
