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

        $DBIx::DBO::Common::Config{_Debug_SQL} = $ENV{DBO_DEBUG_SQL};
        no warnings 'redefine';
        *DBIx::DBO::Common::_carp_last_sql = sub {
            my $me = shift;
            my ($cmd, $sql, @bind) = @{$me->_last_sql};
            local $Carp::Verbose = 1 if $me->config('_Debug_SQL') > 1;
            my @mess = split /\n/, Carp::shortmess("\t$cmd called");
            splice @mess, 0, 3 if $Carp::Verbose;
            Test::More::diag join "\n", "DEBUG_SQL: $sql", 'DEBUG_SQL: ('.join(', ', map $me->rdbh->quote($_), @bind).')', @mess;
        };
    }
}
use DBIx::DBO;

our $dbd;
our $dbd_name;
(our $prefix = "DBO_${DBIx::DBO::VERSION}_test") =~ s/\W/_/g;
our @_cleanup_sql;

sub import {
    my $class = shift;
    $dbd = shift;
    $dbd_name = shift;
    my %opt = splice @_;

    grep $_ eq $dbd, DBI->available_drivers or
        plan skip_all => "No $dbd driver available!";

    {
        no strict 'refs';
        *{caller().'::sql_err'} = \&sql_err;
    }

    if (exists $opt{tempdir}) {
        require File::Temp;
        my $dir = File::Temp::tempdir('tmp_XXXX', CLEANUP => 1);
        if (ref $opt{tempdir}) {
            ${$opt{tempdir}} = $dir;
        } else {
            chdir $dir or die "Can't cd to $dir: $!\n";
            eval "END { chdir '..' }";
        }
    }

    return unless exists $opt{tests};

    if (exists $opt{connect_ok}) {
        my $dbo_ref = shift @{$opt{connect_ok}};
        $$dbo_ref = connect_dbo(@{$opt{connect_ok}}) or plan skip_all => "Can't connect: $DBI::errstr";

        plan tests => $opt{tests};
        pass "Connect to $dbd_name";
        isa_ok $$dbo_ref, "DBIx::DBO::DBD::$dbd", '$dbo';
    } else {
        plan tests => $opt{tests};
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
    ok my $dbo = connect_dbo(@_), "Connect to $dbd_name" or die $DBI::errstr;
    isa_ok $dbo, "DBIx::DBO::DBD::$dbd", '$dbo';
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
    my $schema = shift;
    my $table = shift;
    my $quoted_table = $dbo->_qi($schema, $table);

    SKIP: {
        # Create a test table
        ok $dbo->do("CREATE TABLE $quoted_table (id INT, name TEXT)"), 'Method DBIx::DBO->do'
            or diag sql_err($dbo) or skip "Can't create test table $quoted_table", 8;

        # Insert data
        $dbo->do("INSERT INTO $quoted_table VALUES (1, 'John Doe')") or diag sql_err($dbo);
        $dbo->do("INSERT INTO $quoted_table VALUES (?, ?)", undef, 2, 'Jane Smith') or diag sql_err($dbo);

        # Check the DBO select* methods
        my $rv = [];
        @$rv = $dbo->selectrow_array("SELECT * FROM $quoted_table") or diag sql_err($dbo);
        is_deeply $rv, [1,'John Doe'], 'Method DBIx::DBO->selectrow_array';

        $rv = $dbo->selectrow_arrayref("SELECT * FROM $quoted_table") or diag sql_err($dbo);
        is_deeply $rv, [1,'John Doe'], 'Method DBIx::DBO->selectrow_arrayref';

        $rv = $dbo->selectall_arrayref("SELECT * FROM $quoted_table") or diag sql_err($dbo);
        is_deeply $rv, [[1,'John Doe'],[2,'Jane Smith']], 'Method DBIx::DBO->selectall_arrayref';

        # Create a table object
        my $t = $dbo->table([$schema, $table]);
        isa_ok $t, 'DBIx::DBO::Table', '$t';

        # Insert via table object
        $rv = $t->insert(id => 3, name => 'Uncle Arnie') or diag sql_err($t);
        ok $rv, 'Method DBIx::DBO::Table->insert';

        # Create a column object
        my $c = $t->column('id');
        isa_ok $c, 'DBIx::DBO::Column', '$c';

        # Advanced insert using a column object
        $rv = $t->insert($c => {FUNC => '4'}, name => \"'James Bond'") or diag sql_err($t);
        ok $rv, 'Method DBIx::DBO::Table->insert';

        # Delete via table object
        $rv = $t->delete(id => 3) or diag sql_err($t);
        is $rv, 1, 'Method DBIx::DBO::Table->delete';

        # Remove the created table during cleanup
        push @_cleanup_sql, "DROP TABLE $quoted_table";

        return $t;
    }
}

sub advanced_table_methods {
    my $dbo = shift;
    my $t = shift;

    SKIP: {
        skip "No test table for advanced table tests", 2 unless $t;

        # Advanced insert
        my $rv = $t->insert(id => { FUNC => '? + 3', VAL => 2 }, name => \"'Vernon Lyon'") or diag sql_err($t);
        ok $rv, 'Method DBIx::DBO::Table->insert (advanced)';

        $t->insert(id => 6, name => 'Harry Harrelson') or diag sql_err($t);
        $t->insert(id => 7, name => 'Amanda Huggenkiss') or diag sql_err($t);

        # Advanced delete
        $rv = $t->delete(id => \'NOT NULL', name => undef) or diag sql_err($t);
        ok $rv, 'Method DBIx::DBO::Table->delete (advanced)';
    }
}

sub skip_advanced_table_methods {
    my $dbo = shift;
    my $t = shift;

    note "No advanced table tests for $dbd_name";
    $t->insert(id => 5, name => 'Vernon Lyon') or diag sql_err($t);
    $t->insert(id => 6, name => 'Harry Harrelson') or diag sql_err($t);
    $t->insert(id => 7, name => 'Amanda Huggenkiss') or diag sql_err($t);
}

sub query_methods {
    my $dbo = shift;
    my $t = shift;
    my $quoted_table = $t->_quoted_name;

    # Create a query object
    my $q = $dbo->query($t);
    isa_ok $q, 'DBIx::DBO::Query', '$q';

    # Default sql = select everything
    my $sql = $q->sql;
    is $sql, "SELECT * FROM $quoted_table", 'Method DBIx::DBO::Query->sql';

    # Get a valid sth
    isa_ok $q->sth, 'DBI::st', '$q->sth';

    # Count the number of rows
    is $q->rows, 6, 'Row count is 6';

    # Get a Record object
    my $r = $q->fetch;
    isa_ok $r, 'DBIx::DBO::Row', '$r';

    # Access methods
    is $r->{name}, 'John Doe', 'Access row as a hashref';
    is $r->[0], 1, 'Access row as an arrayref';
    $r = $q->fetch;
    is $r->value($t->column('name')), 'Jane Smith', 'Access row via method DBIx::DBO::Row::value';
    is $r ** $t ** 'name', 'Jane Smith', 'Access row via shortcut method **';

    return $q;
}

sub advanced_query_methods {
    my $dbo = shift;
    my $t = shift;
    my $q = shift;

    # Show specific columns only
    $q->show({ FUNC => 'UPPER(?)', COL => 'name', AS => 'name' }, 'id', 'name');
    my $r = $q->fetch;
    is $r->{name}, 'JOHN DOE', 'Method DBIx::DBO::Query->show';
    is $r ** $t ** 'name', 'John Doe', 'Access specific column';

    # Show whole tables
    $q->show({ FUNC => "'who?'", AS => 'name' }, $t);
    $r = $q->fetch;
    is $r ** $t ** 'name', 'John Doe', 'Access specific column from a shown table';

    # Check case sensitivity of LIKE
    my $case_sensitive = $dbo->selectrow_arrayref('SELECT ? LIKE ?', undef, 'a', 'A') or diag sql_err($dbo);
    $case_sensitive = $case_sensitive->[0];
    note "$dbd_name 'LIKE' is".($case_sensitive ? '' : ' NOT').' case sensitive';

    # Where clause
    $q->show('id');
    ok $q->where('name', 'LIKE', '%a%'), 'Method DBIx::DBO::Query->where LIKE';
    my $a = $q->col_arrayref or diag sql_err($q);
    is_deeply $a, [2,4,6,7], 'Method DBIx::DBO::Query->col_arrayref';
    ok $q->where('id', 'BETWEEN', [2, 6]), 'Method DBIx::DBO::Query->where BETWEEN';
    $a = $q->arrayref or diag sql_err($q);
    is_deeply $a, [[2],[4],[6]], 'Method DBIx::DBO::Query->arrayref';
    ok $q->where('name', 'NOT LIKE', '%i%'), 'Method DBIx::DBO::Query->where NOT LIKE';
    $a = $q->hashref('id') or diag sql_err($q);
    is_deeply $a, {4 => {id => 4},6 => {id => 6}}, 'Method DBIx::DBO::Query->hashref';
}

sub skip_advanced_query_methods {
    note "No advanced query tests for $dbd_name";
}

sub cleanup {
    my $dbo = shift;

    note 'Doing cleanup';
    for my $sql (@_cleanup_sql) {
        $dbo->do($sql) or diag sql_err($dbo);
    }
}

1;
