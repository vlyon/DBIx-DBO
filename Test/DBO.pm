use Test::More;

package # Hide from PAUSE
    Test::DBO;

use 5.010_000;
use strict;
use warnings;

use Scalar::Util qw(blessed reftype);
use Test::More;
BEGIN {
    # If we are using a version of Test::More older than 0.82 ...
    unless (exists $Test::More::{note}) {
        eval q#
            sub Test::More::note {
                local $Test::Builder::{_print_diag} = $Test::Builder::{_print};
                Test::More->builder->diag(@_);
            }
            *note = \&Test::More::note;
            no strict 'refs';
            *{caller(2).'::note'} = \&note;
        #;
        die $@ if $@;
    }

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
        isa_ok $$dbo_ref, "DBIx::DBO::DBD::${dbd}::Handle", '$dbo';
    } else {
        plan tests => $opt{tests};
    }
}

sub sql_err {
    my $obj = shift;

    my $errstr = $DBI::errstr or return;
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
    isa_ok $dbo, "DBIx::DBO::DBD::${dbd}::Handle", '$dbo';
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
    my $t;

    # Create a test table with a multi-column primary key
    if ($dbo->do("CREATE TABLE $quoted_table (name TEXT, id INT, type VARCHAR(8), PRIMARY KEY (type, id))")) {
        pass 'Create a test table';

        # Create a table object
        $t = $dbo->table([$schema, $table]);
        isa_ok $t, 'DBIx::DBO::Table', '$t';

        # Check the Primary Keys
        is_deeply $t->{PrimaryKeys}, ['type', 'id'], 'Check PrimaryKeys';

        # Recreate our test table
        $dbo->do("DROP TABLE $quoted_table") && $dbo->do("CREATE TABLE $quoted_table (id INT, name TEXT)")
            or diag sql_err($dbo) or die "Can't recreate the test table!\n";
        $dbo->_get_table_info($t->{Schema}, $t->{Name});
        $t = $dbo->table([$schema, $table]);
    }
    else {
        diag sql_err($dbo);
        SKIP: {
            skip "Can't create a multi-column primary key", 1;
        }

        # Create our test table
        ok $dbo->do("CREATE TABLE $quoted_table (id INT, name TEXT)"), 'Create our test table'
            or diag sql_err($dbo) or die "Can't create the test table!\n";

        # Create our table object
        $t = $dbo->table([$schema, $table]);
        isa_ok $t, 'DBIx::DBO::Table', '$t';
    }
    pass 'Method DBIx::DBO->do';

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

    # Insert via table object
    $rv = $t->insert(id => 3, name => 'Uncle Arnie') or diag sql_err($t);
    ok $rv, 'Method DBIx::DBO::Table->insert';

    # Create a column object
    my $c = $t->column('id');
    isa_ok $c, 'DBIx::DBO::Column', '$c';

    # Advanced insert using a column object
    $rv = $t->insert($c => {FUNC => '4'}, name => \"'James Bond'") or diag sql_err($t);
    ok $rv, 'Method DBIx::DBO::Table->insert';

    # Fetch one value from the Table
    is $t->fetch_value($t ** 'name', id => 3), 'Uncle Arnie', 'Method DBIx::DBO::Table->fetch_value';

    # Fetch one value from the Table
    is_deeply $t->fetch_hash(id => 3), {id=>3,name=>'Uncle Arnie'}, 'Method DBIx::DBO::Table->fetch_hash';

    # Fetch one value from the Table
    my $r = $t->fetch_row(id => 3);
    is $r->{name}, 'Uncle Arnie', 'Method DBIx::DBO::Table->fetch_row';

    # Fetch a column arrayref from the Table
    is_deeply $t->fetch_column($t ** 'name', id => 3), ['Uncle Arnie'], 'Method DBIx::DBO::Table->fetch_column';

    # Delete via table object
    $rv = $t->delete(id => 3) or diag sql_err($t);
    is $rv, 1, 'Method DBIx::DBO::Table->delete';

    # Remove the created table during cleanup
    push @_cleanup_sql, "DROP TABLE $quoted_table";

    return $t;
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

sub row_methods {
    my $dbo = shift;
    my $t = shift;

    my $r = $dbo->row($t);
    isa_ok $r, 'DBIx::DBO::Row', '$r';

    is $$r->{array}, undef, 'Row is empty';

    ok $r->load(id => 2, name => 'Jane Smith'), 'Method DBIx::DBO::Row->load' or diag sql_err($r);
    is_deeply $$r->{array}, [ 2, 'Jane Smith' ], 'Row loaded correctly';

$r->config(DEBUG_SQL => 1);
    is $r->update(name => 'Someone Else'), 1, 'Method DBIx::DBO::Row->update' or diag sql_err($r);
    is $$r->{array}, undef, 'Row is empty again';
    is_deeply \@{$r->load(id => 2)}, [ 2, 'Someone Else' ], 'Row updated correctly' or diag sql_err($r);

    ok $r->delete, 'Method DBIx::DBO::Row->delete' or diag sql_err($r);
    $t->insert(id => 2, name => 'Jane Smith');

    is $r->load(name => 'non-existent'), undef, 'Load non-existent row';
    is_deeply $$r->{array}, undef, 'Row is empty again';
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

    # Sort the result
    $q->order_by('id');
    pass 'Method DBIx::DBO::Query->order_by';

    # Get a valid sth
    isa_ok $q->sth, 'DBI::st', '$q->sth';

    # Get a Row object
    my $r = $q->row;
    isa_ok $r, 'DBIx::DBO::Row', '$q->row';
    my $r_str = "$r";

    # Remove the reference so that the row wont detach
    undef $r;

    # Fetch the first row
    $r = $q->fetch;
    ok $r->isa('DBIx::DBO::Row'), 'Method DBIx::DBO::Query->fetch';
    is $r_str, "$r", 'Re-use the same row object';

    # Access methods
    is $r->{name}, 'John Doe', 'Access row as a hashref';
    is $r->[0], 1, 'Access row as an arrayref';

    # Fetch another row
    $r_str = "$r";
    $r = $q->fetch;
    isnt $r_str, "$r", 'Row detaches during fetch when a ref still exists';

    # More access methods
    is $r->value($t->column('name')), 'Jane Smith', 'Access row via method DBIx::DBO::Row::value';
    is $r ** $t ** 'name', 'Jane Smith', 'Access row via shortcut method **';

    # Count the number of rows
    1 while $q->fetch;
    is $q->rows, 6, 'Row count is 6';

    $q->finish;
    return $q;
}

sub advanced_query_methods {
    my $dbo = shift;
    my $t = shift;
    my $q = shift;

    # Show specific columns only
    $q->show({ FUNC => 'UPPER(?)', COL => 'name', AS => 'name' }, 'id', 'name');
    is $q->fetch->{name}, 'JOHN DOE', 'Method DBIx::DBO::Query->show';
    is $q->row ** $t ** 'name', 'John Doe', 'Access specific column';

    # Show whole tables
    $q->show({ FUNC => "'who?'", AS => 'name' }, $t);
    is $q->fetch ** $t ** 'name', 'John Doe', 'Access specific column from a shown table';

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

    $q->finish;
}

sub skip_advanced_query_methods {
    note "No advanced query tests for $dbd_name";
}

sub join_methods {
    my $dbo = shift;
    my $table = shift;
    my $skip_multi = shift;

    my ($q, $t1, $t2) = $dbo->query($table, $table);
    $q->limit(3);
    $q->config(CalcFoundRows => 1);
    ok $q, 'Comma JOIN';
    is $q->count_rows, 3, 'Method DBIx::DBO::Query->count_rows' or diag sql_err($q);
    is $q->found_rows, 36, 'Method DBIx::DBO::Query->found_rows' or diag sql_err($q);

    $q->join_on($t2, $t1 ** 'id', '=', { FUNC => '?/2.0', VAL => $t2 ** 'id' });
    $q->order_by({ COL => $t1 ** 'name', ORDER => 'DESC' });
    $q->where($t1 ** 'name', '<', $t2 ** 'name', FORCE => 'OR');
    $q->where($t1 ** 'name', '>', $t2 ** 'name', FORCE => 'OR');
    my $r;
    SKIP: {
        $r = $q->fetch or diag sql_err($q) or fail 'JOIN ON' or skip 'No Left Join', 1;

        is_deeply \@$r, [ 1, 'John Doe', 2, 'Jane Smith' ], 'JOIN ON';
        $r->load($t1 ** id => 2) or diag sql_err($r);
        is_deeply \@$r, [ 2, 'Jane Smith', 4, 'James Bond' ], 'Method DBIx::DBO::Row->load';
    }

    ($q, $t1) = $dbo->query($table);
    $t2 = $q->join_table($table, 'left');
    $q->join_on($t2, $t1 ** 'id', '=', { FUNC => '?/2.0', COL => $t2 ** 'id' });
    $q->order_by({ COL => $t1 ** 'name', ORDER => 'DESC' });
    $q->limit(1, 3);

    SKIP: {
        $r = $q->fetch or diag sql_err($q) or fail 'LEFT JOIN' or skip 'No Left Join', 3;

        is_deeply \@$r, [ 4, 'James Bond', undef, undef ], 'LEFT JOIN';
        is $r->_column_idx($t2 ** 'id'), 2, 'Method DBIx::DBO::Row->_column_idx';
        is $r->value($t2 ** 'id'), undef, 'Method DBIx::DBO::Row->value';

        # Update the LEFT JOINed row
        SKIP: {
            skip "Mutli-table UPDATE is not supported by $dbd_name", 1 if $skip_multi;
            ok $r->update($t1 ** 'name' => 'Vernon Wayne Lyon'), 'Method DBIx::DBO::Row->update' or diag sql_err($r);
        }
    }

    $q->finish;
}

sub cleanup {
    my $dbo = shift;

    note 'Doing cleanup';
    for my $sql (@_cleanup_sql) {
        $dbo->do($sql) or diag sql_err($dbo);
    }
}

my @_no_recursion;
sub Dump {
    my $val = shift;
    my $var = shift;
    if (blessed $val and !defined $var) {
        if ($val->isa('DBIx::DBO')) {
            $var = 'dbo';
        } elsif ($val->isa('DBIx::DBO::Table')) {
            $var = 't';
        } elsif ($val->isa('DBIx::DBO::Query')) {
            $var = 'q';
        } elsif ($val->isa('DBIx::DBO::Row')) {
            $var = 'r';
        }
    }
    $var //= 'dump';
    require Data::Dumper;
    my $d = Data::Dumper->new([$val], [$var]);
    my %seen;
    @_no_recursion = ($val);
    given (reftype $val) {
        when ('ARRAY') { _Find_Seen(\%seen, $_) for @$val }
        when ('HASH')  { _Find_Seen(\%seen, $_) for values %$val }
        when ('REF')   { _Find_Seen(\%seen, $$val) }
    }
    $d->Seen(\%seen);
    warn $d->Dump;
}

sub _Find_Seen {
    my $seen = shift;
    my $val = shift;
    return unless ref $val;
    for (@_no_recursion) {
        return if $val == $_;
    }
    push @_no_recursion, $val;

    if (blessed $val) {
        if ($val->isa('DBIx::DBO')) {
            $seen->{dbo} = $val;
            return;
        } elsif ($val->isa('DBIx::DBO::Table')) {
            my $t = 1;
            while (my ($k, $v) = each %$seen) {
                next if $k !~ /^t\d+$/;
                return if $val == $v;
                $t++;
            }
            $seen->{"t$t"} = $val;
            return;
        } elsif ($val->isa('DBIx::DBO::Query')) {
            $seen->{q} = $val;
            return;
        } elsif ($val->isa('DBIx::DBO::Row')) {
            $seen->{r} = $val;
            return;
        }
    }
    given (reftype $val) {
        when ('ARRAY') { _Find_Seen($seen, $_) for @$val }
        when ('HASH')  { _Find_Seen($seen, $_) for values %$val }
        when ('REF')   { _Find_Seen($seen, $$val) }
    }
}

1;
