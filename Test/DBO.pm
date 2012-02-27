use Test::More;

package # Hide from PAUSE
    Test::DBO;

use 5.008;
use strict;
use warnings;

use Scalar::Util qw(blessed reftype);
use Test::More;
use DBIx::DBO;
BEGIN {
    require Carp::Heavy if $Carp::VERSION < 1.12;

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

    # Set up DebugSQL if requested
    if ($ENV{DBO_DEBUG_SQL}) {
        diag "DBO_DEBUG_SQL=$ENV{DBO_DEBUG_SQL}";
        DBIx::DBO->config(DebugSQL => $ENV{DBO_DEBUG_SQL});
    }

    # Set up $Carp::Verbose if requested
    if ($ENV{DBO_CARP_VERBOSE}) {
        diag "DBO_CARP_VERBOSE=$ENV{DBO_CARP_VERBOSE}";
        $Carp::Verbose = $ENV{DBO_CARP_VERBOSE};
    }

    # Store the last SQL executed, and show debug info
    {
        package DBIx::DBO::Common;
        no warnings 'redefine';
        *DBIx::DBO::Common::_sql = sub {
            my $me = shift;
            my $loc = Carp::short_error_loc();
            my %i = Carp::caller_info($loc);
            @{(Scalar::Util::reftype($me) eq 'REF' ? $$me : $me)->{LastSQL}} = ($i{'sub'}, @_);
            my $dbg = $me->config('DebugSQL') or return;
            my $trace;
            if ($dbg > 1) {
                $trace = "\t$i{sub_name} called at $i{file} line $i{line}\n";
                $trace .= "\t$i{sub_name} called at $i{file} line $i{line}\n" while %i = Carp::caller_info(++$loc);
            } else {
                $trace = "\t$i{sub} called at $i{file} line $i{line}\n";
            }
            my $sql = shift;
            Test::More::diag "DEBUG_SQL: $sql\nDEBUG_SQL: (".join(', ', map $me->rdbh->quote($_), @_).")\n".$trace;
        };
    }
}

our $dbd;
our $dbd_name;
(our $test_db = "DBO_${DBIx::DBO::VERSION}_test_db") =~ s/\W/_/g;
(our $test_sch = "DBO_${DBIx::DBO::VERSION}_test_sch") =~ s/\W/_/g;
(our $test_tbl = "DBO_${DBIx::DBO::VERSION}_test_tbl") =~ s/\W/_/g;
our @_cleanup_sql;
our $case_sensitivity_sql = 'SELECT ? LIKE ?';
our %can;

sub import {
    my $class = shift;
    $dbd = shift or return;
    $dbd_name = shift;
    my %opt = splice @_;

    grep $_ eq $dbd, DBI->available_drivers
        or plan skip_all => "No $dbd driver available!";

    # Catch install_driver errors
    eval { DBI->install_driver($dbd) };
    if ($@) {
        die $@ if $@ !~ /\binstall_driver\b/;
        plan skip_all => $@;
    }

    # Skip tests with missing module requirements
    unless (eval { DBIx::DBO->_require_dbd_class($dbd) }) {
        if ($@ =~ /^Can't locate ([\w\/]+)\.pm in \@INC /m) {
            # Module is not installed
            ($_ = $1) =~ s'/'::'g;
        } elsif ($@ =~ /^([\w:]+ version [\d\.]+) required/m) {
            # Module is not correct version
            ($_ = $1);
        } elsif ($@ =~ /^\Q$dbd_name\E is not yet supported/m) {
            # DBM is not yet supported
            plan skip_all => "Can't load $dbd driver: $dbd_name is not yet supported";
        } else {
            die $@;
        }
        plan skip_all => "Can't load $dbd driver: $_ is required";
    }

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

    # Query tests must produce the same result regardless of caching
    DBIx::DBO->config(CacheQuery => defined $ENV{DBO_CACHE_QUERY} ? $ENV{DBO_CACHE_QUERY} : int rand 2);

    if (exists $opt{try_connect}) {
        try_to_connect($opt{try_connect});
    }

    return unless exists $opt{tests};

    if (exists $opt{connect_ok}) {
        my $dbo = connect_ok(@{$opt{connect_ok}}) or plan skip_all => "Can't connect: $DBI::errstr";

        plan tests => $opt{tests};
        pass "Connect to $dbd_name";
        isa_ok $dbo, "DBIx::DBO::DBD::${dbd}", '$dbo';
    } else {
        plan tests => $opt{tests};
    }
}

sub sql_err {
    my $me = shift;
    my ($cmd, $sql, @bind) = @{(Scalar::Util::reftype($me) eq 'REF' ? $$me : $me)->{LastSQL}};
    $sql =~ s/^/  /mg;
    my @err = ($DBI::errstr || $me->rdbh->errstr || '???');
    unshift @err, 'Bind Values: ('.join(', ', map $me->rdbh->quote($_), @bind).')' if @bind;
    unshift @err, "SQL command failed: $cmd", $sql.';';
    $err[-1] =~ s/ at line \d+$//;
    join "\n", @err;
}

sub connect_dbo {
    my ($dsn, $user, $pass) = @_;
    defined $dsn or $dsn = '';
    DBIx::DBO->connect("DBI:$dbd:$dsn", $user, $pass, {RaiseError => 0});
}

sub try_to_connect {
    my $dbo_ref = shift;
    my @env = map $ENV{"DBO_TEST_\U$dbd\E_$_"}, qw(DSN USER PASS);
    if (grep defined, @env) {
        return $$dbo_ref if $$dbo_ref = connect_dbo(@env);
        plan skip_all => "Can't connect: $DBI::errstr";
    }
    return undef;
}

sub connect_ok {
    my $dbo_ref = shift;
    return try_to_connect($dbo_ref) || ($$dbo_ref = connect_dbo(@_));
}

sub basic_methods {
    my $dbo = shift;

    note 'Testing with: CacheQuery => '.DBIx::DBO->config('CacheQuery');

    # Create a DBO from DBI handles
    isa_ok(DBIx::DBO->new($dbo->{dbh}, $dbo->{rdbh}), 'DBIx::DBO', 'Method DBIx::DBO->new, $dbo');

    my $quoted_table = $dbo->_qi($test_sch, $test_tbl);
    my @quoted_cols = map $dbo->_qi($_), qw(type id name);
    my $t;
    my $create_table = "CREATE TABLE $quoted_table ($quoted_cols[1] ".
        ($can{auto_increment_id} || 'INT NOT NULL').", $quoted_cols[2] VARCHAR(20))";

    # Create a test table with a multi-column primary key
    if ($dbo->do("CREATE TABLE $quoted_table ($quoted_cols[2] VARCHAR(20), $quoted_cols[1] INT, $quoted_cols[0] VARCHAR(8), PRIMARY KEY ($quoted_cols[0], $quoted_cols[1]))")) {
        pass 'Create the test table';

        # Create a table object
        $t = $dbo->table([$test_sch, $test_tbl]);
        isa_ok $t, 'DBIx::DBO::Table', '$t';

        # Check the Primary Keys
        is_deeply $t->{PrimaryKeys}, ['type', 'id'], 'Check PrimaryKeys';

        # Recreate our test table
        $dbo->do("DROP TABLE $quoted_table") && $dbo->do($create_table)
            or diag sql_err($dbo) or die "Can't recreate the test table!\n";

        # Remove the created table during cleanup
        todo_cleanup("DROP TABLE $quoted_table");

        $dbo->_get_table_info($t->{Schema}, $t->{Name});
        $t = $dbo->table([$test_sch, $test_tbl]);
    }
    else {
        diag sql_err($dbo);
        SKIP: {
            skip "Can't create a multi-column primary key", 1;
        }

        # Create the test table
        ok $dbo->do($create_table), 'Create the test table'
            or diag sql_err($dbo) or die "Can't create the test table!\n";

        # Remove the created table during cleanup
        todo_cleanup("DROP TABLE $quoted_table");

        # Create a table object
        $t = $dbo->table([$test_sch, $test_tbl]);
        isa_ok $t, 'DBIx::DBO::Table', '$t';
    }
    die "Couldn't create the DBIx::DBO::Table object!" unless $t;

    pass 'Method DBIx::DBO->do';

    ok my $table_info = $dbo->table_info([$test_sch, $test_tbl]), 'Method DBIx::DBO->table_info';
    is $table_info, $dbo->table_info($quoted_table), 'Method DBIx::DBO->table_info (quoted)';
    is $table_info, $dbo->table_info(defined $test_sch ? "$test_sch.$test_tbl" : $test_tbl),
        'Method DBIx::DBO->table_info (unquoted)';

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

    is_deeply [$t->columns], [qw(id name)], 'Method DBIx::DBO::Table->columns';

    # Create a column object
    my $c = $t->column('id');
    isa_ok $c, 'DBIx::DBO::Column', '$c';

    # Fetch one value from the Table
    is $t->fetch_value($t ** 'name', id => 3), 'Uncle Arnie', 'Method DBIx::DBO::Table->fetch_value';

    # Fetch one value from the Table
    is_deeply $t->fetch_hash(id => \3), {id=>3,name=>'Uncle Arnie'}, 'Method DBIx::DBO::Table->fetch_hash';

    # Fetch one value from the Table
    my $r = $t->fetch_row(id => 3, name => \'NOT NULL');
    is $r->{name}, 'Uncle Arnie', 'Method DBIx::DBO::Table->fetch_row';

    # Fetch a column arrayref from the Table
    is_deeply $t->fetch_column($t ** 'name', id => 3), ['Uncle Arnie'], 'Method DBIx::DBO::Table->fetch_column';

    # Advanced insert using a column object
    $rv = $t->insert($c => {FUNC => '4'}, name => 'NotUsed', name => \"'James Bond'") or diag sql_err($t);
    ok $rv, 'Method DBIx::DBO::Table->insert (complex values)';
    is $t->fetch_value('name', id => 4), 'James Bond', 'Method DBIx::DBO::Table->insert (remove duplicate cols)';

    # Delete via table object
    $rv = $t->delete(id => 3) or diag sql_err($t);
    is $rv, 1, 'Method DBIx::DBO::Table->delete';

    if ($can{auto_increment_id}) {
        $t->insert(name => 'Vernon Lyon') or diag sql_err($t);
    } else {
        $t->insert(id => 5, name => 'Vernon Lyon') or diag sql_err($t);
    }

    SKIP: {
        skip "No auto-increment $quoted_cols[1] column", 1 unless $can{auto_increment_id};
        is $t->last_insert_id, 5, 'Method DBIx::DBO::Table->last_insert_id'
            or $t->delete(name => 'Vernon Lyon'), $t->insert(id => 5, name => 'Vernon Lyon');
    }

    # Bulk insert
    my @bulk_data = ({id=>6,name=>'Bulk Insert'},{id=>7,name=>'Bulk Insert'});

    $rv = $t->bulk_insert(rows => [map [@$_{qw(id name)}], @bulk_data]) or diag sql_err($t);
    is $rv, 2, 'Method DBIx::DBO::Table->bulk_insert (ARRAY)';
    $t->delete(name => 'Bulk Insert') or diag sql_err($t);

    $rv = $t->bulk_insert(rows => \@bulk_data) or diag sql_err($t);
    is $rv, 2, 'Method DBIx::DBO::Table->bulk_insert (HASH)';
    $t->delete(name => 'Bulk Insert') or diag sql_err($t);

    $rv = $t->bulk_insert(columns => [qw(name id)], rows => [map [@$_{qw(name id)}], @bulk_data]) or diag sql_err($t);
    is $rv, 2, 'Method DBIx::DBO::Table->bulk_insert (ARRAY)';
    $t->delete(name => 'Bulk Insert') or diag sql_err($t);

    $rv = $t->bulk_insert(columns => [qw(name id)], rows => \@bulk_data) or diag sql_err($t);
    is $rv, 2, 'Method DBIx::DBO::Table->bulk_insert (HASH)';
    $t->delete(name => 'Bulk Insert') or diag sql_err($t);

    return $t;
}

sub advanced_table_methods {
    my $dbo = shift;
    my $t = shift;

    SKIP: {
        skip "No test table for advanced table tests", 2 unless $t;

        # Advanced insert
        my $rv = $t->insert(id => { FUNC => '? + 3', VAL => 3 }, name => \"'Harry Harrelson'") or diag sql_err($t);
        ok $rv, 'Method DBIx::DBO::Table->insert (advanced)';

        $t->insert(id => 7, name => 'Amanda Huggenkiss') or diag sql_err($t);
        $t->insert(id => 8, name => undef) or diag sql_err($t);

        # Advanced delete
        $rv = $t->delete(id => \'NOT NULL', name => undef) or diag sql_err($t);
        ok $rv, 'Method DBIx::DBO::Table->delete (advanced)';
    }
}

sub skip_advanced_table_methods {
    my $dbo = shift;
    my $t = shift;

    note "No advanced table tests for $dbd_name";
    $t->insert(id => 6, name => 'Harry Harrelson') or diag sql_err($t);
    $t->insert(id => 7, name => 'Amanda Huggenkiss') or diag sql_err($t);
}

sub row_methods {
    my $dbo = shift;
    my $t = shift;

    my $r = DBIx::DBO::Row->new($dbo, $t->_quoted_name);
    isa_ok $r, 'DBIx::DBO::Row', '$r (using quoted table name)';

    $r = $dbo->row([ @$t{qw(Schema Name)} ]);
    isa_ok $r, 'DBIx::DBO::Row', '$r (using table name array)';

    $r = $dbo->row($t);
    isa_ok $r, 'DBIx::DBO::Row', '$r (using Table object)';

    is $$r->{array}, undef, 'Row is empty';
    is_deeply [$r->columns], [qw(id name)], 'Method DBIx::DBO::Row->columns';

    ok $r->load(id => 2, name => 'Jane Smith'), 'Method DBIx::DBO::Row->load' or diag sql_err($r);
    is_deeply $$r->{array}, [ 2, 'Jane Smith' ], 'Row loaded correctly';

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
    isa_ok $q->_sth, 'DBI::st', '$q->_sth';

    # Get a Row object
    my $r = $q->row;
    isa_ok $r, 'DBIx::DBO::Row', '$q->row';
    my $r_str = "$r";

    # Alter the SQL to ensure the row is detached and rebuilt
    $q->order_by('id');
    $r = $q->row;
    isnt $r_str, "$r", 'Row rebuilds SQL and detaches when a ref still exists';
    $r_str = "$r";

    # Remove the reference so that the row wont detach
    undef $r;

    # Fetch the first row
    $r = $q->fetch;
    ok $r->isa('DBIx::DBO::Row'), 'Method DBIx::DBO::Query->fetch';
    SKIP: {
        skip "Re-use row doesn't work with Devel::Cover", 1 if exists $INC{'Devel/Cover.pm'};
        is $r_str, "$r", 'Re-use the same row object';
    }

    # Access methods
    is_deeply [$q->columns], [qw(id name)], 'Method DBIx::DBO::Query->columns';
    is $r->{name}, 'John Doe', 'Access row as a hashref';
    is $r->[0], 1, 'Access row as an arrayref';

    # Fetch another row
    $r_str = "$r";
    $r = $q->fetch;
    isnt $r_str, "$r", 'Row detaches during fetch when a ref still exists';

    # More access methods
    is $r->value($t->column('name')), 'Jane Smith', 'Access row via method DBIx::DBO::Row::value';
    is $r ** $t ** 'name', 'Jane Smith', 'Access row via shortcut method **';

    # Re-run the query
    $q->run or diag sql_err($q);
    is $q->fetch->{name}, 'John Doe', 'Method DBIx::DBO::Query->run';
    $q->finish;
    is $q->fetch->{name}, 'John Doe', 'Method DBIx::DBO::Query->finish';

    # Count the number of rows
    1 while $q->fetch;
    is $q->rows, 6, 'Row count is 6';

    # WHERE clause
    ok $q->where('name', 'LIKE', \"'%o%'"), 'Method DBIx::DBO::Query->where' or diag sql_err($q);

    # Parentheses
    $q->open_bracket('OR');
    $q->where('name', 'LIKE', \"'%a%'");
    $q->where('id', '!=', \1);
    $q->where('id', '=', undef);
    $q->open_bracket('AND');
    $q->where('id', '<>', 12345);
    $q->where('id', '!=', undef);
    $q->where('id', 'NOT IN', [1,22,333]);
    $q->where('id', 'NOT BETWEEN', [123,456]);
    my $got = $q->col_arrayref({ Columns => [1] });
    is_deeply $got, [4,5,6], 'Method DBIx::DBO::Query->open_bracket' or diag sql_err($q);

    my $old_sql = $q->sql;
    $q->unwhere('name');
    is $q->sql, $old_sql, 'Method DBIx::DBO::Query->unwhere (before close_bracket)';

    $q->close_bracket;
    $q->close_bracket;
    $q->unwhere('name');
    isnt $q->sql, $old_sql, 'Method DBIx::DBO::Query->close_bracket';

    $got = $q->col_arrayref({ Columns => [1] });
    is_deeply $got, [2,4,5,6,7], 'Method DBIx::DBO::Query->unwhere';

    # Reset the Query
    $q->reset;
    is $q->sql, $dbo->query($t)->sql, 'Method DBIx::DBO::Query->reset';

    # Group by the first initial
    $q->show(\'COUNT(*)');
    ok(($q->group_by({FUNC => 'SUBSTR(?, 1, 1)', COL => 'name'}), $q->run),
        'Method DBIx::DBO::Query->group_by') or diag sql_err($q);

    # Update & Load a Row with aliased columns
    $q->show('name', {COL => 'id', AS => 'key'});
    $q->group_by;
    $r = $q->fetch;
    ok $r->update(id => $r->{key}), 'Can update a Row using aliases' or diag sql_err($r);
    ok $r->load(id => 5), 'Can load a Row using aliases' or diag sql_err($r);

    $q->finish;
    return $q;
}

sub advanced_query_methods {
    my $dbo = shift;
    my $t = shift;
    my $q = shift;
    $q->reset;

    # Show specific columns only
    SKIP: {
        unless ($can{collate}) {
            $q->order_by('id');
            $q->run or diag sql_err($q);
            skip 'COLLATE is not supported', 1;
        }
        $q->order_by({ COL => 'id', COLLATE => $can{collate} });
        ok $q->run, 'Method DBIx::DBO::Query->order_by COLLATE' or diag sql_err($q);
    }
    $q->show({ FUNC => 'UPPER(?)', COL => 'name', AS => 'name' }, 'id', 'name');
    ok $q->run && $q->fetch->{name} eq 'JOHN DOE', 'Method DBIx::DBO::Query->show' or diag sql_err($q);

    is $q->row ** $t ** 'name', 'John Doe', 'Access specific column';
    is_deeply [$q->row->columns], [qw(name id name)], 'Method DBIx::DBO::Row->columns (aliased)';
    is_deeply [$q->columns], [qw(name id name)], 'Method DBIx::DBO::Query->columns (aliased)';

    # Show whole tables
    $q->show({ FUNC => "'who?'", AS => 'name' }, $t);
    is $q->fetch ** $t ** 'name', 'John Doe', 'Access specific column from a shown table';

    # Check case sensitivity of LIKE
    my $case_sensitive = $dbo->selectrow_arrayref($case_sensitivity_sql, undef, 'a', 'A') or diag sql_err($dbo);
    $case_sensitive = not $case_sensitive->[0];
    note "$dbd_name 'LIKE' is".($case_sensitive ? '' : ' NOT').' case sensitive';

    # WHERE clause
    $q->show('id');
    ok $q->where('name', 'LIKE', '%a%'), 'Method DBIx::DBO::Query->where LIKE';
    my $a = $q->col_arrayref or diag sql_err($q);
    is_deeply $a, [2,4,6,7], 'Method DBIx::DBO::Query->col_arrayref';
    ok $q->where('id', 'BETWEEN', [2, \6]), 'Method DBIx::DBO::Query->where BETWEEN';
    $a = $q->arrayref or diag sql_err($q);
    is_deeply $a, [[2],[4],[6]], 'Method DBIx::DBO::Query->arrayref';
    ok $q->where('name', 'IN', ['Harry Harrelson', 'James Bond']), 'Method DBIx::DBO::Query->where IN';
    $a = $q->hashref('id') or diag sql_err($q);
    is_deeply $a, {4 => {id => 4},6 => {id => 6}}, 'Method DBIx::DBO::Query->hashref';

    # HAVING clause
    $q->show('id', 'name', { FUNC => 'CONCAT(?, ?)', COL => [qw(id name)], AS => 'combo'});
    ok $q->having('combo', '=', '4James Bond'), 'Method DBIx::DBO::Query->having';

    $q->finish;
}

sub skip_advanced_query_methods {
    note "No advanced query tests for $dbd_name";
}

sub join_methods {
    my $dbo = shift;
    my $table = shift;

    my ($q, $t1, $t2) = $dbo->query($table, $table);

    # DISTINCT clause
    $q->order_by('id');
    $q->show('id');
    $q->distinct(1);
    is_deeply $q->arrayref, [[1],[2],[4],[5],[6],[7]], 'Method DBIx::DBO::Query->distinct';
    $q->distinct(0);
    $q->show($t1, $t2);

    # Counting rows
    $q->limit(3);
    $q->config(CalcFoundRows => 1);
    ok $q, 'Comma JOIN';
    is $q->count_rows, 3, 'Method DBIx::DBO::Query->count_rows' or diag sql_err($q);
    is $q->found_rows, 36, 'Method DBIx::DBO::Query->found_rows' or diag sql_err($q);

    # JOIN
    $q->join_on($t2, $t1 ** 'id', '=', { FUNC => '?/2.0', VAL => $t2 ** 'id' });
    $q->order_by({ COL => $t1 ** 'name', ORDER => 'DESC' });
    $q->where($t1 ** 'name', '<', $t2 ** 'name', FORCE => 'OR');
    $q->where($t1 ** 'name', '>', $t2 ** 'name', FORCE => 'OR');
    $q->where($t1 ** 'name', 'LIKE', '%');
    my $r;
    # Oracle Can't do a SELECT * from a subquery that has "ambiguous" columns (two columns with the same name)
    $q->show() if $dbd eq 'Oracle';
    SKIP: {
        $q->run or fail 'JOIN ON' or diag sql_err($q) or skip 'No Left Join', 1;
        $r = $q->fetch or fail 'JOIN ON' or skip 'No Left Join', 1;

        is_deeply \@$r, [ 1, 'John Doe', 2, 'Jane Smith' ], 'JOIN ON';
        $r->load($t1 ** id => 2) or diag sql_err($r);
        is_deeply \@$r, [ 2, 'Jane Smith', 4, 'James Bond' ], 'Method DBIx::DBO::Row->load';
    }

    # LEFT JOIN
    ($q, $t1) = $dbo->query($table);
    $t2 = $q->join_table($table, 'left');
    $q->join_on($t2, $t1 ** 'id', '=', { FUNC => '?/2.0', COL => $t2 ** 'id' });
    $q->order_by({ COL => $t1 ** 'name', ORDER => 'DESC' });
    if ($dbd eq 'Oracle') {
        # Oracle doesn't support LIMIT OFFSET
        $q->fetch;
        $q->fetch;
        $q->fetch;
    } else {
        $q->limit(1, 3);
    }

    SKIP: {
        $q->_sth or diag sql_err($q) or fail 'LEFT JOIN' or skip 'No Left Join', 3;
        $r = $q->fetch or fail 'LEFT JOIN' or skip 'No Left Join', 3;

        is_deeply \@$r, [ 4, 'James Bond', undef, undef ], 'LEFT JOIN';
        is $r->_column_idx($t2 ** 'id'), 2, 'Method DBIx::DBO::Row->_column_idx';
        is $r->value($t2 ** 'id'), undef, 'Method DBIx::DBO::Row->value';

        # Update the LEFT JOINed row
        SKIP: {
            skip "Multi-table UPDATE is not supported by $dbd_name", 1 unless $can{multi_table_update};
            ok $r->update($t1 ** 'name' => 'Vernon Wayne Lyon'), 'Method DBIx::DBO::Row->update' or diag sql_err($r);
        }
    }

    $q->finish;
}

sub todo_cleanup {
    my $sql = shift;
    unshift @_cleanup_sql, $sql;
}

sub cleanup {
    my $dbo = shift;

    note 'Doing cleanup';
    for my $sql (@_cleanup_sql) {
        $dbo->do($sql) or diag sql_err($dbo);
    }

    $dbo->disconnect;
    ok !defined $dbo->{dbh} && !defined $dbo->{rdbh}, 'Method DBIx::DBO->disconnect';
}

sub Dump {
    my($val, $var) = @_;
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
    $var = 'dump' unless defined $var;
    require Data::Dumper;
    local $Data::Dumper::Sortkeys = 1;
    local $Data::Dumper::Quotekeys = 0;
    my $d = Data::Dumper->new([$val], [$var]);
    if (ref $val) {
        my %seen;
        my @_no_recursion = ($val);
        if (reftype $val eq 'ARRAY')   { _Find_Seen(\%seen, \@_no_recursion, $_) for @$val }
        elsif (reftype $val eq 'HASH') { _Find_Seen(\%seen, \@_no_recursion, $_) for values %$val }
        elsif (reftype $val eq 'REF')  { _Find_Seen(\%seen, \@_no_recursion, $$val) }
        $d->Seen(\%seen);
    }
    print $d->Dump;
}

sub _Find_Seen {
    my($seen, $_no_recursion, $val) = @_;
    return unless ref $val;
    for (@$_no_recursion) {
        return if $val == $_;
    }
    push @$_no_recursion, $val;

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
    if (reftype $val eq 'ARRAY')   { _Find_Seen($seen, $_no_recursion, $_) for @$val }
    elsif (reftype $val eq 'HASH') { _Find_Seen($seen, $_no_recursion, $_) for values %$val }
    elsif (reftype $val eq 'REF')  { _Find_Seen($seen, $_no_recursion, $$val) }
}

# When testing via Sponge, use fake tables
package # hide from PAUSE
    DBIx::DBO::DBD::Sponge;
sub _get_table_schema {
    return;
}
my $fake_table_info = {
    PrimaryKeys => [],
    Columns => [ 'id', 'name', 'age' ],
    Column_Idx => { id => 1, name => 2, age => 3 },
};
sub _get_table_info {
    my $me = shift;
    my ($schema, $table) = @_;
    # Fake table info
    return $me->{TableInfo}{''}{$table} ||= $fake_table_info;
}

1;
