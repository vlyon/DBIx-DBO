package DBIx::DBO::Table;

use strict;
use warnings;
use DBIx::DBO::Common;
use Carp 'croak';
our @ISA = qw(DBIx::DBO::Common);

use overload '**' => \&column, fallback => 1;

=head1 NAME

DBIx::DBO::Table - An OO interface to SQL queries and results.  Encapsulates a table in an object.

=head1 SYNOPSIS

  # Create a Table object
  my $table = $dbo->table('my_table');
  
  # Get a column reference
  my $column = $table ** 'employee_id';
  
  # Quickly display my employee id
  print $table->fetch_value('employee_id', name => 'Vernon');
  
  # Find the IDs of fired employees
  my @fired = @{ $table->fetch_column('id', status => 'fired');
  
  # Insert a new row into the table
  $table->insert(employee_id => 007, name => 'James Bond');
  
  # Remove rows from the table where the name IS NULL
  $table->delete(name => undef);

=head1 DESCRIPTION

C<Table> objects are mostly used for column references in a L<Query|DBIx::DBO::Query>.
They can also be used for INSERTs, DELETEs and simple lookups (fetch_*).

=head1 METHODS

=head3 C<new>

  DBIx::DBO::Table->new($dbo, $table);
  DBIx::DBO::Table->new($dbo, [$schema, $table]);
  DBIx::DBO::Table->new($dbo, $table_object);

Create and return a new C<Table> object.
Tables can be specified by their name or an arrayref of schema and table name or another C<Table> object.

=cut

sub new {
    my $proto = shift;
    UNIVERSAL::isa($_[0], 'DBIx::DBO') or croak 'Invalid DBO Object';
    my $class = ref($proto) || $proto;
    $class = $class->_set_dbd_inheritance($_[0]{dbd});
    $class->_init(@_);
}

sub _init {
    my($class, $dbo, $table) = @_;
    (my $schema, $table, my $me) = $dbo->table_info($table) or croak 'No such table: '.$table;
    bless { %$me, Schema => $schema, Name => $table, DBO => $dbo, LastInsertID => undef }, $class;
}

=head3 C<tables>

Return a list of C<Table> objects, which will always be this C<Table> object.

=cut

sub tables {
    wantarray ? $_[0] : 1;
}

sub _table_alias {
    return if $_[0] == $_[1];
    croak 'The table is not in this query';
}

sub _quoted_name {
    my $me = shift;
    defined $me->{_quoted_name} ? $me->{_quoted_name} : ($me->{_quoted_name} = $me->_qi(@$me{qw(Schema Name)}));
}

=head3 C<columns>

Return a list of column names.

=cut

sub columns {
    @{$_[0]->{Columns}};
}

=head3 C<column>

  $table->column($column_name);
  $table ** $column_name;

Returns a reference to a column for use with other methods.
The C<**> method is a shortcut for the C<column> method.

=cut

sub column {
    my($me, $col) = @_;
    croak 'Invalid column '.$me->_qi($col).' in table '.$me->_quoted_name
        unless exists $me->{Column_Idx}{$col};
    $me->{Column}{$col} ||= bless [$me, $col], 'DBIx::DBO::Column';
}

sub _valid_col {
    my($me, $col) = @_;
    return $col if $col->[0] == $me;
}

=head3 C<row>

Returns a new L<Row|DBIx::DBO::Row> object for this table.

=cut

sub row {
    my $me = shift;
    $me->_row_class->new($me->{DBO}, $me);
}

=head3 C<fetch_row>

  $table->fetch_row(%where);

Fetch the first matching row from the table returning it as a L<Row|DBIx::DBO::Row> object.

The C<%where> is a hash of field/value pairs.  The value can be a SCALAR ref, which will be used without quoting.

  $someone = $table->fetch_row(name => \'NOT NULL', age => 21, join_date => \'CURDATE()', end_date => undef);

=cut

sub fetch_row {
    my $me = shift;
    $me->row->load(@_);
}

=head3 C<fetch_value>

  $table->fetch_value($column, %where);

Fetch the first matching row from the table returning the value in one column.

=cut

sub fetch_value {
    my ($me, $col) = splice @_, 0, 2;
    $col = $me->_parse_col($col);
    my $sql = 'SELECT '.$me->_qi($col->[1]).' FROM '.$me->_quoted_name;
    my @bind;
    $sql .= ' WHERE '.$_ if $_ = $me->_build_quick_where(\@bind, @_);
    $me->_sql($sql, @bind);
    my $ref = $me->rdbh->selectrow_arrayref($sql, undef, @bind);
    return $ref && $ref->[0];
}

=head3 C<fetch_hash>

  $table->fetch_hash(%where);

Fetch the first matching row from the table returning it as a hashref.

=cut

sub fetch_hash {
    my $me = shift;
    my $sql = 'SELECT * FROM '.$me->_quoted_name;
    my @bind;
    $sql .= ' WHERE '.$_ if $_ = $me->_build_quick_where(\@bind, @_);
    $me->_sql($sql, @bind);
    $me->rdbh->selectrow_hashref($sql, undef, @bind);
}

=head3 C<fetch_column>

  $table->fetch_column($column, %where);

Fetch all matching rows from the table returning an arrayref of the values in one column.

=cut

sub fetch_column {
    my ($me, $col) = splice @_, 0, 2;
    $col = $me->_parse_col($col);
    my $sql = 'SELECT '.$me->_qi($col->[1]).' FROM '.$me->_quoted_name;
    my @bind;
    $sql .= ' WHERE '.$_ if $_ = $me->_build_quick_where(\@bind, @_);
    $me->_sql($sql, @bind);
    return $me->rdbh->selectcol_arrayref($sql, undef, @bind);
}

=head3 C<insert>

  $table->insert(name => 'Richard', age => 103);

Insert a row into the table.  Returns true on success or C<undef> on failure.

On supporting databases you may also use C<$table-E<gt>last_insert_id> to retreive
the autogenerated ID (if there was one) from the last inserted row.

=cut

sub insert {
    my $me = shift;
    croak 'Called insert() without args on table '.$me->_quoted_name unless @_;
    croak 'Wrong number of arguments' if @_ & 1;
    my @cols;
    my @vals;
    my @bind;
    my %remove_duplicates;
    while (@_) {
        my @val = $me->_parse_val(pop);
        my $col = $me->_build_col($me->_parse_col(pop));
        next if $remove_duplicates{$col}++;
        push @cols, $col;
        push @vals, $me->_build_val(\@bind, @val);
    }
    my $sql = 'INSERT INTO '.$me->_quoted_name.' ('.join(', ', @cols).') VALUES ('.join(', ', @vals).')';
    $me->_sql($sql, @bind);
    my $sth = $me->dbh->prepare($sql) or return undef;
    my $rv = $sth->execute(@bind) or return undef;
    $me->{LastInsertID} = $me->_save_last_insert_id($sth);
    return $rv;
}

sub _save_last_insert_id {
    # Should be provided in a DBD specific method
    # It is called after insert and must return the autogenerated ID

    # my($me, $sth) = @_;
    # return $sth->{Database}->last_insert_id(undef, @$me{qw(Schema Name)}, undef);
}

=head3 C<last_insert_id>

  $table->insert(name => 'Quentin');
  my $row_id = $table->last_insert_id;

Retreive the autogenerated ID (if there was one) from the last inserted row.

Returns the ID or undef if it's unavailable.

=cut

sub last_insert_id {
    my $me = shift;
    $me->{LastInsertID};
}

=head3 C<bulk_insert>

  $table->bulk_insert(
      columns => [qw(id name age)],
      rows => [{name => 'Richard', age => 103}, ...]
  );
  $table->bulk_insert(
      columns => [qw(id name age)],
      rows => [[ undef, 'Richard', 103 ], ...]
  );

Insert multiple rows into the table.
Returns the number of rows inserted or C<undef> on failure.

On supporting databases you may also use C<$table-E<gt>last_insert_id> to retreive
the autogenerated ID (if there was one) from the last inserted row.

=cut

sub bulk_insert {
    my($me, %opt) = @_;
    croak 'The "rows" argument must be an arrayref' if ref $opt{rows} ne 'ARRAY';
    my $sql = 'INSERT INTO '.$me->_quoted_name;

    my @cols;
    if (defined $opt{columns}) {
        @cols = map $me->column($_), @{$opt{columns}};
        $sql .= ' ('.join(', ', map $me->_build_col($_), @cols).')';
        @cols = map $_->[1], @cols;
    } else {
        @cols = @{$me->{Columns}};
    }
    $sql .= ' VALUES ';

    $me->_do_bulk_insert($sql, \@cols, %opt);
}

sub _fast_bulk_insert {
    my($me, $sql, $cols, %opt) = @_;

    my @vals;
    my @bind;
    if (ref $opt{rows}[0] eq 'ARRAY') {
        for my $row (@{$opt{rows}}) {
            push @vals, '('.join(', ', map $me->_build_val(\@bind, $me->_parse_val($_)), @$row).')';
        }
    } else {
        for my $row (@{$opt{rows}}) {
            push @vals, '('.join(', ', map $me->_build_val(\@bind, $me->_parse_val($_)), @$row{@$cols}).')';
        }
    }

    $sql .= join(",\n", @vals);
    $me->do($sql, undef, @bind);
}

sub _safe_bulk_insert {
    my($me, $sql, $cols, %opt) = @_;

    # TODO: Wrap in a transaction
    my $rv;
    my $sth;
    my $prev_vals = '';
    if (ref $opt{rows}[0] eq 'ARRAY') {
        for my $row (@{$opt{rows}}) {
            my @bind;
            my $vals = '('.join(', ', map $me->_build_val(\@bind, $me->_parse_val($_)), @$row).')';
            $me->_sql($sql.$vals, @bind);
            if ($prev_vals ne $vals) {
                $sth = $me->dbh->prepare($sql.$vals) or return undef;
                $prev_vals = $vals;
            }
            $rv += $sth->execute(@bind) or return undef;
        }
    } else {
        for my $row (@{$opt{rows}}) {
            my @bind;
            my $vals = '('.join(', ', map $me->_build_val(\@bind, $me->_parse_val($_)), @$row{@$cols}).')';
            $me->_sql($sql.$vals, @bind);
            if ($prev_vals ne $vals) {
                $sth = $me->dbh->prepare($sql.$vals) or return undef;
                $prev_vals = $vals;
            }
            $rv += $sth->execute(@bind) or return undef;
        }
    }

    return $rv || '0E0';
}
*_do_bulk_insert = \&_safe_bulk_insert;

=head3 C<delete>

  $table->delete(name => 'Richard', age => 103);

Delete all rows from the table matching the criteria.  Returns the number of rows deleted or C<undef> on failure.

=cut

sub delete {
    my $me = shift;
    my $sql = 'DELETE FROM '.$me->_quoted_name;
    my @bind;
    $sql .= ' WHERE '.$_ if $_ = $me->_build_quick_where(\@bind, @_);
    $me->do($sql, undef, @bind);
}

=head3 C<truncate>

  $table->truncate;

Truncate the table.  Returns true on success or C<undef> on failure.

=cut

sub truncate {
    my $me = shift;
    $me->do('TRUNCATE TABLE '.$me->_quoted_name);
}

=head2 Common Methods

These methods are accessible from all DBIx::DBO* objects.

=head3 C<dbh>

The I<read-write> C<DBI> handle.

=head3 C<rdbh>

The I<read-only> C<DBI> handle, or if there is no I<read-only> connection, the I<read-write> C<DBI> handle.

=head3 C<do>

  $table->do($statement)         or die $table->dbh->errstr;
  $table->do($statement, \%attr) or die $table->dbh->errstr;
  $table->do($statement, \%attr, @bind_values) or die ...

This provides access to L<DBI-E<gt>do|DBI/"do"> method.  It defaults to using the I<read-write> C<DBI> handle.

=head3 C<config>

  $table_setting = $table->config($option);
  $table->config($option => $table_setting);

Get or set the C<Table> config settings.  When setting an option, the previous value is returned.  When getting an option's value, if the value is undefined, the L<DBIx::DBO|DBIx::DBO>'s value is returned.

See L<DBIx::DBO/Available_config_options>.

=cut

sub config {
    my $me = shift;
    my $opt = shift;
    return $me->_set_config($me->{Config} ||= {}, $opt, shift) if @_;
    return defined $me->{Config}{$opt} ? $me->{Config}{$opt} : $me->{DBO}->config($opt);
}

sub DESTROY {
    undef %{$_[0]};
}

1;

__END__

=head1 TODO LIST

=over 4

=item *

Add a multi_insert method for extended INSERTs.

=back

=head1 SEE ALSO

L<DBIx::DBO>

=cut

