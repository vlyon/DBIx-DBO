package DBIx::DBO::Table;
use DBIx::DBO::Common;

use strict;
use warnings;

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

  # Insert a new row into the table
  $table->insert(employee_id => 007, name => 'James Bond');

  # Remove rows from the table where the name IS NULL
  $table->delete(name => undef);

=head1 METHODS

=cut

sub _new {
    my ($proto, $dbo, $table) = @_;
    my $class = ref($proto) || $proto;
    blessed $dbo and $dbo->isa('DBIx::DBO') or ouch 'Invalid DBO Object';
    (my $schema, $table, $_) = $dbo->table_info($table) or ouch 'No such table: '.$table;
    bless { %$_, Schema => $schema, Name => $table, DBO => $dbo, LastInsertID => undef }, $class;
}

=head2 tables

Return a list of L<DBIx::DBO::Table|DBIx::DBO::Table> objects, which will always be this C<Table> object.

=cut

sub tables {
    $_[0];
}

sub _table_alias {
    return if $_[0] == $_[1];
    ouch 'The table is not in this query';
}

sub _quoted_name {
    my $me = shift;
    defined $me->{_quoted_name} ? $me->{_quoted_name} : ($me->{_quoted_name} = $me->_qi(@$me{qw(Schema Name)}));
}

=head2 column

  $table->column($column_name);
  $table ** $column_name;

Returns a reference to a column for use with other methods.
The C<**> method is a shortcut for the C<column> method.

=cut

sub column {
    my ($me, $col) = @_;
    ouch 'Invalid column '.$me->_qi($col).' in table '.$me->_quoted_name
        unless exists $me->{Column_Idx}{$col};
    defined $me->{Column}{$col} ? $me->{Column}{$col} : ($me->{Column}{$col} = bless [$me, $col], 'DBIx::DBO::Column');
}

=head2 fetch_row

  $table->fetch_row(%where);

Fetch the first matching row from the table returning it as a L<DBIx::DBO::Row|DBIx::DBO::Row> object.

The C<%where> is a hash of field/value pairs. The value can be a SCALAR ref, which will be used without quoting.

  $someone = $table->fetch_row(name => \'NOT NULL', age => 21, join_date => \'CURDATE()', end_date => undef);

=cut

sub fetch_row {
    my $me = shift;
    $me->{DBO}->row($me)->load(@_);
}

=head2 fetch_value

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

=head2 fetch_hash

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

=head2 fetch_column

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

sub _last_insert_id {
    # Must be provided in a DBD specific method
}

=head2 insert

  $table->insert(name => 'Richard', age => 103);

Insert a row into the table. Returns true on success or C<undef> on failure.

=cut

sub insert {
    my $me = shift;
    ouch 'Called insert() without args on table '.$me->_quoted_name unless @_;
    ouch 'Wrong number of arguments' if @_ & 1;
    my @cols;
    my @vals;
    my @bind;
    while (my ($col, $val) = splice @_, 0, 2) {
        push @cols, $me->_build_col($me->_parse_col($col));
        push @vals, $me->_build_val(\@bind, $me->_parse_val($val));
    }
    my $sql = 'INSERT INTO '.$me->_quoted_name.' ('.join(', ', @cols).') VALUES ('.join(', ', @vals).')';
    $me->_sql($sql, @bind);
    my $sth = $me->dbh->prepare($sql) or return undef;
    my $rv = $sth->execute(@bind) or return undef;
    $me->{LastInsertID} = $me->_last_insert_id($sth);
    return $rv;
}

=head2 delete

  $table->delete(name => 'Richard', age => 103);

Delete all rows from the table matching the criteria. Returns the number of rows deleted or C<undef> on failure.

=cut

sub delete {
    my $me = shift;
    my $sql = 'DELETE FROM '.$me->_quoted_name;
    my @bind;
    $sql .= ' WHERE '.$_ if $_ = $me->_build_quick_where(\@bind, @_);
    $me->do($sql, undef, @bind);
}

=head1 COMMON METHODS

These methods are accessible from all DBIx::DBO* objects.

=head2 dbh

The read-write C<DBI> handle.

=head2 rdbh

The read-only C<DBI> handle, or if there is no read-only connection, the read-write C<DBI> handle.

=head2 do

  $dbo->do($statement)         or die $dbo->dbh->errstr;
  $dbo->do($statement, \%attr) or die $dbo->dbh->errstr;
  $dbo->do($statement, \%attr, @bind_values) or die ...

This provides access to L<DBI-E<gt>do|DBI/"do"> method. It defaults to using the read-write C<DBI> handle.

=head2 config

  $table_setting = $table->config($option);
  $table->config($option => $table_setting);

Get or set the C<Table> config settings.
When setting an option, the previous value is returned.

=cut

sub config {
    my $me = shift;
    my $opt = shift;
    my $val = defined $me->{Config}{$opt} ? $me->{Config}{$opt} : $me->{DBO}->config($opt);
    $me->{Config}{$opt} = shift if @_;
    return $val;
}

sub DESTROY {
    undef %{$_[0]};
}

1;
