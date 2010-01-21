package DBIx::DBO::Handle;
use DBIx::DBO::Common;

use strict;
use warnings;

sub _bless_dbo {
    my $class = shift;
    my $new = shift;
    bless $new, $class;
}

=head2 config

  $global_setting = DBIx::DBO->config($option)
  DBIx::DBO->config($option => $global_setting)
  $dbo_setting = $dbo->config($option)
  $dbo->config($option => $dbo_setting)

Get or set the global or dbo config settings.
When setting an option, the previous value is returned.

=cut

sub config {
    my $me = shift;
    my $opt = shift;
    my $val = $me->{Config}{$opt} // $Config{$opt};
    $me->{Config}{$opt} = shift if @_;
    return $val;
}

sub dbh {
    my $me = shift;
    ouch 'Invalid action for a read-only connection' unless $me->{dbh};
    return $me->{dbh} if $me->{dbh}->ping;
    $me->{dbh} = _connect($me->{ConnectArgs});
}

sub rdbh {
    my $me = shift;
    return $me->dbh unless $me->{rdbh};
    return $me->{rdbh} if $me->{rdbh}->ping;
    $me->{rdbh} = _connect($me->{ConnectReadOnlyArgs});
}

=head2 selectrow_array

  $dbo->selectrow_array($statement, \%attr, @bind_values);

This provides access to DBI C<selectrow_array> method.

=head2 selectrow_arrayref

  $dbo->selectrow_arrayref($statement, \%attr, @bind_values);

This provides access to DBI C<selectrow_arrayref> method.

=head2 selectall_arrayref

  $dbo->selectall_arrayref($statement, \%attr, @bind_values);

This provides access to DBI C<selectall_arrayref> method.

=cut

sub selectrow_array {
    my ($me, $sql, $attr) = splice @_, 0, 3;
    $me->_sql($sql, @_);
    $me->rdbh->selectrow_array($sql, $attr, @_);
}

sub selectrow_arrayref {
    my ($me, $sql, $attr) = splice @_, 0, 3;
    $me->_sql($sql, @_);
    $me->rdbh->selectrow_arrayref($sql, $attr, @_);
}

sub selectall_arrayref {
    my ($me, $sql, $attr) = splice @_, 0, 3;
    $me->_sql($sql, @_);
    $me->rdbh->selectall_arrayref($sql, $attr, @_);
}

=head2 table_info

  $dbo->table_info($table);
  $dbo->table_info([$schema, $table]);
  $dbo->table_info($table_object);

Returns a hashref of PrimaryKeys and Column_Idx for the table.
Mainly for internal use.

=cut

sub _get_table_schema {
    my $me = shift;
    my $schema = my $q_schema = shift;
    my $table = my $q_table = shift;
    ouch 'No table name supplied' unless defined $table and length $table;

    $q_schema =~ s/([\\_%])/\\$1/g if defined $q_schema;
    $q_table =~ s/([\\_%])/\\$1/g;

    my $info = $me->rdbh->table_info(undef, $q_schema, $q_table)->fetchall_arrayref;
    ouch 'Invalid table: '.$me->_qi($table) unless $info and @$info == 1 and $info->[0][2] eq $table;
    return $info->[0][1];
}

sub _get_table_info {
    my $me = shift;
    my $schema = shift;
    my $table = shift;
    ouch 'No table name supplied' unless defined $table and length $table;

    my $cols = $me->rdbh->column_info(undef, $schema, $table, '%')->fetchall_arrayref({});
    ouch 'Invalid table: '.$me->_qi($table) unless @$cols;

    my %h;
    $h{Column_Idx}{$_->{COLUMN_NAME}} = $_->{ORDINAL_POSITION} for @$cols;
    $h{Columns} = [ sort { $h{Column_Idx}{$a} cmp $h{Column_Idx}{$b} } keys %{$h{Column_Idx}} ];
    if (my $keys = $me->rdbh->primary_key_info(undef, $schema, $table)) {
        $h{PrimaryKeys} = [ map $cols->[$_->{KEY_SEQ} - 1]{COLUMN_NAME}, @{$keys->fetchall_arrayref({})} ];
    } else {
        $h{PrimaryKeys} = [];
    }
    $me->{TableInfo}{$schema // ''}{$table} = \%h;
}

sub table_info {
    my $me = shift;
    my $table = shift;
    my $schema;

    if (blessed $table and $table->isa('DBIx::DBO::Table')) {
        ($schema, $table) = @$table{qw(Schema Name)};
        return ($schema, $table, $me->{TableInfo}{$schema // ''}{$table});
    }
    if (ref $table eq 'ARRAY') {
        ($schema, $table) = @$table;
    }
    $schema //= $me->_get_table_schema($schema, $table);

    unless (exists $me->{TableInfo}{$schema // ''}{$table}) {
        $me->_get_table_info($schema, $table);
    }
    return ($schema, $table, $me->{TableInfo}{$schema // ''}{$table});
}

=head2 table

  $dbo->table($table);
  $dbo->table([$schema, $table]);
  $dbo->table($table_object);

Create a new table object for the table specified.

=cut

sub table {
    my $class = ref($_[0]);
    $class =~ s/\w+$/Table/;
    $class->_new(@_);
}

=head2 query

  $dbo->query($table, ...);
  $dbo->query([$schema, $table], ...);
  $dbo->query($table_object, ...);

Create a new query object from the tables specified.
In scalar context, just the query object will be returned.
In list context table objects will also be returned for each table specified.

=cut

sub query {
    my $class = ref($_[0]);
    $class =~ s/\w+$/Query/;
    $class->_new(@_);
}

=head2 row

  $dbo->row($table_object);
  $dbo->row($query_object);

Create a new row object.

=cut

sub row {
    my $class = ref($_[0]);
    $class =~ s/\w+$/Row/;
    $class->_new(@_);
}

=head2 disconnect

Disconnect both the read-write & read-only connections to the database.

=cut

sub disconnect {
    my $me = shift;
    if ($me->{dbh}) {
        $me->{dbh}->disconnect;
        undef $me->{dbh};
    }
    if ($me->{rdbh}) {
        $me->{rdbh}->disconnect;
        undef $me->{rdbh};
    }
}

sub DESTROY {
    undef %{$_[0]};
}

1;
