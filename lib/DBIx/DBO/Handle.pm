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

  $dbo_setting = $dbo->config($option);
  $dbo->config($option => $dbo_setting);

Get or set the C<DBIx::DBO::Handle> settings.
When setting an option, the previous value is returned.

=cut

sub config {
    my $me = shift;
    my $opt = shift;
    my $val = $me->{Config}{$opt} // $Config{$opt};
    $me->{Config}{$opt} = shift if @_;
    return $val;
}

=head2 connect

  $dbo->connect($data_source, $username, $password, \%attr) or die $DBI::errstr;

Takes the same arguments as L<DBI-E<gt>connect|DBI/"connect"> to add a read-write connection to a database. It will fail if the read-write handle is already connected. It returns the DBIx::DBO object if the connection succeeds or undefined on failure.

=head2 connect_readonly

  $dbo->connect_readonly($data_source, $username, $password, \%attr) or die $DBI::errstr;

Takes the same arguments as C<connect> for a read-only connection to a database. It will replace the read-only handle if it is already connected. It returns the C<DBIx::DBO> object if the connection succeeds or undefined on failure.

=cut

sub connect {
    my $me = shift;
    ouch 'DBO is already connected' if $me->{dbh};
    $me->_check_driver($_[0]) if @_;
    $me->{dbh} = $me->_reconnect($me->{ConnectArgs}, @_) or return;
    return $me;
}

sub connect_readonly {
    my $me = shift;
    $me->{rdbh}->disconnect if $me->{rdbh};
    $me->_check_driver($_[0]) if @_;
    $me->{rdbh} = $me->_reconnect($me->{ConnectReadOnlyArgs}, @_) or return;
    return $me;
}

sub _check_driver {
    my $me = shift;
    my $dsn = shift;
    my $driver = (DBI->parse_dsn($dsn))[1] or
        ouch "Can't connect to data source '$dsn' because I can't work out what driver to use " .
            "(it doesn't seem to contain a 'dbi:driver:' prefix and the DBI_DRIVER env var is not set)";
    ref($me) =~ /::DBD::\Q$driver\E::Handle$/ or
        ouch "Can't connect to the data source '$dsn'\n" .
            "The read-write and read-only connections must use the same DBI driver";
}

=head2 dbh

The read-write DBI handle.

=head2 rdbh

The read-only DBI handle, or if there is no read-only connection, the read-write DBI handle.

=head2 do

  $dbo->do($statement)         or die $dbo->dbh->errstr;
  $dbo->do($statement, \%attr) or die $dbo->dbh->errstr;
  $dbo->do($statement, \%attr, @bind_values) or die ...

This provides access to DBI C<do> method. It defaults to using the read-write DBI handle.

=cut

sub dbh {
    my $me = shift;
    ouch 'Invalid action for a read-only connection' unless $me->{dbh};
    return $me->{dbh} if $me->{dbh}->ping;
    $me->{dbh} = $me->_reconnect($me->{ConnectArgs});
}

sub rdbh {
    my $me = shift;
    return $me->dbh unless $me->{rdbh};
    return $me->{rdbh} if $me->{rdbh}->ping;
    $me->{rdbh} = $me->_reconnect($me->{ConnectReadOnlyArgs});
}

sub _reconnect {
    my $class = ref(shift);
    $class =~ s/::DBD::\w+::Handle$//;
    $class->_connect(@_);
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

    # First try just these types
    my $info = $me->rdbh->table_info(undef, $q_schema, $q_table,
        'TABLE,VIEW,GLOBAL TEMPORARY,LOCAL TEMPORARY,SYSTEM TABLE')->fetchall_arrayref;
    # Then if we found nothing, try any type
    $info = $me->rdbh->table_info(undef, $q_schema, $q_table)->fetchall_arrayref if $info and @$info == 0;
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
    $h{PrimaryKeys} = [];
    if (my $keys = $me->rdbh->primary_key_info(undef, $schema, $table)) {
        $h{PrimaryKeys}[$_->{KEY_SEQ} - 1] = $_->{COLUMN_NAME} for @{$keys->fetchall_arrayref({})};
    }
    $me->{TableInfo}{$schema // ''}{$table} = \%h;
}

sub table_info {
    my $me = shift;
    my $table = shift;
    my $schema;

    if (blessed $table and $table->isa('DBIx::DBO::Table')) {
        ($schema, $table) = @$table{qw(Schema Name)};
    } else {
        if (ref $table eq 'ARRAY') {
            ($schema, $table) = @$table;
        } elsif ($table =~ /\./) {
            # TODO: Better splitting of: schema.table or `schema`.`table` or "schema"."table"@"catalog" or ...
            ($schema, $table) = split /\./, $table, 2;
        }
        $schema //= $me->_get_table_schema($schema, $table);

        $me->_get_table_info($schema, $table) unless exists $me->{TableInfo}{$schema // ''}{$table};
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
    $class =~ s/Handle$/Table/;
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
    $class =~ s/Handle$/Query/;
    $class->_new(@_);
}

=head2 row

  $dbo->row($table_object);
  $dbo->row($query_object);

Create a new row object.

=cut

sub row {
    my $class = ref($_[0]);
    $class =~ s/Handle$/Row/;
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
