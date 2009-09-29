package DBIx::DBO;

use 5.010;
use strict;
use warnings;
use DBI;
use DBIx::DBO::Common;
use DBIx::DBO::Table;
use DBIx::DBO::Query;
use DBIx::DBO::Row;

=head1 NAME

DBIx::DBO - An OO interface to SQL queries and results.

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';

=head1 SYNOPSIS

  use DBIx::DBO;

  # Create the DBO
  my $dbo = DBIx::DBO->connect('DBI:mysql:my_db', 'me', 'mypasswd') or die $DBI::errstr;

  # Create a "readonly" connection - useful for a slave database
  $dbo->connect_readonly('DBI:mysql:my_db', 'me', 'mypasswd') or die $DBI::errstr;

  # Start with a Query object
  my $query = $dbo->query('my_table');

  # Find records with an 'o' in the name
  $query->where('name', 'LIKE', '%o%');

  # And with an id that is less than 50
  $query->where('id', '<', 50);

  # Return only the first 10 rows
  $query->limit(10);

  # Fetch the rows
  while (my $row = $query->fetch) {

      # Use the row as an array reference
      printf "id=%d  name=%s  status=%s\n", $row->[0], $row->[1], $row->[4];

      # Or as a hash reference
      print 'id=', $row->{id}, "\n", 'name=', $row->{name};

      # Update/delete rows
      $row->update(status => 'Fired!') if $row->{name} eq 'Harry';
      $row->delete if $record->{id} == 27;
  }

=head1 DESCRIPTION

This module provides a convenient and efficient way to access a database. It can construct queries for you and returns the results in an easy to use method.

=head1 FUNCTIONS

=cut

sub import {
    my $class = shift;
    if (@_ & 1) {
        my $opt = pop;
        oops "Import option '$opt' passed without a value";
    }
    while (my ($opt, $val) = splice @_, 0, 2) {
        if (exists $Config{$opt}) {
            $Config{$opt} = $val;
        } else {
            oops "Unknown import option '$opt'";
        }
    }
}

=head2 connect

  $dbo = DBIx::DBO->connect($data_source, $username, $password, \%attr)
      or die $DBI::errstr;

Takes the same arguments as L<DBI-E<gt>connect|DBI/"connect"> for a read-write connection to a database. It returns the DBIx::DBO object if the connection succeeds or undefined on failure.

=head2 connect_readonly

Takes the same arguments as C<connect> for a read-only connection to a database. It returns the DBIx::DBO object if the connection succeeds or undefined on failure.

Both connect & connect_readonly can be called on a $dbo object without that respective connection to have a DBO with both read-write and read-only connections.

  my $dbo = DBIx::DBO->connect($master_dsn, $username, $password, \%attr)
      or die $DBI::errstr;
  $dbo->connect_readonly($slave_dsn, $username, $password, \%attr)
      or die $DBI::errstr;

=cut

sub connect {
    my $me = shift;
    if (blessed $me and $me->isa('DBIx::DBO')) {
        ouch 'DBO is already connected' if $me->{dbh};
        $me->_check_driver($_[0]) if @_;
        $me->{dbh} = _connect($me->{ConnectArgs}, @_) or return;
        return $me;
    }
    my $new = { rdbh => undef, ConnectArgs => [], ConnectReadOnlyArgs => [], TransactionDepth => 0 };
    $new->{dbh} = _connect($new->{ConnectArgs}, @_) or return;
    my $class = $me->_require_dbd_class($new->{dbh}) or return;
    $class->_bless_dbo($new);
}

sub connect_readonly {
    my $me = shift;
    if (blessed $me and $me->isa('DBIx::DBO')) {
        $me->{rdbh}->disconnect if $me->{rdbh};
        $me->_check_driver($_[0]) if @_;
        $me->{rdbh} = _connect($me->{ConnectReadOnlyArgs}, @_) or return;
        return $me;
    }
    my $new = { dbh => undef, ConnectArgs => [], ConnectReadOnlyArgs => [], TransactionDepth => 0 };
    $new->{rdbh} = _connect($new->{ConnectReadOnlyArgs}, @_) or return;
    my $class = $me->_require_dbd_class($new->{rdbh}) or return;
    $class->_bless_dbo($new);
}

sub _require_dbd_class {
    my $me = shift;
    my $dbh = shift;
    my $class = $me.'::DBD::'.$dbh->{Driver}{Name};

    # Set inheritance for all dbd classes
    {
        no strict 'refs';
        @{$class.'::Common::ISA'} = ($me.'::Common');
        @{$class.'::ISA'} = ($me, $class.'::Common');
        @{$class.'::Table::ISA'} = ($me.'::Table', $class.'::Common');
        @{$class.'::Query::ISA'} = ($me.'::Query', $class.'::Common');
        @{$class.'::Row::ISA'} = ($me.'::Row', $class.'::Common');
    }

    my @warn;
    {
        local $SIG{__WARN__} = sub { push @warn, join '', @_ };
        return $class if eval "require $class";
    }

    (my $file = $class.'.pm') =~ s'::'/'g;
    if ($@ !~ / \Q$file\E in \@INC /) {
        # Set $DBI::errstr
        (my $err = $@) =~ s/\n.*$//; # Remove the last line
        chomp @warn;
        chomp $err;
        $dbh->set_err('', join("\n", "Can't load driver $class", @warn, $err));
        return;
    }

    delete $INC{$file};
    $INC{$file} = 1;
    return $class;
}

sub _bless_dbo {
    my $class = shift;
    my $new = shift;
    bless $new, $class;
}

sub _check_driver {
    my $me = shift;
    my $dsn = shift;
    my $driver = DBI->parse_dsn($dsn) or
        ouch "Can't connect to data source '$dsn' because I can't work out what driver to use " .
            "(it doesn't seem to contain a 'dbi:driver:' prefix and the DBI_DRIVER env var is not set)";
    ref $me eq 'DBIx::DBO::'.$driver or
        ouch "Can't connect to the data source '$dsn'\n" .
            "The read-write and read-only connections must use the same DBI driver";
}

sub _connect {
    my $conn = shift;
    if (@_) {
        my ($dsn, $user, $auth, $attr) = @_;
        my %attr = %$attr if ref($attr) eq 'HASH';

### Add a stack trace to PrintError & RaiseError
        $attr{HandleError} = sub {
            if ($Config{_Debug_SQL} > 1) {
                $_[0] = Carp::longmess($_[0]);
                return 0;
            }
            oops $_[1]->errstr if $_[1]->{PrintError};
            ouch $_[1]->errstr if $_[1]->{RaiseError};
            return 1;
        } unless exists $attr{HandleError};

### AutoCommit is always on
        %attr = (PrintError => 0, RaiseError => 1, %attr, AutoCommit => 1);
        @$conn = ($dsn, $user, $auth, \%attr);
    }
    DBI->connect(@$conn);
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
    ouch "Invalid config option '$opt'" unless exists $Config{$opt};
    unless (blessed $me) {
        my $val = $Config{$opt};
        $Config{$opt} = shift if @_;
        return $val;
    }
    my $val = $me->{Config}{$opt} // $Config{$opt};
    $me->{Config}{$opt} = shift if @_;
    return $val;
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

sub _get_table_schema {
    my $me = shift;
    my $schema = my $q_schema = shift;
    my $table = my $q_table = shift;
    ouch 'No table name supplied' unless defined $table and length $table;

    $q_schema =~ s/([\\_%])/\\$1/g if defined $q_schema;
    $q_table =~ s/([\\_%])/\\$1/g;

    my $info = $me->rdbh->table_info(undef, $q_schema, $q_table)->fetchall_arrayref;
    ouch 'Invalid table: '.$table unless $info and @$info == 1 and $info->[0][2] eq $table;
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
    $h{Fields}{$_->{COLUMN_NAME}} = $_->{ORDINAL_POSITION} for @$cols;
    if (my $keys = $me->rdbh->primary_key_info(undef, $schema, $table)) {
        $h{PrimaryKeys} = [ map $cols->[$_->{KEY_SEQ} - 1]{COLUMN_NAME}, @{$keys->fetchall_arrayref({})} ];
    } else {
        $h{PrimaryKeys} = [];
    }
    $me->{TableInfo}{$schema // ''}{$table} = \%h;
}

=head2 table_info

  $dbo->table_info($table);
  $dbo->table_info([$schema, $table]);
  $dbo->table_info($table_object);

Dunno yet.

=cut

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
    my $class = ref($_[0]).'::Table';
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
    my $class = ref($_[0]).'::Query';
    $class->_new(@_);
}

=head2 record

  $dbo->record($query_object);

Create a new record object.

=cut

sub row {
    my $class = ref($_[0]).'::Row';
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

__END__

=head1 AUTHOR

Vernon Lyon, C<< <vlyon at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-dbix-dbo at rt.cpan.org>, or through the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DBIx-DBO>.  I will be notified, and then you'll automatically be notified of progress on your bug as I make changes.


=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc DBIx::DBO


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=DBIx-DBO>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/DBIx-DBO>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/DBIx-DBO>

=item * Search CPAN

L<http://search.cpan.org/dist/DBIx-DBO>

=back


=head1 COPYRIGHT & LICENSE

Copyright 2009 Vernon Lyon, all rights reserved.

This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.


=cut

1;
