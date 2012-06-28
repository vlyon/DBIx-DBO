package DBIx::DBO;

use 5.008;
use strict;
use warnings;
use DBI;
use Carp qw(carp croak);

our $VERSION;
our %Config = (
    AutoReconnect => 0,
    DebugSQL => 0,
    QuoteIdentifier => 1,
    CacheQuery => 0,
);
my $need_c3_initialize;
my @ConnectArgs;

BEGIN {
    $VERSION = '0.20';
    # The C3 method resolution order is required.
    if ($] < 5.009_005) {
        require MRO::Compat;
    } else {
        require mro;
    }
}

use DBIx::DBO::DBD;
use DBIx::DBO::Table;
use DBIx::DBO::Query;
use DBIx::DBO::Row;

sub _dbd_class   { 'DBIx::DBO::DBD' }
sub _table_class { 'DBIx::DBO::Table' }
sub _query_class { 'DBIx::DBO::Query' }
sub _row_class   { 'DBIx::DBO::Row' }

=head1 NAME

DBIx::DBO - An OO interface to SQL queries and results.  Easily constructs SQL queries, and simplifies processing of the returned data.

=head1 SYNOPSIS

  use DBIx::DBO;
  
  # Create the DBO
  my $dbo = DBIx::DBO->connect('DBI:mysql:my_db', 'me', 'mypasswd') or die $DBI::errstr;
  
  # Create a "read-only" connection (useful for a replicated database)
  $dbo->connect_readonly('DBI:mysql:my_db', 'me', 'mypasswd') or die $DBI::errstr;
  
  # Start with a Query object
  my $query = $dbo->query('my_table');
  
  # Find records with an 'o' in the name
  $query->where('name', 'LIKE', '%o%');
  
  # And with an id that is less than 500
  $query->where('id', '<', 500);
  
  # Exluding those with an age range from 20 to 29
  $query->where('age', 'NOT BETWEEN', [20, 29]);
  
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
      $row->delete if $row->{id} == 27;
  }

=head1 DESCRIPTION

This module provides a convenient and efficient way to access a database.  It can construct queries for you and returns the results in easy to use methods.

Once you've created a C<DBIx::DBO> object using one or both of C<connect> or C<connect_readonly>, you can begin creating L<Query|DBIx::DBO::Query> objects.  These are the "workhorse" objects, they encapsulate an entire query with JOINs, WHERE clauses, etc.  You need not have to know about what created the C<Query> to be able to use or modify it.  This makes it valuable in environments like mod_perl or large projects that prefer an object oriented approach to data.

The query is only automatically executed when the data is requested.  This is to make it possible to minimise lookups that may not be needed or to delay them as late as possible.

The L<Row|DBIx::DBO::Row> object returned can be treated as both an arrayref or a hashref.  The data is aliased for efficient use of memory.  C<Row> objects can be updated or deleted, even when created by JOINs (If the DB supports it).

=head1 METHODS

=cut

sub import {
    my $class = shift;
    if (@_ & 1) {
        my $opt = pop;
        carp "Import option '$opt' passed without a value";
    }
    while (my($opt, $val) = splice @_, 0, 2) {
        if (exists $Config{$opt}) {
            DBIx::DBO::DBD->_set_config(\%Config, $opt, $val);
        } else {
            carp "Unknown import option '$opt'";
        }
    }
}

=head3 C<new>

  DBIx::DBO->new($dbh);
  DBIx::DBO->new(undef, $readonly_dbh);

Create a new C<DBIx::DBO> object from existsing C<DBI> handles.  You must provide one or both of the I<read-write> and I<read-only> C<DBI> handles.

=head3 C<connect>

  $dbo = DBIx::DBO->connect($data_source, $username, $password, \%attr)
      or die $DBI::errstr;

Takes the same arguments as L<DBI-E<gt>connect|DBI/"connect"> for a I<read-write> connection to a database.  It returns the C<DBIx::DBO> object if the connection succeeds or undefined on failure.

=head3 C<connect_readonly>

Takes the same arguments as C<connect> for a I<read-only> connection to a database.  It returns the C<DBIx::DBO> object if the connection succeeds or undefined on failure.

Both C<connect> & C<connect_readonly> can be called on a C<DBIx::DBO> object to add that respective connection to create a C<DBIx::DBO> with both I<read-write> and I<read-only> connections.

  my $dbo = DBIx::DBO->connect($master_dsn, $username, $password, \%attr)
      or die $DBI::errstr;
  $dbo->connect_readonly($slave_dsn, $username, $password, \%attr)
      or die $DBI::errstr;

=cut

sub new {
    my $me = shift;
    croak 'Too many arguments for '.(caller(0))[3] if @_ > 3;
    my($dbh, $rdbh, $new) = @_;

    if (defined $new and not UNIVERSAL::isa($new, 'HASH')) {
        croak '3rd argument to '.(caller(0))[3].' is not a HASH reference';
    }
    if (defined $dbh) {
        croak 'Invalid read-write database handle' unless UNIVERSAL::isa($dbh, 'DBI::db');
        $new->{dbh} = $dbh;
        $new->{dbd} ||= $dbh->{Driver}{Name};
    }
    if (defined $rdbh) {
        croak 'Invalid read-only database handle' unless UNIVERSAL::isa($rdbh, 'DBI::db');
        croak 'The read-write and read-only connections must use the same DBI driver'
            if $dbh and $dbh->{Driver}{Name} ne $rdbh->{Driver}{Name};
        $new->{rdbh} = $rdbh;
        $new->{dbd} ||= $rdbh->{Driver}{Name};
    }
    croak "Can't create the DBO, unknown database driver" unless $new->{dbd};
    $new->{dbd_class} = $me->_dbd_class->_require_dbd_class($new->{dbd});
    $me->_init($new);
}

sub _init {
    my($class, $new) = @_;
    bless $new, $class;
}

sub connect {
    my $me = shift;
    my $conn;
    if (ref $me) {
        croak 'DBO is already connected' if $me->{dbh};
        $me->_check_driver($_[0]) if @_;
        $conn = $me->{ConnectArgs} ||= scalar @ConnectArgs if $me->config('AutoReconnect');
        $me->{dbh} = $me->_connect($conn, @_) or return;
        return $me;
    }
    my %new;
    $conn = $new{ConnectArgs} = scalar @ConnectArgs if $me->config('AutoReconnect');
    my $dbh = $me->_connect($conn, @_) or return;
    $me->new($dbh, undef, \%new);
}

sub connect_readonly {
    my $me = shift;
    my $conn;
    if (ref $me) {
        $me->{rdbh}->disconnect if $me->{rdbh};
        $me->_check_driver($_[0]) if @_;
        $conn = $me->{ConnectReadOnlyArgs} ||= scalar @ConnectArgs if $me->config('AutoReconnect');
        $me->{rdbh} = $me->_connect($conn, @_) or return;
        return $me;
    }
    my %new;
    $conn = $new{ConnectReadOnlyArgs} = scalar @ConnectArgs if $me->config('AutoReconnect');
    my $dbh = $me->_connect($conn, @_) or return;
    $me->new(undef, $dbh, \%new);
}

sub _check_driver {
    my($me, $dsn) = @_;

    my $driver = (DBI->parse_dsn($dsn))[1] or
        croak "Can't connect to data source '$dsn' because I can't work out what driver to use " .
            "(it doesn't seem to contain a 'dbi:driver:' prefix and the DBI_DRIVER env var is not set)";

    ref($me) =~ /::DBD::\Q$driver\E$/ or
    $driver eq $me->{dbd} or
        croak "Can't connect to the data source '$dsn'\n" .
            "The read-write and read-only connections must use the same DBI driver";
}

sub _connect {
    my $me = shift;
    my $conn_idx = shift;
    my @conn;

    if (@_) {
        my($dsn, $user, $auth, $attr) = @_;
        my %attr = %$attr if ref($attr) eq 'HASH';

        # Add a stack trace to PrintError & RaiseError
        $attr{HandleError} = sub {
            if ($Config{DebugSQL} > 1) {
                $_[0] = Carp::longmess($_[0]);
                return 0;
            }
            carp $_[1]->errstr if $_[1]->{PrintError};
            croak $_[1]->errstr if $_[1]->{RaiseError};
            return 1;
        } unless exists $attr{HandleError};

        # AutoCommit is always on
        %attr = (PrintError => 0, RaiseError => 1, %attr, AutoCommit => 1);
        @conn = ($dsn, $user, $auth, \%attr);
    }
    # If a conn index is given then store the connection args
    $ConnectArgs[$conn_idx] = \@conn if defined $conn_idx;

    local @DBIx::DBO::CARP_NOT = qw(DBI);
    DBI->connect(@conn);
}

=head3 C<table>

  $dbo->table($table);
  $dbo->table([$schema, $table]);
  $dbo->table($table_object);

Create and return a new L<Table|DBIx::DBO::Table> object.
Tables can be specified by their name or an arrayref of schema and table name or another L<Table|DBIx::DBO::Table> object.

=cut

sub table {
    $_[0]->_table_class->new(@_);
}

=head3 C<query>

  $dbo->query($table, ...);
  $dbo->query([$schema, $table], ...);
  $dbo->query($table_object, ...);

Create a new L<Query|DBIx::DBO::Query> object from the tables specified.
In scalar context, just the C<Query> object will be returned.
In list context, the C<Query> object and L<Table|DBIx::DBO::Table> objects will be returned for each table specified.

  my($query, $table1, $table2) = $dbo->query(['my_schema', 'my_table'], 'my_other_table');

=cut

sub query {
    $_[0]->_query_class->new(@_);
}

=head3 C<row>

  $dbo->row($table_object);
  $dbo->row($query_object);

Create and return a new L<Row|DBIx::DBO::Row> object.

=cut

sub row {
    $_[0]->_row_class->new(@_);
}

=head3 C<selectrow_array>

  $dbo->selectrow_array($statement, \%attr, @bind_values);

This provides access to the L<DBI-E<gt>selectrow_array|DBI/"selectrow_array"> method.  It defaults to using the I<read-only> C<DBI> handle.

=head3 C<selectrow_arrayref>

  $dbo->selectrow_arrayref($statement, \%attr, @bind_values);

This provides access to the L<DBI-E<gt>selectrow_arrayref|DBI/"selectrow_arrayref"> method.  It defaults to using the I<read-only> C<DBI> handle.

=head3 C<selectall_arrayref>

  $dbo->selectall_arrayref($statement, \%attr, @bind_values);

This provides access to the L<DBI-E<gt>selectall_arrayref|DBI/"selectall_arrayref"> method.  It defaults to using the I<read-only> C<DBI> handle.

=cut

sub selectrow_array {
    my $me = shift;
    $me->{dbd_class}->_selectrow_array($me, @_);
}

sub selectrow_arrayref {
    my $me = shift;
    $me->{dbd_class}->_selectrow_arrayref($me, @_);
}

sub selectall_arrayref {
    my $me = shift;
    $me->{dbd_class}->_selectall_arrayref($me, @_);
}

=head3 C<do>

  $dbo->do($statement)         or die $dbo->dbh->errstr;
  $dbo->do($statement, \%attr) or die $dbo->dbh->errstr;
  $dbo->do($statement, \%attr, @bind_values) or die ...

This provides access to the L<DBI-E<gt>do|DBI/"do"> method.  It defaults to using the I<read-write> C<DBI> handle.

=cut

sub do {
    my $me = shift;
    $me->{dbd_class}->_do($me, @_);
}

=head3 C<table_info>

  $dbo->table_info($table);
  $dbo->table_info([$schema, $table]);
  $dbo->table_info($table_object);

Returns a hashref containing C<PrimaryKeys>, C<Columns> and C<Column_Idx> for the table.
Mainly for internal use.

=cut

sub table_info {
    my($me, $table) = @_;
    croak 'No table name supplied' unless defined $table and length $table;

    my $schema;
    if (UNIVERSAL::isa($table, 'DBIx::DBO::Table')) {
        ($schema, $table) = @$table{qw(Schema Name)};
    } else {
        ($schema, $table) = ref $table eq 'ARRAY' ? @$table : $me->{dbd_class}->_unquote_table($me, $table);
        defined $schema or $schema = $me->{dbd_class}->_get_table_schema($me, $schema, $table);

        $me->{dbd_class}->_get_table_info($me, $schema, $table)
            unless exists $me->{TableInfo}{defined $schema ? $schema : ''}{$table};
    }
    return ($schema, $table, $me->{TableInfo}{defined $schema ? $schema : ''}{$table});
}

=head3 C<disconnect>

Disconnect both the I<read-write> & I<read-only> connections to the database.

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
    delete $me->{TableInfo};
    return;
}

=head2 Common Methods

These methods are accessible from all DBIx::DBO* objects.

=head3 C<dbo>

This C<DBO> object.

=head3 C<dbh>

The I<read-write> C<DBI> handle.

=head3 C<rdbh>

The I<read-only> C<DBI> handle, or if there is no I<read-only> connection, the I<read-write> C<DBI> handle.

=cut

sub dbo { $_[0] }

sub _handle {
    my $me = shift;
    my $handle = shift;
    my($d, $c) = $handle ne 'read-only' ? qw(dbh ConnectArgs) : qw(rdbh ConnectReadOnlyArgs);
    croak "No $handle handle connected" unless defined $me->{$d};
    # Automatically reconnect, but only if possible and needed
    $me->{$d} = $me->_connect($me->{$c}) if exists $me->{$c} and not $me->{$d}->ping;
    return $me->{$d};
}

sub dbh {
    my $me = shift;
    if (my $handle = $me->config('UseHandle')) {
        return $me->_handle($handle);
    }
    $me->_handle('read-write');
}

sub rdbh {
    my $me = shift;
    if (my $handle = $me->config('UseHandle')) {
        return $me->_handle($handle);
    }
    return $me->dbh unless defined $me->{rdbh};
    $me->_handle('read-only');
}

=head3 C<config>

  $global_setting = DBIx::DBO->config($option);
  DBIx::DBO->config($option => $global_setting);
  $dbo_setting = $dbo->config($option);
  $dbo->config($option => $dbo_setting);

Get or set the global or this C<DBIx::DBO> config settings.  When setting an option, the previous value is returned.  When getting an option's value, if the value is undefined, the global value is returned.

=head2 Available C<config> options

=over

=item C<AutoReconnect>

Boolean setting to store the connection details for re-use.
Before every operation the connection will be tested via ping() and reconnected automatically if needed.
Changing this has no effect after the connection has been made.
Defaults to C<false>.

=item C<DebugSQL>

Set to C<1> or C<2> to warn about each SQL command executed.  C<2> adds a full stack trace.
Defaults to C<0> (silent).

=item C<QuoteIdentifier>

Boolean setting to control quoting of SQL identifiers (schema, table and column names).

=item C<CacheQuery>

Boolean setting to cause C<Query> objects to cache their entire result for re-use.
The query will only be executed automatically once.
To rerun the query, either explicitly call L<run|DBIx::DBO::Query/"run"> or alter the query.
Defaults to C<false>.

=item C<UseHandle>

Set to C<'read-write'> or C<'read-only'> to force using only that handle for all operations.
Defaults to C<false> which chooses the I<read-only> handle for reads and the I<read-write> handle otherwise.

=back

Global options can also be set when C<use>'ing the module:

  use DBIx::DBO QuoteIdentifier => 0, DebugSQL => 1;

=cut

sub config {
    my($me, $opt) = @_;
    if (@_ > 2) {
        return ref $me
            ? $me->{dbd_class}->_set_config($me->{Config} ||= {}, $opt, $_[2])
            : $me->_dbd_class->_set_config(\%Config, $opt, $_[2]);
    }
    return ref $me
        ? $me->{dbd_class}->_get_config($opt, $me->{Config} ||= {}, \%Config)
        : $me->_dbd_class->_get_config($opt, \%Config);
}

sub STORABLE_freeze {
    my $me = $_[0];
    return unless ref $me->{dbh} or ref $me->{rdbh};

    my($dbh, $rdbh) = @$me{qw(dbh rdbh)};
    $me->{dbh} = "$me->{dbh}" if defined $me->{dbh};
    $me->{rdbh} = "$me->{rdbh}" if defined $me->{rdbh};
    my $frozen = Storable::nfreeze($me);
    @$me{qw(dbh rdbh)} = ($dbh, $rdbh);
    return $frozen;
}

sub STORABLE_thaw {
    my($me, $cloning, $frozen) = @_;
    %$me = %{ Storable::thaw($frozen) };
}

sub DESTROY {
    undef %{$_[0]};
}

1;

__END__

=head1 SUBCLASSING

For details on subclassing the C<Query> or C<Row> objects see: L<DBIx::DBO::Query/"SUBCLASSING"> and L<DBIx::DBO::Row/"SUBCLASSING">.
This is the simple (recommended) way to create objects representing a single query, table or row in your database.

C<DBIx::DBO> can be subclassed like any other object oriented module.

  package MySubClass;
  our @ISA = qw(DBIx::DBO);
  ...

The C<DBIx::DBO> object is used to create C<Table>, C<Query> and C<Row> objects.
The classes these objects are blessed into are provided by C<_table_class>, C<_query_class> & C<_row_class> methods.
So to subclass all the C<DBIx::DBO::*> objects, we need to provide our own class names via those methods.

  package MySubClass;
  our @ISA = qw(DBIx::DBO);
  
  sub _table_class { 'MySubClass::Table' }
  sub _query_class { 'MySubClass::Query' }
  sub _row_class   { 'MySubClass::Row' }
  
  ...

  package MySubClass::Table
  our @ISA = qw(DBIx::DBO::Table);
  
  ...

Now all new objects created will be blessed into these classes.

This leaves only the C<DBIx::DBO::DBD> hidden class, which acts as a SQL engine.
This class is also determined in the same way as other objects,
so to subclass C<DBIx::DBO::DBD> add a C<_dbd_class> method to C<DBIx::DBO> with the new class name.

  sub _dbd_class { 'MySubClass::DBD' }

Since databases differ slightly in their SQL, this class contains all the SQL specific calls for different DBDs.
They are found in the class C<DBIx::DBO::DBD::xxx> where I<xxx> is the name of the driver for this DBI handle.
A MySQL connection would have a DBD class of C<DBIx::DBO::DBD::mysql>, and SQLite would use C<DBIx::DBO::DBD::SQLite>.
These classes would both inherit from C<DBIx::DBO::DBD>.

When subclassing C<DBIx::DBO::DBD>, because it uses multiple inheritance, the 'C3' method resolution order is required.
This is setup for you automatically when the connection is first made.
These classes are also automatically created if they don't exist.

=head1 AUTHOR

Vernon Lyon, C<< <vlyon AT cpan.org> >>

=head1 SUPPORT

You can find more information for this module at:

=over 4

=item *

RT: CPAN's request tracker L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=DBIx-DBO>

=item *

AnnoCPAN: Annotated CPAN documentation L<http://annocpan.org/dist/DBIx-DBO>

=item *

CPAN Ratings L<http://cpanratings.perl.org/d/DBIx-DBO>

=item *

Search CPAN L<http://search.cpan.org/dist/DBIx-DBO>

=back


=head1 BUGS

Please report any bugs or feature requests to C<bug-dbix-dbo AT rt.cpan.org>, or through the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DBIx-DBO>.  I will be notified, and then you'll automatically be notified of progress on your bug as I make changes.


=head1 COPYRIGHT & LICENSE

Copyright 2009-2012 Vernon Lyon, all rights reserved.

This package is free software; you can redistribute it and/or modify it under the same terms as Perl itself.


=head1 SEE ALSO

L<DBI>, L<DBIx::SearchBuilder>.


=cut

