package DBIx::DBO;

use 5.008;
use strict;
use warnings;
use DBI;
use Carp qw(carp croak);

our $VERSION;
our @ISA = qw(DBIx::DBO::Common);
our %Config;
my $need_c3_initialize;
my @ConnectArgs;

BEGIN {
    $VERSION = '0.13';
    # The C3 method resolution order is required.
    if ($] < 5.009_005) {
        require MRO::Compat;
    } else {
        require mro;
    }
    # Import %Config
    *Config = \%DBIx::DBO::Common::Config;
}

use DBIx::DBO::Common;
use DBIx::DBO::Table;
use DBIx::DBO::Query;
use DBIx::DBO::Row;

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
    while (my ($opt, $val) = splice @_, 0, 2) {
        if (exists $Config{$opt}) {
            DBIx::DBO::Common->_set_config(\%Config, $opt, $val);
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
    my ($dbh, $rdbh, $new) = @_;

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

    my $class = $me->_require_dbd_class($new->{dbd});
    Class::C3::initialize() if $need_c3_initialize;
    $class->_init($new);
}

sub _init {
    my $class = shift;
    my $new = shift;
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
        my ($dsn, $user, $auth, $attr) = @_;
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

sub _require_dbd_class {
    my $me = shift;
    my $dbd = shift;
    $me = ref $me if ref $me;
    $me =~ s/::DBD::\w+$//;
    my $class = $me.'::DBD::'.$dbd;

    __PACKAGE__->_require_dbd_class($dbd) if $me ne __PACKAGE__;

    my $rv;
    my @warn;
    {
        local $SIG{__WARN__} = sub { push @warn, join '', @_ };
        $rv = eval "require $class";
    }
    if ($rv) {
        warn @warn if @warn;
        return $me->_set_dbd_inheritance($dbd);
    }

    (my $file = $class.'.pm') =~ s'::'/'g;
    if ($@ !~ / \Q$file\E in \@INC /) {
        (my $err = $@) =~ s/\n.*$//; # Remove the last line
        chomp @warn;
        chomp $err;
        croak join "\n", @warn, $err, "Can't load $dbd driver";
    }

    $@ = '';
    delete $INC{$file};
    $INC{$file} = 1;
    return $me->_set_dbd_inheritance($dbd);
}

sub _set_dbd_inheritance {
    my $class = shift;
    my $dbd = shift;

    my $dbd_class = $class->SUPER::_set_dbd_inheritance($dbd);

    # Delay Class::C3::initialize until later
    local *Class::C3::initialize;
    *Class::C3::initialize = sub { $need_c3_initialize = 1 };

    for my $obj_class (map $dbd_class->$_, qw(_table_class _query_class _row_class)) {
        $obj_class->_set_dbd_inheritance($dbd);
    }

    return $dbd_class;
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

  my ($query, $table1, $table2) = $dbo->query(['my_schema', 'my_table'], 'my_other_table');

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

=head3 C<table_info>

  $dbo->table_info($table);
  $dbo->table_info([$schema, $table]);
  $dbo->table_info($table_object);

Returns a hashref containing C<PrimaryKeys>, C<Columns> and C<Column_Idx> for the table.
Mainly for internal use.

=cut

sub _get_table_schema {
    my $me = shift;
    my $schema = my $q_schema = shift;
    my $table = my $q_table = shift;

    $q_schema =~ s/([\\_%])/\\$1/g if defined $q_schema;
    $q_table =~ s/([\\_%])/\\$1/g;

    # First try just these types
    my $info = $me->rdbh->table_info(undef, $q_schema, $q_table,
        'TABLE,VIEW,GLOBAL TEMPORARY,LOCAL TEMPORARY,SYSTEM TABLE')->fetchall_arrayref;
    # Then if we found nothing, try any type
    $info = $me->rdbh->table_info(undef, $q_schema, $q_table)->fetchall_arrayref if $info and @$info == 0;
    croak 'Invalid table: '.$me->_qi($table) unless $info and @$info == 1 and $info->[0][2] eq $table;
    return $info->[0][1];
}

sub _get_table_info {
    my $me = shift;
    my $schema = shift;
    my $table = shift;

    my $cols = $me->rdbh->column_info(undef, $schema, $table, '%')->fetchall_arrayref({});
    croak 'Invalid table: '.$me->_qi($table) unless @$cols;

    my %h;
    $h{Column_Idx}{$_->{COLUMN_NAME}} = $_->{ORDINAL_POSITION} for @$cols;
    $h{Columns} = [ sort { $h{Column_Idx}{$a} cmp $h{Column_Idx}{$b} } keys %{$h{Column_Idx}} ];

    $h{PrimaryKeys} = [];
    $me->_set_table_key_info($schema, $table, \%h);

    $me->{TableInfo}{defined $schema ? $schema : ''}{$table} = \%h;
}

sub _set_table_key_info {
    my $me = shift;
    my $schema = shift;
    my $table = shift;
    my $h = shift;
    if (my $keys = $me->rdbh->primary_key_info(undef, $schema, $table)) {
        $h->{PrimaryKeys}[$_->{KEY_SEQ} - 1] = $_->{COLUMN_NAME} for @{$keys->fetchall_arrayref({})};
    }
}

sub _unquote_table {
    my $me = shift;
    # TODO: Better splitting of: schema.table or `schema`.`table` or "schema"."table"@"catalog" or ...
    $_[0] =~ /^(?:("|)(.+)\1\.|)("|)(.+)\3$/ or croak "Invalid table: \"$_[0]\"";
    return ($4, $2);
}

sub table_info {
    my $me = shift;
    my $table = shift;
    my $schema;
    croak 'No table name supplied' unless defined $table and length $table;

    if (UNIVERSAL::isa($table, 'DBIx::DBO::Table')) {
        ($schema, $table) = @$table{qw(Schema Name)};
    } else {
        if (ref $table eq 'ARRAY') {
            ($schema, $table) = @$table;
        } else {
            ($table, $schema) = $me->_unquote_table($table);
        }
        defined $schema or $schema = $me->_get_table_schema($schema, $table);

        $me->_get_table_info($schema, $table) unless exists $me->{TableInfo}{defined $schema ? $schema : ''}{$table};
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
    delete $me->{LastSQL};
    return;
}

=head2 Common Methods

These methods are accessible from all DBIx::DBO* objects.

=head3 C<dbh>

The I<read-write> C<DBI> handle.

=head3 C<rdbh>

The I<read-only> C<DBI> handle, or if there is no I<read-only> connection, the I<read-write> C<DBI> handle.

=head3 C<do>

  $dbo->do($statement)         or die $dbo->dbh->errstr;
  $dbo->do($statement, \%attr) or die $dbo->dbh->errstr;
  $dbo->do($statement, \%attr, @bind_values) or die ...

This provides access to the L<DBI-E<gt>do|DBI/"do"> method.  It defaults to using the I<read-write> C<DBI> handle.

=cut

sub _handle {
    my $me = shift;
    my $handle = shift;
    my ($d, $c) = $handle ne 'read-only' ? qw(dbh ConnectArgs) : qw(rdbh ConnectReadOnlyArgs);
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

Get or set the global or C<DBIx::DBO> config settings.  When setting an option, the previous value is returned.  When getting an option's value, if the value is undefined, the global value is returned.

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
        my $cfg = ref $me ? $me->{Config} ||= {} : \%Config;
        return $me->_set_config($cfg, $opt, $_[2]);
    }
    return (ref $me and defined $me->{Config}{$opt}) ? $me->{Config}{$opt} : $Config{$opt};
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

The C<DBIx::DBO> modules use multiple inheritance, because objects created are blessed into DBD driver specific classes.
For this to function correctly they must use the 'C3' method resolution order.
To simplify subclassing this is automatically set for you when the objects are first created.

For example, if using MySQL and a subclass of C<DBIx::DBO> named C<MySubClass>, then the object returned from the L</connect>, L</connect_readonly> or L</new> method would be blessed into C<MySubClass::DBD::mysql> which would inherit from both C<MySubClass> and C<DBIx::DBO::DBD::mysql>.
These classes are automatically created if they don't exist.

In this way it is fairly trivial to override most of the methods, but not all of them.
This is because some methods are common to all the classes and are defined in C<DBIx::DBO::Common>.
And new C<Table>, C<Query> and C<Row> objects created will still be blessed into C<DBIx::DBO::*> classes.
The classes these new objects are blessed into are provided by C<_table_class>, C<_query_class> and C<_row_class> methods.
To override these methods and subclass the whole of C<DBIx::DBO::*>, we need to provide our own C<MySubClass::Common> package with new class names for our objects.

  package MySubClass::Common;
  our @ISA = qw(DBIx::DBO::Common);
  
  sub _table_class { 'MySubClass::Table' }
  sub _query_class { 'MySubClass::Query' }
  sub _row_class   { 'MySubClass::Row' }

Also ensure that we inherit from this common class.

  package MySubClass;
  our @ISA = qw(DBIx::DBO MySubClass::Common);
  ...

  package MySubClass::Table;
  our @ISA = qw(DBIx::DBO::Table MySubClass::Common);
  ...

  package MySubClass::Query;
  our @ISA = qw(DBIx::DBO::Query MySubClass::Common);
  ...

  package MySubClass::Row;
  our @ISA = qw(DBIx::DBO::Row MySubClass::Common);
  ...

In this example we will have subclassed all the modules.

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

Copyright 2009-2011 Vernon Lyon, all rights reserved.

This package is free software; you can redistribute it and/or modify it under the same terms as Perl itself.


=head1 SEE ALSO

L<DBI>, L<DBIx::SearchBuilder>.


=cut

1;
