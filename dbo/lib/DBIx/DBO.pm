package DBIx::DBO;

use 5.010;
use strict;
use warnings;
use DBI;
use DBIx::DBO::Common;
use DBIx::DBO::Handle;
use DBIx::DBO::Table;
use DBIx::DBO::Query;
use DBIx::DBO::Row;

=head1 NAME

DBIx::DBO - An OO interface to SQL queries and results.  Easily constructs SQL queries, and simplifies processing of the returned data.

=cut

our $VERSION = '0.01_01';

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
      $row->delete if $record->{id} == 27;
  }

  # Join tables (INNER JOIN)
  my ($query, $table1, $table2, $table3) = $dbo->query('my_table', 't2', 'third');
  $query->join_on($table2 ** 'parent_id', '=', $table3 ** 'child_id');

  # Join tables (LEFT JOIN)
  my ($query, $table1) = $dbo->query('my_table');
  my $table2 = $query->join_table('another_table', 'LEFT');
  $query->join_on($table2 ** 'parent_id', '=', $table1 ** 'child_id');

=head1 DESCRIPTION

This module provides a convenient and efficient way to access a database. It can construct queries for you and returns the results in easy to use methods.

=head1 METHODS

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

=head2 config

  $global_setting = DBIx::DBO->config($option);
  DBIx::DBO->config($option => $global_setting);

Get or set the global config settings.
When setting an option, the previous value is returned.

=cut

sub config {
    my $me = shift;
    my $opt = shift;
    my $val = $Config{$opt};
    $Config{$opt} = shift if @_;
    return $val;
}

=head2 connect

  $dbo = DBIx::DBO->connect($data_source, $username, $password, \%attr)
      or die $DBI::errstr;

Takes the same arguments as L<DBI-E<gt>connect|DBI/"connect"> for a read-write connection to a database. It returns the DBIx::DBO object if the connection succeeds or undefined on failure.

=head2 connect_readonly

Takes the same arguments as C<connect> for a read-only connection to a database. It returns the C<DBIx::DBO> object if the connection succeeds or undefined on failure.

Both C<connect> & C<connect_readonly> can be called on a C<DBIx::DBO> object to add that respective connection to create a C<DBIx::DBO> with both read-write and read-only connections.

  my $dbo = DBIx::DBO->connect($master_dsn, $username, $password, \%attr)
      or die $DBI::errstr;
  $dbo->connect_readonly($slave_dsn, $username, $password, \%attr)
      or die $DBI::errstr;

=cut

sub connect {
    my $me = shift;
    my $new = { rdbh => undef, ConnectArgs => [], ConnectReadOnlyArgs => [] };
    $new->{dbh} = $me->_connect($new->{ConnectArgs}, @_) or return;
    my $class = $me->_require_dbd_class($new->{dbh}{Driver}{Name});
    unless ($class) {
        $new->{dbh}->set_err('', $@);
        return;
    }
    $class->_bless_dbo($new);
}

sub connect_readonly {
    my $me = shift;
    my $new = { dbh => undef, ConnectArgs => [], ConnectReadOnlyArgs => [] };
    $new->{rdbh} = $me->_connect($new->{ConnectReadOnlyArgs}, @_) or return;
    my $class = $me->_require_dbd_class($new->{rdbh}{Driver}{Name});
    unless ($class) {
        $new->{rdbh}->set_err('', $@);
        return;
    }
    $class->_bless_dbo($new);
}

sub _connect {
    my $me = shift;
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

sub _require_dbd_class {
    my $me = shift;
    my $dbd = shift;
    my $class = $me.'::DBD::'.$dbd;

    __PACKAGE__->_require_dbd_class($dbd) or return if $me ne __PACKAGE__;

    my @warn;
    {
        local $SIG{__WARN__} = sub { push @warn, join '', @_ };
        return $me->_set_inheritance($dbd) if eval "require $class";
    }

    (my $file = $class.'.pm') =~ s'::'/'g;
    if ($@ !~ / \Q$file\E in \@INC /) {
        # Set $DBI::errstr
        (my $err = $@) =~ s/\n.*$//; # Remove the last line
        chomp @warn;
        chomp $err;
        $@ = join "\n", "Can't load $dbd driver", @warn, $err;
        return;
    }

    delete $INC{$file};
    $INC{$file} = 1;
    return $me->_set_inheritance($dbd);
}

sub _set_inheritance {
    my $me = shift;
    my $dbd = shift;
    my $class = $me.'::DBD::'.$dbd;

    no strict 'refs';
    @{$class.'::Common::ISA'} = ($me.'::Common');
    @{$class.'::'.$_.'::ISA'} = ($me.'::'.$_, $class.'::Common') for qw(Handle Table Query Row);
    if ($me ne __PACKAGE__) {
        push @{$class.'::'.$_.'::ISA'}, __PACKAGE__.'::DBD::'.$dbd.'::'.$_ for qw(Handle Table Query Row);
        eval "package ${me}::$_" for qw(Common Handle Table Query Row);
    }
    return $class.'::Handle';
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
