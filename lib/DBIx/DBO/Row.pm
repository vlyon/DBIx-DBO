package DBIx::DBO::Row;

use strict;
use warnings;
use Carp 'croak';
use Scalar::Util qw(blessed weaken);
use Storable ();

use overload '@{}' => sub {${$_[0]}->{array} || []}, '%{}' => sub {${$_[0]}->{hash}}, '**' => \&value, fallback => 1;

sub _table_class { ${$_[0]}->{DBO}->_table_class }

=head1 NAME

DBIx::DBO::Row - An OO interface to SQL queries and results.  Encapsulates a fetched row of data in an object.

=head1 SYNOPSIS

  # Create a Row object for the `users` table
  my $row = $dbo->row('users');
  
  # Load my record
  $row->load(login => 'vlyon') or die "Where am I?";
  
  # Double my salary :)
  $row->update(salary => {FUNC => '? * 2', COL => 'salary'});
  
  # Print my email address
  print $row ** 'email';  # Short for: $row->value('email')
  
  # Delete my boss
  $row->load(id => $row ** 'boss_id')->delete or die "Can't kill the boss";

=head1 METHODS

=head3 C<new>

  DBIx::DBO::Row->new($dbo, $table);
  DBIx::DBO::Row->new($dbo, $query_object);

Create and return a new C<Row> object.
The object returned represents rows in the given table/query.
Can take the same arguments as L<DBIx::DBO::Table/new> or a L<Query|DBIx::DBO::Query> object can be used.

=cut

sub new {
    my $proto = shift;
    UNIVERSAL::isa($_[0], 'DBIx::DBO') or croak 'Invalid DBO Object for new Row';
    my $class = ref($proto) || $proto;
    $class->_init(@_);
}

sub _init {
    my($class, $dbo, $parent) = @_;
    croak 'Missing parent for new Row' unless defined $parent;

    my $me = bless \{ DBO => $dbo, array => undef, hash => {} }, $class;
    $parent = $me->_table_class->new($dbo, $parent) unless blessed $parent;

    $$me->{build_data}{LimitOffset} = [1];
    if ($parent->isa('DBIx::DBO::Query')) {
        $$me->{Parent} = $parent;
        # We must weaken this to avoid a circular reference
        weaken $$me->{Parent};
        $$me->{Tables} = [ @{$parent->{Tables}} ];
        $$me->{Columns} = $parent->{Columns};
        $me->_copy_build_data;
    } elsif ($parent->isa('DBIx::DBO::Table')) {
        $$me->{build_data} = {
            show => '*',
            Showing => [],
            from => $parent->_quoted_name,
            group => '',
            order => '',
        };
        $$me->{Tables} = [ $parent ];
        $$me->{Columns} = $parent->{Columns};
    } else {
        croak 'Invalid parent for new Row';
    }
    return wantarray ? ($me, $me->tables) : $me;
}

sub _copy_build_data {
    my $me = $_[0];
    # Store needed build_data
    for my $f (qw(Showing from From_Bind Quick_Where Where_Data Where_Bind group Group_Bind order Order_Bind)) {
        $$me->{build_data}{$f} = $me->_copy($$me->{Parent}{build_data}{$f}) if exists $$me->{Parent}{build_data}{$f};
    }
}

sub _copy {
    my($me, $val) = @_;
    return bless [$me, $val->[1]], 'DBIx::DBO::Column'
        if UNIVERSAL::isa($val, 'DBIx::DBO::Column') and $val->[0] == $$me->{Parent};
    ref $val eq 'ARRAY' ? [map $me->_copy($_), @$val] : ref $val eq 'HASH' ? {map $me->_copy($_), %$val} : $val;
}

=head3 C<tables>

Return a list of L<Table|DBIx::DBO::Table> objects for this row.

=cut

sub tables {
    @{${$_[0]}->{Tables}};
}

sub _table_idx {
    my($me, $tbl) = @_;
    for my $i (0 .. $#{$$me->{Tables}}) {
        return $i if $tbl == $$me->{Tables}[$i];
    }
    return;
}

sub _table_alias {
    my($me, $tbl) = @_;
    return undef if $tbl == $me;
    my $i = $me->_table_idx($tbl);
    croak 'The table is not in this query' unless defined $i;
    @{$$me->{Tables}} > 1 ? 't'.($i + 1) : ();
}

=head3 C<columns>

Return a list of column names.

=cut

sub columns {
    @{${$_[0]}->{Columns}};
}

sub _column_idx {
    my($me, $col) = @_;
    my $idx = -1;
    for my $shown (@{$$me->{build_data}{Showing}} ? @{$$me->{build_data}{Showing}} : @{$$me->{Tables}}) {
        if (UNIVERSAL::isa($shown, 'DBIx::DBO::Table')) {
            if ($col->[0] == $shown and exists $shown->{Column_Idx}{$col->[1]}) {
                return $idx + $shown->{Column_Idx}{$col->[1]};
            }
            $idx += keys %{$shown->{Column_Idx}};
            next;
        }
        $idx++;
        return $idx if not defined $shown->[1] and @{$shown->[0]} == 1 and $col == $shown->[0][0];
    }
    return;
}

=head3 C<column>

  $row->column($column_name);
  $row->column($alias_or_column_name, 1);

Returns a column reference from the name or alias.
By default only column names are searched, set the second argument to true to check column aliases and names.

=cut

sub column {
    my($me, $col, $_check_aliases) = @_;
    if ($_check_aliases) {
        for my $fld (@{$$me->{build_data}{Showing}}) {
            return $$me->{Column}{$col} ||= bless [$me, $col], 'DBIx::DBO::Column'
                if ref($fld) eq 'ARRAY' and exists $fld->[2]{AS} and $col eq $fld->[2]{AS};
        }
    }
    for my $tbl ($me->tables) {
        return $tbl->column($col) if exists $tbl->{Column_Idx}{$col};
    }
    croak 'No such column'.($_check_aliases ? '/alias' : '').': '.$$me->{DBO}{dbd_class}->_qi($me, $col);
}

=head3 C<value>

  $value = $row->value($column);
  $value = $row ** $column;

Return the value in the C<$column> field.  The C<**> method is a shortcut for the C<value> method.
C<$column> can be a column name or a C<Column> object.

Values in the C<Row> can also be obtained by using the object as an array/hash reference.

  $value = $row->[2];
  $value = $row->{some_column};

=cut

sub value {
    my($me, $col) = @_;
    croak 'The row is empty' unless $$me->{array};
    if (UNIVERSAL::isa($col, 'DBIx::DBO::Column')) {
        my $i = $me->_column_idx($col);
        return $$me->{array}[$i] if defined $i;
        croak 'The field '.$$me->{DBO}{dbd_class}->_qi($me, $col->[0]{Name}, $col->[1]).' was not included in this query';
    }
    return $$me->{hash}{$col} if exists $$me->{hash}{$col};
    croak 'No such column: '.$$me->{DBO}{dbd_class}->_qi($me, $col);
}

=head3 C<load>

  $row->load(id => 123);
  $row->load(name => 'Bob', status => 'Employed');

Fetch a new row using the where definition specified.
Returns the C<Row> object if the row is found and loaded successfully.
Returns an empty list if there is no row or an error occurs.

=cut

sub load {
    my $me = shift;

    $me->_detach;

    # Use Quick_Where to load a row, but make sure to restore its value afterward
    my $old_qw = $#{$$me->{build_data}{Quick_Where}};
    push @{$$me->{build_data}{Quick_Where}}, @_;
    undef $$me->{build_data}{where};
    my $sql = $$me->{DBO}{dbd_class}->_build_sql_select($me, $$me->{build_data});
    $old_qw < 0 ? delete $$me->{build_data}{Quick_Where} : ($#{$$me->{build_data}{Quick_Where}} = $old_qw);

    undef @{$$me->{Columns}};
    undef $$me->{array};
    $$me->{hash} = \my %hash;
    $$me->{DBO}{dbd_class}->_sql($me, $sql, $$me->{DBO}{dbd_class}->_bind_params_select($me, $$me->{build_data}));
    my $sth = $me->rdbh->prepare($sql);
    return unless $sth and $sth->execute($$me->{DBO}{dbd_class}->_bind_params_select($me, $$me->{build_data}));

    my $i;
    my @array;
    for (@{$$me->{Columns}} = @{$sth->{NAME}}) {
        $i++;
        $sth->bind_col($i, \$hash{$_}) unless exists $hash{$_};
    }
    $$me->{array} = $sth->fetch or return;
    $sth->finish;
    $me;
}

sub _detach {
    my $me = $_[0];
    if (exists $$me->{Parent}) {
        $$me->{Columns} = [ @{$$me->{Columns}} ];
        $$me->{array} = [ @$me ];
        $$me->{hash} = { %$me };
        undef $$me->{Parent}{Row};
        # Save config from Parent
        if ($$me->{Parent}{Config} and %{$$me->{Parent}{Config}}) {
            $$me->{Config} = { %{$$me->{Parent}{Config}}, $$me->{Config} ? %{$$me->{Config}} : () };
        }
    }
    delete $$me->{Parent};
}

=head3 C<update>

  $row->update(id => 123);
  $row->update(name => 'Bob', status => 'Employed');

Updates the current row with the new values specified.
Returns the number of rows updated or C<'0E0'> for no rows to ensure the value is true,
and returns false if there was an error.

Note: If C<LIMIT> is supported on C<UPDATE>s then only the first matching row will be updated
otherwise ALL rows matching the current row will be updated.

=cut

sub update {
    my $me = shift;
    croak "Can't update an empty row" unless $$me->{array};
    my @update = $$me->{DBO}{dbd_class}->_parse_set($me, @_);
    my $build_data = $$me->{DBO}{dbd_class}->_build_data_matching_this_row($me);
    $build_data->{LimitOffset} = [1] if $me->config('LimitRowUpdate') and $me->tables == 1;
    my $sql = $$me->{DBO}{dbd_class}->_build_sql_update($me, $build_data, @update);

    my $rv = $$me->{DBO}{dbd_class}->_do($me, $sql, undef, $$me->{DBO}{dbd_class}->_bind_params_update($me, $build_data));
    $$me->{DBO}{dbd_class}->_reset_row_on_update($me, $build_data, @update) if $rv and $rv > 0;
    return $rv;
}

=head3 C<delete>

  $row->delete;

Deletes the current row.
Returns the number of rows deleted or C<'0E0'> for no rows to ensure the value is true,
and returns false if there was an error.

Note: If C<LIMIT> is supported on C<DELETE>s then only the first matching row will be deleted
otherwise ALL rows matching the current row will be deleted.

=cut

sub delete {
    my $me = shift;
    croak "Can't delete an empty row" unless $$me->{array};
    my $build_data = $$me->{DBO}{dbd_class}->_build_data_matching_this_row($me);
    $build_data->{LimitOffset} = [1] if $me->config('LimitRowDelete') and $me->tables == 1;
    my $sql = $$me->{DBO}{dbd_class}->_build_sql_delete($me, $build_data, @_);

    undef $$me->{array};
    $$me->{hash} = {};
    $$me->{DBO}{dbd_class}->_do($me, $sql, undef, $$me->{DBO}{dbd_class}->_bind_params_delete($me, $build_data));
}

=head2 Common Methods

These methods are accessible from all DBIx::DBO* objects.

=head3 C<dbo>

The C<DBO> object.

=head3 C<dbh>

The I<read-write> C<DBI> handle.

=head3 C<rdbh>

The I<read-only> C<DBI> handle, or if there is no I<read-only> connection, the I<read-write> C<DBI> handle.

=cut

sub dbo { ${$_[0]}->{DBO} }
sub dbh { ${$_[0]}->{DBO}->dbh }
sub rdbh { ${$_[0]}->{DBO}->rdbh }

=head3 C<config>

  $row_setting = $row->config($option);
  $row->config($option => $row_setting);

Get or set the C<Row> config settings.  When setting an option, the previous value is returned.  When getting an option's value, if the value is undefined, the C<Query> object (If the the C<Row> belongs to one) or L<DBIx::DBO|DBIx::DBO>'s value is returned.

See L<DBIx::DBO/Available_config_options>.

=cut

sub config {
    my $me = shift;
    my $opt = shift;
    return $$me->{DBO}{dbd_class}->_set_config($$me->{Config} ||= {}, $opt, shift) if @_;
    $$me->{DBO}{dbd_class}->_get_config($opt, $$me->{Config} ||= {}, defined $$me->{Parent} ? ($$me->{Parent}{Config}) : (), $$me->{DBO}{Config}, \%DBIx::DBO::Config);
}

*STORABLE_freeze = sub {
    my($me, $cloning) = @_;
    return unless exists $$me->{Parent};

    # Simulate detached row
    local $$me->{Columns} = [ @{$$me->{Columns}} ];
    # Save config from Parent
    my $parent = delete $$me->{Parent};
    local $$me->{Config} = { %{$parent->{Config}}, $$me->{Config} ? %{$$me->{Config}} : () }
        if $parent->{Config} and %{$parent->{Config}};

    my $frozen = Storable::nfreeze($me);
    $$me->{Parent} = $parent;
    return $frozen;
} if $Storable::VERSION >= 2.38;

*STORABLE_thaw = sub {
    my($me, $cloning, @frozen) = @_;
    $$me = { %${ Storable::thaw(@frozen) } }; # Copy the hash, or Storable will wipe it out!
} if $Storable::VERSION >= 2.38;

sub DESTROY {
    undef %${$_[0]};
}

1;

__END__

=head1 SUBCLASSING

Classes can easily be created for tables in your database.
Assume you want to create a simple C<Row> class for a "Users" table:

  package My::User;
  our @ISA = qw(DBIx::DBO::Row);
  
  sub new {
      my($class, $dbo) = @_;
      
      $class->SUPER::new($dbo, 'Users'); # Create the Row for the "Users" table
  }

=head1 SEE ALSO

L<DBIx::DBO>


=cut

