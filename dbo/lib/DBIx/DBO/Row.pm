package DBIx::DBO::Row;

use strict;
use warnings;
use DBIx::DBO::Common;
use Scalar::Util 'weaken';
our @ISA;

use overload '@{}' => sub {${$_[0]}->{array} || []}, '%{}' => sub {${$_[0]}->{hash}};
use overload '**' => \&value, fallback => 1;

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
  $row->load(id => $row ** boss_id)->delete or die "Can't kill the boss";

=head1 METHODS

=head3 C<new>

  DBIx::DBO::Row->new($dbo, $table_object);
  DBIx::DBO::Row->new($dbo, $query_object);

Create and return a new C<Row> object.

=cut

sub dbh { ${$_[0]}->{DBO}->dbh }
sub rdbh { ${$_[0]}->{DBO}->rdbh }

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $me = \{ DBO => shift, Parent => shift, array => undef, hash => {} };
    blessed $$me->{DBO} and $$me->{DBO}->isa('DBIx::DBO') or ouch 'Invalid DBO Object';
    ouch 'Invalid Parent Object' unless defined $$me->{Parent};
    $$me->{Parent} = $$me->{DBO}->table($$me->{Parent}) unless blessed $$me->{Parent};
    bless $me, $class->_set_dbd_inheritance($$me->{DBO}{dbd});
    $me->_init;
    return wantarray ? ($me, $me->tables) : $me;
}

sub _set_dbd_inheritance {
    my $class = shift;
    my $dbd = shift;
    # Let DBIx::DBO::Row secretly inherit from DBIx::DBO::Common
    @_ = (@ISA, 'DBIx::DBO::Common') if not @_ and $class eq __PACKAGE__;
    $class->DBIx::DBO::Common::_set_dbd_inheritance($dbd, @_);
}

sub _init {
    my $me = shift;
    $$me->{build_data}{LimitOffset} = [1];
    if ($$me->{Parent}->isa('DBIx::DBO::Query')) {
        $$me->{Tables} = [ @{$$me->{Parent}{Tables}} ];
        $me->_copy_build_data;
        # We must weaken this to avoid a circular reference
        weaken $$me->{Parent};
    } elsif ($$me->{Parent}->isa('DBIx::DBO::Table')) {
        $$me->{build_data} = {
            show => '*',
            Showing => [],
            from => $$me->{Parent}->_quoted_name,
            group => '',
            order => '',
        };
        $$me->{Tables} = [ delete $$me->{Parent} ];
    } else {
        ouch 'Invalid Parent Object';
    }
}

sub _copy {
    my $val = shift;
    ref $val eq 'ARRAY' ? [map _copy($_), @$val] : ref $val eq 'HASH' ? {map _copy($_), %$val} : $val;
}

sub _copy_build_data {
    my $me = shift;
    # Store needed build_data
    for my $f (qw(Showing from From_Bind Quick_Where Where_Data Where_Bind group Group_Bind order Order_Bind)) {
        $$me->{build_data}{$f} = _copy($$me->{Parent}{build_data}{$f}) if exists $$me->{Parent}{build_data}{$f};
    }
}

=head3 C<tables>

Return a list of L<DBIx::DBO::Table|DBIx::DBO::Table> objects for this row.

=cut

sub tables {
    my $me = shift;
    @{$$me->{Tables}};
}

sub _table_idx {
    my ($me, $tbl) = @_;
    for my $i (0 .. $#{$$me->{Tables}}) {
        return $i if $tbl == $$me->{Tables}[$i];
    }
    return;
}

sub _table_alias {
    my ($me, $tbl) = @_;
    my $i = $me->_table_idx($tbl);
    ouch 'The table is not in this query' unless defined $i;
    @{$$me->{Tables}} > 1 ? 't'.($i + 1) : ();
}

sub _column_idx {
    my $me = shift;
    my $col = shift;
    my $idx = -1;
    for my $shown (@{$$me->{build_data}{Showing}} ? @{$$me->{build_data}{Showing}} : @{$$me->{Tables}}) {
        if (blessed $shown and $shown->isa('DBIx::DBO::Table')) {
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

  $query->column($column_name);
  $query->column($column_or_alias_name, 1);

Returns a reference to a column for use with other methods.

=cut

sub column {
    my ($me, $col, $_check_aliases) = @_;
    if ($_check_aliases) {
        for my $fld (@{$$me->{build_data}{Showing}}) {
            return $$me->{Column}{$col} ||= bless [$me, $col], 'DBIx::DBO::Column'
                if !blessed $fld and exists $fld->[2]{AS} and $col eq $fld->[2]{AS};
        }
    }
    for my $tbl ($me->tables) {
        return $tbl->column($col) if exists $tbl->{Column_Idx}{$col};
    }
    ouch 'No such column'.($_check_aliases ? '/alias' : '').': '.$me->_qi($col);
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
    my $me = shift;
    my $col = shift;
    ouch 'The record is empty' unless $$me->{array};
    if (blessed $col and $col->isa('DBIx::DBO::Column')) {
        my $i = $me->_column_idx($col);
        return $$me->{array}[$i] if defined $i;
        ouch 'The field '.$me->_qi($col->[0]{Name}, $col->[1]).' was not included in this query';
    }
    return $$me->{hash}{$col} if exists $$me->{hash}{$col};
    ouch 'No such column: '.$col;
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
    my $sql = $me->_build_sql_select($$me->{build_data});
    $old_qw < 0 ? delete $$me->{build_data}{Quick_Where} : ($#{$$me->{build_data}{Quick_Where}} = $old_qw);

    undef $$me->{array};
    $$me->{hash} = {};
    $me->_sql($sql, $me->_bind_params_select($$me->{build_data}));
    my $sth = $me->rdbh->prepare($sql);
    return unless $sth and $sth->execute($me->_bind_params_select($$me->{build_data}));
    my $i = 1;
    for (@{$sth->{NAME}}) {
        $sth->bind_col($i, \$$me->{hash}{$_}) unless exists $$me->{hash}{$_};
        $i++;
    }
    $$me->{array} = $sth->fetch or return;
    $sth->finish;
    $me;
}

sub _detach {
    my $me = shift;
    if ($$me->{Parent}) {
        $$me->{array} = [ @$me ];
        $$me->{hash} = { %$me };
        undef $$me->{Parent}{Row};
        # Save config from Parent
        if ($$me->{Parent}{Config} and %{$$me->{Parent}{Config}}) {
            $$me->{Config} = { %{$$me->{Parent}{Config}}, $$me->{Config} ? %{$$me->{Config}} : () };
        }
    }
    undef $$me->{Parent};
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
    ouch 'No current record to update!' unless $$me->{array};
    my $build_data = $me->_build_data_matching_this_row;
    $build_data->{LimitOffset} = [1] if $me->config('LimitRowUpdate') and $me->tables == 1;
    my $sql = $me->_build_sql_update($build_data, @_);

    my $rv = $me->do($sql, undef, $me->_bind_params_update($build_data));
    $me->_reset_on_update($build_data, @_) if $rv and $rv > 0;
    return $rv;
}

sub _reset_on_update {
    my $me = shift;
    # TODO: Reload/update instead of leaving the row empty?
    # To update the Row object is difficult because columns may have been aliased
    undef $$me->{array};
    $$me->{hash} = {};
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
    ouch 'No current record to delete!' unless $$me->{array};
    my $build_data = $me->_build_data_matching_this_row;
    $build_data->{LimitOffset} = [1] if $me->config('LimitRowDelete') and $me->tables == 1;
    my $sql = $me->_build_sql_delete($build_data, @_);

    undef $$me->{array};
    $$me->{hash} = {};
    $me->do($sql, undef, $me->_bind_params_delete($build_data));
}

sub _build_data_matching_this_row {
    my $me = shift;
    # Identify the row by the PrimaryKeys if any, otherwise by all Columns
    my @quick_where;
    for my $tbl (@{$$me->{Tables}}) {
        for my $col (map $tbl ** $_, @{$tbl->{ @{$tbl->{PrimaryKeys}} ? 'PrimaryKeys' : 'Columns' }}) {
            my $i = $me->_column_idx($col);
            defined $i or ouch 'The '.$me->_qi($tbl->{Name}, $col->[1]).' field needed to identify this row, was not included in this query';
            push @quick_where, $col => $$me->{array}[$i];
        }
    }
    my %h = (
        from => $$me->{build_data}{from},
        Quick_Where => \@quick_where
    );
    $h{From_Bind} = $$me->{build_data}{From_Bind} if exists $$me->{build_data}{From_Bind};
    return \%h;
}

=head2 Common Methods

These methods are accessible from all DBIx::DBO* objects.

=head3 C<dbh>

The I<read-write> C<DBI> handle.

=head3 C<rdbh>

The I<read-only> C<DBI> handle, or if there is no I<read-only> connection, the I<read-write> C<DBI> handle.

=head3 C<do>

  $row->do($statement)         or die $row->dbh->errstr;
  $row->do($statement, \%attr) or die $row->dbh->errstr;
  $row->do($statement, \%attr, @bind_values) or die ...

This provides access to the L<DBI-E<gt>do|DBI/"do"> method.  It defaults to using the I<read-write> C<DBI> handle.

=head3 C<config>

  $row_setting = $row->config($option);
  $row->config($option => $row_setting);

Get or set the C<Row> config settings.  When setting an option, the previous value is returned.  When getting an option's value, if the value is undefined, the C<Query> object (If the the C<Row> belongs to one) or L<DBIx::DBO|DBIx::DBO>'s value is returned.

See L<DBIx::DBO/Available_config_options>.

=cut

sub config {
    my $me = shift;
    my $opt = shift;
    return $me->_set_config($$me->{Config} ||= {}, $opt, shift) if @_;
    return defined $$me->{Config}{$opt} ? $$me->{Config}{$opt} :
        (defined $$me->{Parent} ? $$me->{Parent} : $$me->{DBO})->config($opt);
}

sub DESTROY {
    undef %${$_[0]};
}

1;

__END__

=head1 SUBCLASSING

When subclassing C<DBIx::DBO::Row>, please note that C<Row> objects created with the L</new> method are blessed into a DBD driver specific module.
For example, if using MySQL, a new C<Row> object will be blessed into C<DBIx::DBO::Row::DBD::mysql> which inherits from C<DBIx::DBO::Row>.
However if objects are created from a subclass called C<MySubClass> the new object will be blessed into C<MySubClass::DBD::mysql> which will inherit from both C<MySubClass> and C<DBIx::DBO::Row::DBD::mysql>.

Classes can easily be created for tables in your database.
Assume you want to create a simple C<Row> class for a "Users" table:

  package My::User;
  use base 'DBIx::DBO::Row';
  
  sub new {
      my $class = shift;
      my $dbo = shift;
      
      $class->SUPER::new($dbo, 'Users'); # Create the Row for the "Users" table only
  }

=head1 SEE ALSO

L<DBIx::DBO>


=cut

