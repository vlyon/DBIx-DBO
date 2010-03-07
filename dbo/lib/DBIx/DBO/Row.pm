package DBIx::DBO::Row;
use DBIx::DBO::Common;
use Scalar::Util 'weaken';

use strict;
use warnings;

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
    bless $me, $$me->{DBO}->_create_dbd_class($class, __PACKAGE__);
    $me->_init;
    return wantarray ? ($me, $me->tables) : $me;
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

sub _copy_build_data {
    my $me = shift;
    # Store needed build_data
    for (qw(Showing from From_Bind Quick_Where Where_Data Where_Bind group Group_Bind order Order_Bind)) {
        $$me->{build_data}{$_} = $$me->{Parent}{build_data}{$_} if exists $$me->{Parent}{build_data}{$_};
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

    # TODO: Shouldn't this replace the Quick_Where?
    my $old_qw = $#{$$me->{build_data}{Quick_Where}};
    push @{$$me->{build_data}{Quick_Where}}, @_;
    undef $$me->{build_data}{where};
    my $sql = $me->_build_sql_select($$me->{build_data});
    $old_qw < 0 ? delete $$me->{build_data}{Quick_Where} : ($#{$$me->{build_data}{Quick_Where}} = $old_qw);

    undef $$me->{array};
    undef %$me;
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
        # TODO: Save configs from Parent
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

    # TODO: Reload/update instead of leaving the row empty?
    # To update the Row object is difficult because columns may have been aliased
    undef $$me->{array};
    undef %$me;
    $me->do($sql, undef, $me->_bind_params_update($build_data));
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
    undef %$me;
    $me->do($sql, undef, $me->_bind_params_delete($build_data));
}

sub _build_data_matching_this_row {
    my $me = shift;
    # Identify the row by the PrimaryKeys if any, otherwise by all Columns
    my @cols;
    for my $tbl (@{$$me->{Tables}}) {
        push @cols, map $tbl ** $_, @{$tbl->{ @{$tbl->{PrimaryKeys}} ? 'PrimaryKeys' : 'Columns' }};
    }
    my %h = (
        from => $$me->{build_data}{from},
        Quick_Where => [ map {$_ => $me->value($_)} @cols ]
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

  $dbo->do($statement)         or die $dbo->dbh->errstr;
  $dbo->do($statement, \%attr) or die $dbo->dbh->errstr;
  $dbo->do($statement, \%attr, @bind_values) or die ...

This provides access to the L<DBI-E<gt>do|DBI/"do"> method.  It defaults to using the I<read-write> C<DBI> handle.

=head3 C<config>

  $row_setting = $dbo->config($option);
  $dbo->config($option => $row_setting);

Get or set the C<Row> config settings.  When setting an option, the previous value is returned.  When getting an option's value, if the value is undefined, the C<Query> object (If the the C<Row> belongs to one) or L<DBIx::DBO|DBIx::DBO>'s value is returned.

See L<DBIx::DBO/available_config_options>.

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

=head1 SEE ALSO

L<DBIx::DBO>


=cut

