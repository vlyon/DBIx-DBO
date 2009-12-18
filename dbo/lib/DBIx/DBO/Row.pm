package DBIx::DBO::Row;
use DBIx::DBO::Common;
use Scalar::Util 'weaken';

use strict;
use warnings;

use overload '@{}' => sub {${$_[0]}->{array} || []}, '%{}' => sub {${$_[0]}->{hash}};
use overload '**' => \&value, fallback => 1;

sub dbh { ${$_[0]}->{DBO}->dbh }
sub rdbh { ${$_[0]}->{DBO}->rdbh }

=head2 config

  $parent_setting = $dbo->config($option)
  $dbo->config($option => $parent_setting)

Get or set the parent (if it has one) or dbo or global config settings.
When setting an option, the previous value is returned.

=cut

sub config {
    my $me = shift;
    ($$me->{Parent} // $$me->{DBO})->config(@_);
}

sub _new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $me = \{ DBO => shift, Parent => shift, array => undef, hash => {} };
    blessed $$me->{DBO} and $$me->{DBO}->isa('DBIx::DBO') or ouch 'Invalid DBO Object';
    ouch 'Invalid Parent Object' unless defined $$me->{Parent};
    $$me->{Parent} = $$me->{DBO}->table($$me->{Parent}) unless blessed $$me->{Parent};
    _init($me);
    bless $me, $class;
    return wantarray ? ($me, $me->_tables) : $me;
}

sub _init {
    my $me = shift;
    $$me->{build_data}{LimitOffset} = [1];
    if ($$me->{Parent}->isa('DBIx::DBO::Query')) {
        $$me->{Tables} = [ @{$$me->{Parent}{Tables}} ];
        _copy_build_data($me);
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

sub _tables {
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

sub _showing {
    my $me = shift;
    @{$$me->{build_data}{Showing}} ? @{$$me->{build_data}{Showing}} : @{$$me->{Tables}};
}

sub _column_idx {
    my $me = shift;
    my $col = shift;
    my $idx = -1;
    for my $shown ($me->_showing) {
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

=head2 value

  $row->value($column)

Return the value in this field.
$column can be a column name or a column object.

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

=head2 load

  $row->load(id => 123);
  $row->load(name => 'Bob', status => 'Employed');

Fetch a new row using the where definition specified.
Returns the Row object if the row is found and loaded successfully.
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

=head2 update

  $row->update(id => 123);
  $row->update(name => 'Bob', status => 'Employed');

Updates the current row with the new values specified.
Returns the number of rows updated or '0E0' for no rows to ensure the value is true,
and returns false if there was an error.

Note: If LIMIT is supported on UPDATEs then only the first matching row will be updated
otherwise ALL rows matching the current row will be updated.

=cut

sub update {
    my $me = shift;
    ouch 'No current record to update!' unless $$me->{array};
    my $build_data = $me->_build_data_matching_this_row;
    # TODO: LimitOffset
#    $h{LimitOffset} = [1] if ???
    my $sql = $me->_build_sql_update($build_data, @_);

    # TODO: Reload/update instead of leaving the row empty?
    # To update the Row object is difficult because columns may have been aliased
    undef $$me->{array};
    undef %$me;
    $me->do($sql, undef, $me->_bind_params_update($build_data));
}

=head2 delete

  $row->delete;

Deletes the current row.
Returns the number of rows deleted or '0E0' for no rows to ensure the value is true,
and returns false if there was an error.

Note: If LIMIT is supported on DELETEs then only the first matching row will be deleted
otherwise ALL rows matching the current row will be deleted.

=cut

sub delete {
    my $me = shift;
    ouch 'No current record to delete!' unless $$me->{array};
    my $build_data = $me->_build_data_matching_this_row;
    # TODO: LimitOffset
#    $h{LimitOffset} = [1] if ???
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

sub DESTROY {
    undef %${$_[0]};
}

1;
