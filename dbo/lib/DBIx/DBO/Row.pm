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
    if (defined $$me->{Parent}) {
        $$me->{Parent} = $$me->{DBO}->table($$me->{Parent}) unless blessed $$me->{Parent};
        if ($$me->{Parent}->isa('DBIx::DBO::Query')) {
            $$me->{Tables} = $$me->{Parent}{Tables};
            $$me->{Showing} = $$me->{Parent}{build_data}{Showing};
            # We must weaken this to avoid a circular reference
            weaken $$me->{Parent};
        } elsif ($$me->{Parent}->isa('DBIx::DBO::Table')) {
            $$me->{show} = [ '*' ];
            $$me->{from} = [ $$me->{Parent}->_quoted_name ];
            $$me->{group_order} = [ '' ];
            $$me->{Tables} = [ delete $$me->{Parent} ];
            $$me->{Showing} = [];
        } else {
            ouch 'Invalid Parent Object';
        }
    }
    bless $me, $class;
    return wantarray ? ($me, $me->_tables) : $me;
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
    @{$$me->{@{$$me->{Showing}} ? 'Showing' : 'Tables'}};
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
    my @bind;
    my $sql = 'SELECT '.$me->_build_show(\@bind);
    $sql .= ' FROM '.$me->_build_from(\@bind);
    $sql .= ' WHERE '.$_ if $_ = $me->_build_quick_where(\@bind, @_);
    $sql .= $me->_build_group_order(\@bind);
    undef $$me->{array};
    undef %$me;
    $me->_sql($sql, @bind);
    my $sth = $me->rdbh->prepare($sql);
    return unless $sth and $sth->execute(@bind);
    my $i = 1;
    for (@{$sth->{NAME}}) {
        $sth->bind_col($i, \$$me->{hash}{$_}) unless exists $$me->{hash}{$_};
        $i++;
    }
    $$me->{array} = $sth->fetch or return;
    $sth->finish;
    $me;
}

sub _build_show {
    my $me = shift;
    my $bind = shift;
    if ($$me->{show}) {
        push @$bind, @{$$me->{show}[1 .. $#{$$me->{show}}]} if $#{$$me->{show}} > 0;
        return $$me->{show}[0];
    }
    my $q = $$me->{Parent};
    $q->sql;
    push @$bind, @{$q->{build_data}{Show_Bind}};
    $q->{build_data}{show};
}

sub _build_from {
    my $me = shift;
    my $bind = shift;
    if ($$me->{from}) {
        push @$bind, @{$$me->{from}[1 .. $#{$$me->{from}}]} if $#{$$me->{from}} > 0;
        return $$me->{from}[0];
    }
    my $q = $$me->{Parent};
    $q->sql;
    push @$bind, @{$q->{build_data}{From_Bind}};
    $q->{build_data}{from};
}

sub _build_group_order {
    my $me = shift;
    my $bind = shift;
    if ($$me->{group_order}) {
        push @$bind, @{$$me->{group_order}[1 .. $#{$$me->{group_order}}]} if $#{$$me->{group_order}} > 0;
        return $$me->{group_order}[0];
    }
    my $q = $$me->{Parent};
    my $sql = '';
    $sql .= " GROUP BY $q->{build_data}{group}" if $q->{build_data}{group};
    $sql .= " ORDER BY $q->{build_data}{order}" if $q->{build_data}{order};
    push @$bind, @{$q->{build_data}{Group_Bind}}, @{$q->{build_data}{Order_Bind}};
    return $sql;
}

sub _detach {
    my $me = shift;
    if ($$me->{Parent}) {
        $$me->{array} = [ @$me ];
        $$me->{hash} = { %$me };
        unshift @{$$me->{show}}, $me->_build_show($$me->{show});
        unshift @{$$me->{from}}, $me->_build_from($$me->{from});
        unshift @{$$me->{group_order}}, $me->_build_group_order($$me->{group_order});
        $$me->{Tables} = [ @{$$me->{Tables}} ];
        $$me->{Showing} = [ @{$$me->{Showing}} ];
        # TODO: Save configs from Parent
    }
    undef $$me->{Parent}{Row};
    undef $$me->{Parent};
}

=head2 update

  $row->update(id => 123);
  $row->update(name => 'Bob', status => 'Employed');

Updates the current row with the new values specified.
Returns the number of rows updated or '0E0' for no rows to unsure the value is true,
and returns false if there was an error.

Note: If LIMIT is supported on UPDATEs then only the first matching row will be updated
otherwise ALL rows matching the current row will be updated.

=cut

sub update {
    my $me = shift;
    ouch 'No current record to update!' unless $$me->{array};
    my @bind;
    my $sql = 'UPDATE '.$me->_build_from(\@bind);
    $sql .= ' SET '.$me->_build_set(\@bind, @_);
    $sql .= ' WHERE '.$me->_build_where_matching_this_row(\@bind);
    # TODO: Reload/update instead of leaving the row empty?
    # To update the row is difficult because columns may have been aliased
    undef $$me->{array};
    undef %$me;
    $me->do($sql, undef, @bind);
}

=head2 delete

  $row->delete;

Deletes the current row.
Returns the number of rows deleted or '0E0' for no rows to unsure the value is true,
and returns false if there was an error.

Note: If LIMIT is supported on DELETEs then only the first matching row will be deleted
otherwise ALL rows matching the current row will be deleted.

=cut

sub delete {
    my $me = shift;
    ouch 'No current record to delete!' unless $$me->{array};
    my @bind;
    my $sql = 'DELETE FROM '.$me->_build_from(\@bind);
    $sql .= ' WHERE '.$me->_build_where_matching_this_row(\@bind);
    undef $$me->{array};
    undef %$me;
    $me->do($sql, undef, @bind);
}

sub _build_where_matching_this_row {
    my $me = shift;
    my $bind = shift;
    # TODO: Try to use any UNIQUE key, but this will mean storing them in TableInfo
    my @cols;
    for my $tbl (@{$$me->{Tables}}) {
        # Identify the row by the PrimaryKeys if any, otherwise by all Columns
        push @cols, map $tbl ** $_, @{$tbl->{ @{$tbl->{PrimaryKeys}} ? 'PrimaryKeys' : 'Columns' }};
    }
    $me->_build_quick_where($bind, map {$_ => $me->value($_)} @cols);
}

sub DESTROY {
    undef ${$_[0]};
}

1;
