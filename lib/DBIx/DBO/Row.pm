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
        ouch 'Invalid Parent Object' unless blessed $$me->{Parent};
        if ($$me->{Parent}->isa('DBIx::DBO::Query')) {
            $$me->{Tables} = $$me->{Parent}{Tables};
            $$me->{Showing} = $$me->{Parent}{Showing};
            # We must weaken this to avoid a circular reference
            weaken $$me->{Parent};
        } elsif ($$me->{Parent}->isa('DBIx::DBO::Table')) {
            $$me->{show_from} = [ 'SELECT * FROM '.$$me->{Parent}->_quoted_name ];
            $$me->{group_order} = [ '' ];
            $$me->{Tables} = [ delete $$me->{Parent} ];
            $$me->{Showing} = [];
        } else {
            ouch 'Invalid Parent Object';
        }
    }
    bless $me, $class;
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
    return undef;
}

sub _table_alias {
    my ($me, $tbl) = @_;
    my $i = $me->_table_idx($tbl);
    ouch 'The table is not in this query' unless defined $i;
    $#{$me->{Tables}} > 0 ? 't'.($i + 1) : ();
}

sub _column_idx {
    my $me = shift;
    my $col = shift;
    my $idx = -1;
    for my $shown (@{$$me->{ @{$$me->{Showing}} ? 'Showing' : 'Tables' }}) {
        if (blessed $shown and $shown->isa('DBIx::DBO::Table')) {
            return $idx + $shown->{Column_Idx}{$col->[1]} if exists $shown->{Column_Idx}{$col->[1]};
            $idx += keys %{$shown->{Column_Idx}};
            next;
        }
        $idx++;
        return $idx if not defined $shown->[1] and @{$shown->[0]} == 1 and $col == $shown->[0][0];
    }
    return undef;
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

=cut

sub load {
    my $me = shift;
    my @bind;
    my $sql = $me->_build_show_from(\@bind);
    $sql .= $me->_build_where(\@bind, @_);
    # TODO: GroupBy, OrderBy & Limit 1
    $sql .= $me->_build_group_order(\@bind);
    $sql .= $me->_build_sql_suffix(\@bind);
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

sub _build_show_from {
    my $me = shift;
    my $bind = shift;
    if ($$me->{show_from}) {
        push @$bind, @{$$me->{show_from}[1 .. $#{$$me->{show_from}}]};
        return $$me->{show_from}[0];
    }
    my $q = $$me->{Parent};
    $q->sql;
    push @$bind, @{$q->{Show_Bind}}, @{$q->{From_Bind}};
    return "SELECT $q->{show} FROM $q->{from}";
}

sub _build_group_order {
    my $me = shift;
    my $bind = shift;
    if ($$me->{group_order}) {
        push @$bind, @{$$me->{group_order}[1 .. $#{$$me->{group_order}}]};
        return $$me->{group_order}[0];
    }
    my $q = $$me->{Parent};
    my $sql = '';
    $sql .= " GROUP BY $q->{order}" if $q->{group};
    $sql .= " ORDER BY $q->{order}" if $q->{order};
    push @$bind, @{$q->{Group_Bind}}, @{$q->{Order_Bind}};
    return $sql;
}

sub _build_sql_suffix {
    my $me = shift;
    ' LIMIT 1';
}

sub _detach {
    my $me = shift;
    if ($$me->{Parent}) {
        $$me->{array} = [ @$me ];
        $$me->{hash} = { %$me };
        unshift @{$$me->{show_from}}, $me->_build_show_from($$me->{show_from});
        unshift @{$$me->{group_order}}, $me->_build_group_order($$me->{group_order});
        $$me->{Tables} = [ @{$$me->{Tables}} ];
        $$me->{Showing} = [ @{$$me->{Showing}} ];
        # TODO: Save configs from Parent
    }
    undef $$me->{Parent}{Row};
    undef $$me->{Parent};
}

sub DESTROY {
    undef ${$_[0]};
}

1;
