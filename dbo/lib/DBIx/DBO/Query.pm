package DBIx::DBO::Query;
use DBIx::DBO::Common;

use strict;
use warnings;

sub _new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $me = { DBO => shift, sql => undef };
    blessed $me->{DBO} and $me->{DBO}->isa('DBIx::DBO') or ouch 'Invalid DBO Object';
    ouch 'No table specified in new Query' unless @_;
    bless $me, $class;

    for my $table (@_) {
        $me->add_table($table);
    }
    $me->blank;
    return wantarray ? ($me, $me->tables) : $me;
}

sub add_table {
    my ($me, $tbl) = @_;
    $tbl = $me->{DBO}->table($tbl) unless blessed $tbl and $tbl->isa('DBIx::DBO::Table');
    push @{$me->{Tables}}, $tbl;
    push @{$me->{Join}}, ', ';
#    push @{$me->{JoinOn}}, undef;
    return $tbl;
}

sub tables {
    my $me = shift;
    @{$me->{'Tables'}};
}

sub _table_idx {
    my ($me, $tbl) = @_;
    for my $i (0 .. $#{$me->{'Tables'}}) {
        return $i if $tbl == $me->{'Tables'}[$i];
    }
    return undef;
}

sub _table_alias {
    my ($me, $tbl) = @_;
    my $i = $me->_table_idx($tbl);
    ouch 'The table is not in this query' unless defined $i;
    $#{$me->{'Tables'}} > 0 ? 't'.($i + 1) : ();
}

sub blank {
    my $me = shift;
    $me->undo_where;
#    $me->{'IsDistinct'} = 0;
}

sub undo_where {
    my $me = shift;
    # TODO: ...
    # This forces a new search
    undef $me->{sql};
}

sub sth {
    my $me = shift;
    # Ensure the sql is rebuilt if needed
    my $sql = $me->sql;
    $me->{sth} ||= $me->rdbh->prepare($sql);
}

sub sql {
    my $me = shift;
    $me->{sql} ||= $me->_build_sql;
}

sub _build_sql {
    my $me = shift;
    undef $me->{sth};
    my $sql = 'SELECT ';
    $sql .= $me->_build_show;
    $sql .= ' FROM '.$me->_build_from;
    $sql .= ' WHERE '.$_ if $_ = $me->_build_complex_where;
    $sql .= ' ORDER BY '.$_ if $_ = $me->_build_order;
    $sql .= ' LIMIT '.$me->{Limit} if defined $me->{Limit};
    $me->{sql} = $sql;
}

sub _build_show {
    my $me = shift;
    # TODO: Implement
    $me->{show} = '*';
}

sub _build_from {
    my $me = shift;
    $me->{from} = $me->_build_table($me->{Tables}[0]);
    for (my $i = 1; $i < @{$me->{Tables}}; $i++) {
        $me->{from} .= $me->{Join}[$i].$me->_build_table($me->{Tables}[$i]);
        # TODO: JoinOn
    }
    $me->{from};
}

sub _build_table {
    my $me = shift;
    my $t = shift;
    my $alias = $me->_table_alias($t);
    $alias = $alias ? ' AS '.$me->_qi($alias) : '';
    $t->_quoted_name.$alias;
}

sub _build_complex_where {
    my $me = shift;
    my @chunks;
    # TODO: ...
    $me->{where} = join ' AND ', @chunks;
}

sub _build_order {
    my $me = shift;
    # TODO: ...
    $me->{order} = '';
}

sub DESTROY {
    undef %{$_[0]};
}

1;
