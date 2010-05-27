use strict;
use warnings;

package # hide from PAUSE
    DBIx::DBO::Common::DBD::Oracle;
use DBIx::DBO::Common;

sub _build_limit {
    '';
}

sub _build_sql_select {
    my $me = shift;
    my $h = shift;
    my $sql = $me->SUPER::_build_sql_select($h);
    return $sql unless defined $h->{LimitOffset};
    return 'SELECT * FROM ('.$sql.') WHERE ROWNUM <= '.$h->{LimitOffset}[0];
}

package # hide from PAUSE
    DBIx::DBO::Query::DBD::Oracle;
use DBIx::DBO::Common;

# Oracle doesn't allow the use of aliases in GROUP BY
sub group_by {
    my $me = shift;
    undef $me->{sql};
    undef $me->{build_data}{group};
    undef @{$me->{build_data}{GroupBy}};
    for my $col (@_) {
        my @group = $me->_parse_col_val($col, Aliases => 0);
        push @{$me->{build_data}{GroupBy}}, \@group;
    }
}

sub limit {
    my $me = shift;
    ouch "The LIMIT clause with an OFFSET is not supported by Oracle" if @_ > 1;
    $me->SUPER::limit(@_);
}

sub found_rows {
    my $me = shift;
    my $sql = $me->sql;
    if (not defined $me->{Found_Rows}) {
        my $limit = $me->{build_data}{LimitOffset};
        undef $me->{build_data}{LimitOffset};
        $me->{Found_Rows} = $me->count_rows;
        $me->{build_data}{LimitOffset} = $limit;
    }
    $me->{Found_Rows};
}

1;
