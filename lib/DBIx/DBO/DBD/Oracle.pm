use strict;
use warnings;

package # hide from PAUSE
    DBIx::DBO::Common::DBD::Oracle;

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

# Oracle doesn't allow the use of aliases in GROUP BY
sub _alias_preference {
    my $me = shift;
    my $method = shift;
    return 0 if $method eq 'join_on' or $method eq 'group_by';
    return 1;
}

package # hide from PAUSE
    DBIx::DBO::Query::DBD::Oracle;
use Carp 'croak';

sub limit {
    my $me = shift;
    croak "The LIMIT clause with an OFFSET is not supported by Oracle" if @_ > 1;
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
