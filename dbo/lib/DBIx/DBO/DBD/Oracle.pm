use strict;
use warnings;

package # hide from PAUSE
    DBIx::DBO::DBD::Oracle;
use Carp 'croak';

sub _build_limit {
    '';
}

sub _build_sql_select {
    my($class, $me, $h) = @_;
    my $sql = $class->SUPER::_build_sql_select($me, $h);
    return $sql unless defined $h->{LimitOffset};
    return 'SELECT * FROM ('.$sql.') WHERE ROWNUM <= '.$h->{LimitOffset}[0];
}

sub _alias_preference {
    my($class, $me, $method) = @_;
    # Oracle doesn't allow the use of aliases in GROUP BY
    return 0 if $method eq 'join_on' or $method eq 'group_by';
    return 1;
}

# Query
sub _calc_found_rows {
    my($class, $me) = @_;
    local $me->{build_data}{LimitOffset};
    $me->{Found_Rows} = $me->count_rows;
}

1;
