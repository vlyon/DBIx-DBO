package DBIx::DBO::DBD::mysql::Query;
use DBIx::DBO::Common;

use strict;
use warnings;

sub _build_show {
    my $me = shift;
    my $extra = $me->config('CalcFoundRows') ? 'SQL_CALC_FOUND_ROWS ' : '';
    $extra.$me->SUPER::_build_show;
}

sub found_rows {
    my $me = shift;
    my $sql = $me->sql;
    if (not defined $me->{Found_Rows} and $sql =~ / SQL_CALC_FOUND_ROWS /) {
        $me->run unless $me->sth->{Executed};
        $me->{Found_Rows} = ( $me->rdbh->selectrow_array('SELECT FOUND_ROWS()') )[0];
    }
    $me->{Found_Rows};
}

1;
