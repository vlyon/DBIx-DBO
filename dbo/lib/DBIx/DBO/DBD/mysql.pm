use strict;
use warnings;

package # hide from PAUSE
    DBIx::DBO::DBD::mysql;
use DBIx::DBO::Common;

sub config {
    my $class = shift;
    my $val = $class->SUPER::config(@_);
    # MySQL supports LIMIT on UPDATE/DELETE by default
    ($_[0] ne 'LimitRowUpdate' and $_[0] ne 'LimitRowDelete' or defined $val) ? $val : 1;
}

package # hide from PAUSE
    DBIx::DBO::DBD::mysql::Common;
use DBIx::DBO::Common;

sub _build_sql_select {
    my $me = shift;
    my $sql = $me->SUPER::_build_sql_select(@_);
    $sql =~ s/SELECT /SELECT SQL_CALC_FOUND_ROWS / if $me->config('CalcFoundRows');
    return $sql;
}

package # hide from PAUSE
    DBIx::DBO::DBD::mysql::Query;
use DBIx::DBO::Common;

sub found_rows {
    my $me = shift;
    my $sql = $me->sql;
    if (not defined $me->{Found_Rows} and $sql =~ / SQL_CALC_FOUND_ROWS /) {
        $me->run unless $me->sth->{Executed};
        ($me->{Found_Rows}) = $me->rdbh->selectrow_array('SELECT FOUND_ROWS()');
    }
    $me->{Found_Rows};
}

1;
