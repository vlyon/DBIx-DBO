use strict;
use warnings;

package DBIx::DBO::DBD::mysql::Query;
use DBIx::DBO::Common;

sub _build_show {
    my $me = shift;
    my $extra = $me->config('CalcFoundRows') ? 'SQL_CALC_FOUND_ROWS ' : '';
    $extra.$me->SUPER::_build_show;
}

=head2 found_rows

  $query->config(CalcFoundRows => 1);
  my $total_rows = $query->found_rows;

Return the number of rows that would have been returned if there was no limit clause.
Before runnning the query the 'CalcFoundRows' config option must be enabled.

=cut

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
