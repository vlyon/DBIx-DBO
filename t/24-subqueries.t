use strict;
use warnings;

use Test::DBO Sponge => 'Sponge', tests => 6;

# Create the DBO
my $dbh = MySponge->connect('DBI:Sponge:') or die $DBI::errstr;
my $dbo = DBIx::DBO->new($dbh);
$dbo->config(QuoteIdentifier => 0);

# Create a few Table objects
my($aa, $bb, $cc, $dd, $ee, $ff) = map {
    local $Test::DBO::test_tbl = $_;
    $dbo->table($_);
} qw(aa bb cc dd ee ff);

# Create a few Query objects to use as subqueries
my $sq_aa = $dbo->query($aa) or die sql_err($dbo);
my $aa_sql = $sq_aa->sql;
my $sq_dd = $dbo->query($dd) or die sql_err($dbo);
my $dd_sql = $sq_dd->sql;

# Create our main Query
my $q = $dbo->query($bb) or die sql_err($dbo);

# SELECT clause subquery
$q->show({VAL => $sq_aa, AS => 'sq_aa'});
is $q->sql, "SELECT ($aa_sql) AS sq_aa FROM bb", 'Add a subquery to the SELECT clause';

$sq_aa->show(\1);
$aa_sql = $sq_aa->sql;
is $q->sql, "SELECT ($aa_sql) AS sq_aa FROM bb", 'Changes to the subquery also change the Query';

$sq_aa->show({VAL => $q, AS => 'q'});
eval { $q->sql };
like $@, qr/^Recursive subquery found /, 'Check for recursion';
$sq_aa->show(\1);

# WHERE clause subquery
$q->where($sq_dd, '=', \7);
is $q->sql, "SELECT ($aa_sql) AS sq_aa FROM bb WHERE ($dd_sql) = 7", 'Add a subquery to the SELECT clasue';

$sq_dd->show(\1);
$dd_sql = $sq_dd->sql;
is $q->sql, "SELECT ($aa_sql) AS sq_aa FROM bb WHERE ($dd_sql) = 7", 'Changes to the subquery also change the Query';

$sq_dd->show({VAL => $q, AS => 'q'});
eval { $q->sql };
like $@, qr/^Recursive subquery found /, 'Check for recursion';
$sq_dd->show(\1);

