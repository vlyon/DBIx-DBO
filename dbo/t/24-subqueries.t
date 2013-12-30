use strict;
use warnings;

use Test::DBO Sponge => 'Sponge', tests => 13;

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
my $sq_cc = $dbo->query($cc) or die sql_err($dbo);
my $cc_sql = $sq_cc->sql;
my $sq_dd = $dbo->query($dd) or die sql_err($dbo);
my $dd_sql = $sq_dd->sql;
my $sq_ee = $dbo->query($ee) or die sql_err($dbo);
my $ee_sql = $sq_ee->sql;
my $sq_ff = $dbo->query($ff) or die sql_err($dbo);
my $ff_sql = $sq_ff->sql;

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
$q->where($sq_ff, '=', \7);
is $q->sql, "SELECT ($aa_sql) AS sq_aa FROM bb WHERE ($ff_sql) = 7", 'Add a subquery to the WHERE clause';

$sq_ff->show(\1);
$ff_sql = $sq_ff->sql;
is $q->sql, "SELECT ($aa_sql) AS sq_aa FROM bb WHERE ($ff_sql) = 7", 'Changes to the subquery also change the Query';

$sq_ff->show({VAL => $q, AS => 'q'});
eval { $q->sql };
like $@, qr/^Recursive subquery found /, 'Check for recursion';
$sq_ff->show(\1);

# JOIN clause subquery
$q->join_table($sq_cc, 'JOIN');
is $q->sql, "SELECT ($aa_sql) AS sq_aa FROM bb t1 JOIN ($cc_sql) t2 WHERE ($ff_sql) = 7", 'Add a subquery to the JOIN clause';

$sq_cc->show(\1);
$cc_sql = $sq_cc->sql;
is $q->sql, "SELECT ($aa_sql) AS sq_aa FROM bb t1 JOIN ($cc_sql) t2 WHERE ($ff_sql) = 7", 'Changes to the subquery also change the Query';

$sq_cc->show({VAL => $q, AS => 'q'});
eval { $q->sql };
like $@, qr/^Recursive subquery found /, 'Check for recursion';
$sq_cc->show(\1);

# JOIN ON clause subquery
$q->join_on($sq_cc, $sq_ee, '=', \3);
is $q->sql, "SELECT ($aa_sql) AS sq_aa FROM bb t1 JOIN ($cc_sql) t2 ON ($ee_sql) = 3 WHERE ($ff_sql) = 7", 'Add a subquery to the JOIN ON clause';

$sq_ee->show(\1);
$ee_sql = $sq_ee->sql;
is $q->sql, "SELECT ($aa_sql) AS sq_aa FROM bb t1 JOIN ($cc_sql) t2 ON ($ee_sql) = 3 WHERE ($ff_sql) = 7", 'Changes to the subquery also change the Query';

$sq_ee->show({VAL => $q, AS => 'q'});
eval { $q->sql };
like $@, qr/^Recursive subquery found /, 'Check for recursion';
$sq_ee->show(\1);

# Add a join to a subquery
$sq_cc->join_table($dd);
is $q->sql, "SELECT ($aa_sql) AS sq_aa FROM bb t1 JOIN ($cc_sql t3, dd t4) t2 ON ($ee_sql) = 3 WHERE ($ff_sql) = 7", 'Add a join to the subquery';

