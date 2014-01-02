use strict;
use warnings;

use Test::DBO Sponge => 'Sponge', tests => 15;

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
my $aa_sql;
my $sq_cc = $dbo->query($cc) or die sql_err($dbo);
my $cc_sql;
my $sq_dd = $dbo->query($dd) or die sql_err($dbo);
my $dd_sql;
my $sq_ee = $dbo->query($ee) or die sql_err($dbo);
my $ee_sql;
my $sq_ff = $dbo->query($ff) or die sql_err($dbo);
my $ff_sql;

# Create our main Query
my $q = $dbo->query($bb) or die sql_err($dbo);

# SELECT clause subquery
$q->show({VAL => $sq_aa, AS => 'sq_aa'});
$aa_sql = '(SELECT * FROM aa) AS sq_aa';
is $q->sql, "SELECT $aa_sql FROM bb", 'Add a subquery to the SELECT clause';

$sq_aa->show(\1);
$aa_sql = '(SELECT 1 FROM aa) AS sq_aa';
is $q->sql, "SELECT $aa_sql FROM bb", 'Changes to the subquery also change the Query';

$sq_aa->show({VAL => $q, AS => 'q'});
eval { $q->sql };
like $@, qr/^Recursive subquery found /, 'Check for recursion';
$sq_aa->show(\1);

# WHERE clause subquery
$q->where($sq_ff, '=', \7);
$ff_sql = '(SELECT * FROM ff)';
is $q->sql, "SELECT $aa_sql FROM bb WHERE $ff_sql = 7", 'Add a subquery to the WHERE clause';

$sq_ff->show(\1);
$ff_sql = '(SELECT 1 FROM ff)';
is $q->sql, "SELECT $aa_sql FROM bb WHERE $ff_sql = 7", 'Changes to the subquery also change the Query';

$sq_ff->where($q, '=', \7);
eval { $q->sql };
like $@, qr/^Recursive subquery found /, 'Check for recursion';
$sq_ff->unwhere($q);

# JOIN clause subquery
$q->join_table($sq_cc, 'JOIN');
$cc_sql = '(SELECT * FROM cc) t2';
is $q->sql, "SELECT $aa_sql FROM bb t1 JOIN $cc_sql WHERE $ff_sql = 7", 'Add a subquery to the JOIN clause';

$sq_cc->show(\1);
$cc_sql = '(SELECT 1 FROM cc) t2';
is $q->sql, "SELECT $aa_sql FROM bb t1 JOIN $cc_sql WHERE $ff_sql = 7", 'Changes to the subquery also change the Query';

$sq_cc->join_table($q);
eval { $q->sql };
like $@, qr/^Recursive subquery found /, 'Check for recursion';
pop @{$sq_cc->{Tables}};
pop @{$sq_cc->{build_data}{Join}};
pop @{$sq_cc->{build_data}{Join_On}};
pop @{$sq_cc->{Join_Bracket_Refs}};
pop @{$sq_cc->{Join_Brackets}};

# JOIN ON clause subquery
$q->join_on($sq_cc, $sq_ee, '=', \3);
$ee_sql = '(SELECT * FROM ee)';
is $q->sql, "SELECT $aa_sql FROM bb t1 JOIN $cc_sql ON $ee_sql = 3 WHERE $ff_sql = 7", 'Add a subquery to the JOIN ON clause';

$sq_ee->show(\1);
$ee_sql = '(SELECT 1 FROM ee)';
is $q->sql, "SELECT $aa_sql FROM bb t1 JOIN $cc_sql ON $ee_sql = 3 WHERE $ff_sql = 7", 'Changes to the subquery also change the Query';

$q->join_on($sq_cc, $q, '=', \3);
eval { $q->sql };
like $@, qr/^Recursive subquery found /, 'Check for recursion';
pop @{$q->{build_data}{Join_On}[1]};

# Add a join to a subquery
$sq_cc->join_table($dd);
$sq_cc->show();
$dd_sql = '(SELECT * FROM cc t3, dd t4) t2';
is $q->sql, "SELECT $aa_sql FROM bb t1 JOIN $dd_sql ON $ee_sql = 3 WHERE $ff_sql = 7", 'Add a join to the subquery';

# Refer to a subquery in show()
$q->show({VAL => $sq_aa, AS => 'sq_aa'}, $sq_cc);
is $q->sql, "SELECT $aa_sql, t2.* FROM bb t1 JOIN $dd_sql ON $ee_sql = 3 WHERE $ff_sql = 7", 'Refer to a subquery in DBIx::DBO->show';

is_deeply [$q->columns], [qw(sq_aa id name age id name age)], 'Columns discovered correctly from subqueries';

