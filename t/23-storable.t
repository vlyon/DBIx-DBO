use strict;
use warnings;

use Storable;
use Test::DBO Sponge => 'Sponge', tests => 27;
note 'Testing with: CacheQuery => '.DBIx::DBO->config('CacheQuery');

MySponge::db::setup([qw(id name age)], [1, 'one', 1], [7, 'test', 123], [3, 'three', 333], [999, 'end', 0]);

# Create the DBO
my $dbh = MySponge->connect('DBI:Sponge:') or die $DBI::errstr;
my $dbo = DBIx::DBO->new($dbh);

ok my $frozen = Storable::freeze($dbo), 'Freeze DBO';
isa_ok my $thawed = Storable::thaw($frozen), 'DBIx::DBO', 'Thawed';
@$thawed{qw(dbh rdbh)} = @$dbo{qw(dbh rdbh)};
is_deeply $thawed, $dbo, 'Same DBO';

my $t = $dbo->table($Test::DBO::test_tbl) or die sql_err($dbo);

ok $frozen = Storable::freeze($t), 'Freeze Table';
isa_ok $thawed = Storable::thaw($frozen), 'DBIx::DBO::Table', 'Thawed';
@{$thawed->{DBO}}{qw(dbh rdbh)} = @$dbo{qw(dbh rdbh)};
is_deeply $thawed, $t, 'Same Table';

my $q = $dbo->query($t) or die sql_err($dbo);
$q->show($t ** 'id', $t);

ok $frozen = Storable::freeze($q), 'Freeze Query';
isa_ok $thawed = Storable::thaw($frozen), 'DBIx::DBO::Query', 'Thawed';
@{$thawed->{DBO}}{qw(dbh rdbh)} = @$dbo{qw(dbh rdbh)};
is_deeply $thawed, $q, 'Same Query';

my $r = $t->fetch_row(id => 7) or die sql_err($t);

ok $frozen = Storable::freeze($r), 'Freeze Row';
isa_ok $thawed = Storable::thaw($frozen), 'DBIx::DBO::Row', 'Thawed';
@{$$thawed->{DBO}}{qw(dbh rdbh)} = @$dbo{qw(dbh rdbh)};
is_deeply $thawed, $r, 'Same Row';

$q->run;

ok $frozen = Storable::freeze($q), 'Freeze Query (after run)';
isa_ok $thawed = Storable::thaw($frozen), 'DBIx::DBO::Query', 'Thawed';
@{$thawed->{DBO}}{qw(dbh rdbh)} = @$dbo{qw(dbh rdbh)};
{ # Reset the active query
    local(@$q{qw(sth Active)});
    is_deeply $thawed, $q, 'Same Query';
}

$r = $q->fetch or die sql_err($q);

ok $frozen = Storable::freeze($r), 'Freeze Row (after fetch)';
isa_ok $thawed = Storable::thaw($frozen), 'DBIx::DBO::Row', 'Thawed';
@{$$thawed->{DBO}}{qw(dbh rdbh)} = @$dbo{qw(dbh rdbh)};
{ # Detach Parent
    local $$r->{Parent};
    is_deeply $thawed, $r, 'Same Row';
}

$q->fetch or die sql_err($q);

ok $frozen = Storable::freeze($r), 'Freeze Row (after fetch & detach)';
isa_ok $thawed = Storable::thaw($frozen), 'DBIx::DBO::Row', 'Thawed';
@{$$thawed->{DBO}}{qw(dbh rdbh)} = @$dbo{qw(dbh rdbh)};
is_deeply $thawed, $r, 'Same Row';

ok $frozen = Storable::freeze($q), 'Freeze Query (after fetch)';
isa_ok $thawed = Storable::thaw($frozen), 'DBIx::DBO::Query', 'Thawed';
@{$thawed->{DBO}}{qw(dbh rdbh)} = @$dbo{qw(dbh rdbh)};
{ # Reset the active query
    local(@$q{qw(sth Active)});
    local $q->{cache}{idx} = 0 if exists $q->{cache};
    is_deeply $thawed, $q, 'Same Query';
    is_deeply $thawed->row, $q->row, 'Same Row from $q->row';
    if ($thawed->config('CacheQuery')) {
        is_deeply scalar $thawed->fetch, scalar $q->fetch, 'Same Row from $q->fetch';
    } else {
        is_deeply \@{$thawed->fetch}, [999,'end',0], 'Same Row from $q->fetch';
    }
}
is ${$thawed->{Row}}->{Columns}, $thawed->{Columns}, 'Row has not detached';

