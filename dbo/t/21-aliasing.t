use strict;
use warnings;

use Test::DBO Sponge => 'Sponge', tests => 7;

MySponge::db::setup([qw(id alias)], ['vlyon', 22]);

# Create the DBO
my $dbh = MySponge->connect('DBI:Sponge:') or die $DBI::errstr;
my $dbo = DBIx::DBO->new($dbh);
my $t = $dbo->table($Test::DBO::test_tbl) or die sql_err($dbo);
my $q = $dbo->query($t) or die sql_err($t);

$q->show({COL => 'name', AS => 'id'}, {COL => 'age', AS => 'alias'});

isa_ok my $c = $q->column('age'), 'DBIx::DBO::Column', '$c';
isa_ok my $a = $q->column('alias'), 'DBIx::DBO::Column', '$a';

ok $q->{DBO}{dbd_class}->_parse_col($q, 'alias', 2), 'Parse a column via an alias name';
ok $q->{DBO}{dbd_class}->_parse_col($q, $a, 2), 'Parse a column via an alias object';

ok $q->where('alias', '=', 123), 'WHERE clause using an alias name';
ok $q->where($a, '=', 123), 'WHERE clause using an alias object';

my $r = $q->row;
ok $r->load(id => 'vlyon'), 'Load a row via an alias';

# TODO: Aliases in quick_where

