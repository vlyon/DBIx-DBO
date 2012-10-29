use strict;
use warnings;

use Test::DBO ExampleP => 'ExampleP';
use Test::DBO Sponge => 'Sponge', tests => 17;

{
    my $warn = '';
    local $SIG{__WARN__} = sub {
        $warn .= join '', @_;
    };
    local $Carp::Verbose = 0;
    DBIx::DBO->import(
        AutoReconnect => 0,
        DebugSQL => 0,
        QuoteIdentifier => 1,
        CacheQuery => 0,
        UseHandle => 0,
        'NoValue'
    );
    is $warn =~ s/^(Import option 'NoValue' passed without a value|Unknown import option 'UseHandle') at .* line \d+\.?\n//mg, 2, 'DBIx::DBO->import validation';
    is $warn, '', 'DBIx::DBO->import options';
}

eval { DBIx::DBO->new(1, 2, 3, 4) };
like $@, qr/^Too many arguments for /, 'DBIx::DBO->new takes only 3 args';

eval { DBIx::DBO->new(1, 2, \3) };
like $@, qr/^3rd argument to DBIx::DBO::new is not a HASH reference /, 'DBIx::DBO->new 3rd arg must be a HASH';

my $dbh1 = DBI->connect('DBI:Sponge:') or die $DBI::errstr;
my $dbh2 = DBI->connect('DBI:ExampleP:') or die $DBI::errstr;

eval { DBIx::DBO->new($dbh1, $dbh1, {dbd => 'NoDBD'}) };
is $@, '', 'DBD class is overridable';

eval { DBIx::DBO->new($dbh1, $dbh2, {dbd => 'NoDBD'}) };
like $@, qr/^The read-write and read-only connections must use the same DBI driver /, 'Validate both $dbh drivers';

my $dbo = DBIx::DBO->new($dbh2, undef, {dbd => 'NoDBD'}) or die $DBI::errstr;
eval { $dbo->connect_readonly('DBI:Sponge:') };
like $@, qr/^The read-write and read-only connections must use the same DBI driver /m, 'Check extra connection driver';

eval { $dbo->connect('DBI:Sponge:') };
like $@, qr/^DBO is already connected/, 'DBO is already connected';

$dbo = DBIx::DBO->connect_readonly('DBI:Sponge:');
$dbo->connect_readonly('DBI:Sponge:');
my($q, $t) = $dbo->query($Test::DBO::test_tbl);
my $t2 = $dbo->table($Test::DBO::test_tbl);

eval { $t->new('no_dbo_object') };
like $@, qr/^Invalid DBO Object/, 'Requires DBO object';

eval { $dbo->table('no_such_table') };
like $@, qr/^Invalid table: "no_such_table"/, 'Ensure table exists';

eval { $q->where('id', '=', {FUNC => '(?,?)', VAL => [1,2,3]}) };
like $@, qr/^The number of params \(3\) does not match the number of placeholders \(2\)/,
    'Number of params must equal placeholders';

eval { $t->column('WrongColumn') };
like $@, qr/^Invalid column "WrongColumn" in table/, 'Invalid column';

eval { $t->delete($t2 ** 'name' => undef) };
like $@, qr/^Invalid column, the column is from a table not included in this query/, 'Invalid column (another table)';

eval { $t->delete(name => [qw(doesnt exist)]) };
like $@, qr/^No read-write handle connected/, 'No read-write handle connected';

$dbo->disconnect;
eval { $dbo->connect_readonly };
like $@, qr/^Can't connect to data source ''/, 'AutoReconnect is off by default';

$dbo->connect_readonly('DBI:Sponge:');
eval { $dbo->connect_readonly('') };
like $@, qr/^Can't connect to data source ''/, 'Empty DSN';

eval { $dbo->connect_readonly('DBI:ExampleP:') };
like $@, qr/The read-write and read-only connections must use the same DBI driver/, 'Check driver on extra connect';

