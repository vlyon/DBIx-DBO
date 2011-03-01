use strict;
use warnings;

use Test::DBO Sponge => 'Sponge', tests => 8;

{
    my $warn = '';
    local $SIG{__WARN__} = sub {
        $warn .= join '', @_;
    };
    DBIx::DBO->import(
        AutoReconnect => 0,
        DebugSQL => 0,
        QuoteIdentifier => 1,
        RowClass => undef,
        StoreRows => 0,
        UseHandle => 0,
        xyz => 123,
    );
    is $warn =~ s/^Unknown import option '(UseHandle|xyz)' at .* line \d+\n//mg, 2, 'DBIx::DBO->import validation';
    is $warn, '', 'DBIx::DBO->import options';
}

eval { DBIx::DBO->new(1, 2, 3, 4) };
like $@, qr/^Too many arguments for /, 'DBIx::DBO->new takes only 3 args';

eval { DBIx::DBO->new(1, 2, \3) };
like $@, qr/^3rd argument to DBIx::DBO::new is not a HASH reference /, 'DBIx::DBO->new 3rd arg must be a HASH';

my $dbh1 = DBI->connect('DBI:Sponge:') or die $DBI::errstr;
my $dbh2 = DBI->connect('DBI:DBM:') or die $DBI::errstr;

eval { DBIx::DBO->new($dbh1, $dbh1, {dbd => 'NoDBD'}) };
is $@, '', 'DBD class is overridable';

eval { DBIx::DBO->new($dbh1, $dbh2, {dbd => 'NoDBD'}) };
like $@, qr/^The read-write and read-only connections must use the same DBI driver /, 'Validate both $dbh drivers';

#my $dbo = DBIx::DBO->new(undef, $dbh2, {dbd => 'NoDBD'}) or die $DBI::errstr;
#eval { $dbo->connect('DBI:Sponge:') };
my $dbo = DBIx::DBO->new($dbh2, undef, {dbd => 'NoDBD'}) or die $DBI::errstr;
eval { $dbo->connect_readonly('DBI:Sponge:') };
like $@, qr/^The read-write and read-only connections must use the same DBI driver /m, 'Check extra connection driver';

$dbo = DBIx::DBO->new($dbh1, $dbh1);
my ($q, $t) = $dbo->query($Test::DBO::test_tbl);

eval { $q->where('id', '=', {FUNC => '(?,?)', VAL => [1,2,3]}) };
like $@, qr/^The number of params \(3\) does not match the number of placeholders \(2\)/,
    'Number of params must equal placeholders';

