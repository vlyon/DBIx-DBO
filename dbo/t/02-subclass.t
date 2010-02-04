use strict;
use warnings;

# Create the DBO (3 tests)
use Test::DBO mysql => 'Sponge', tests => 3;

# Empty Subclass
@SubClass::ISA = ('DBIx::DBO');
ok my $dbo = SubClass->connect('DBI:Sponge:'), 'Connect to Sponge' or die $DBI::errstr;
isa_ok $dbo, 'SubClass::DBD::Sponge', '$dbo';

$dbo->connect_readonly('DBI:Sponge:'), 'Connect (read-only) to Sponge' or die $DBI::errstr;

my $test_db = $Test::DBO::prefix.'_db';
my $test_tbl = $Test::DBO::prefix.'_tbl';
my $quoted = $dbo->_qi($test_db, $test_tbl);
is $quoted, qq{"$test_db"."$test_tbl"}, 'Method DBIx::DBO->_qi';

