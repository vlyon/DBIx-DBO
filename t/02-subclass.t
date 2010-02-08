use strict;
use warnings;

# Create the DBO (2 tests)
use Test::DBO mysql => 'Sponge', tests => 5;

# Empty Subclass
@SubClass::ISA = ('DBIx::DBO');
ok my $dbo = SubClass->connect('DBI:Sponge:'), 'Connect to Sponge' or die $DBI::errstr;
isa_ok $dbo, 'SubClass::DBD::Sponge', '$dbo';

$dbo->connect_readonly('DBI:Sponge:'), 'Connect (read-only) to Sponge' or die $DBI::errstr;

my $test_db = $Test::DBO::prefix.'_db';
my $test_tbl = $Test::DBO::prefix.'_tbl';
my $quoted = $dbo->_qi($test_db, $test_tbl);
is $quoted, qq{"$test_db"."$test_tbl"}, 'SubClass Method _qi';

{
    package # hide from PAUSE
        DBIx::DBO::DBD::Sponge;
    use DBIx::DBO::Common;
    sub _get_table_schema {
        my $me = shift;
        my $schema = shift; # Not used
        my $table = shift;
        ouch 'No table name supplied' unless defined $table and length $table;
        return;
    }
    sub _get_table_info {
        my $me = shift;
        my $schema = shift; # Not used
        my $table = shift;
        ouch 'No table name supplied' unless defined $table and length $table;
        # Fake table info
        return $me->{TableInfo}{''}{$table} = {
            PrimaryKeys => [],
            Columns => [ 'id', 'name', 'age' ],
            Column_Idx => { id => 1, name => 2, age => 3 }
        };
    }
}

isa_ok my $t = $dbo->table($test_tbl), 'SubClass::DBD::Sponge::Table', '$t';
isa_ok my $q = $dbo->query($t), 'SubClass::DBD::Sponge::Query', '$q';

