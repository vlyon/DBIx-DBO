use strict;
use warnings;

# Create the DBO (2 tests)
use Test::DBO Sponge => 'Sponge', tests => 11;

# Empty Subclass
@SubClass::ISA = ('DBIx::DBO');
ok my $dbo = SubClass->connect('DBI:Sponge:'), 'Connect to Sponge' or die $DBI::errstr;
isa_ok $dbo, 'SubClass::DBD::Sponge', '$dbo';

$dbo->connect_readonly('DBI:Sponge:'), 'Connect (read-only) to Sponge' or die $DBI::errstr;

my $quoted = $dbo->_qi($Test::DBO::test_db, $Test::DBO::test_tbl);
is $quoted, qq{"$Test::DBO::test_db"."$Test::DBO::test_tbl"}, 'SubClass Method _qi';

note 'C3 Method Resolution Order is needed for optimal inheritance when subcalssing!' unless $DBIx::DBO::use_c3_mro;
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
    # Hack for machines not using MRO::Compat
    unless ($DBIx::DBO::use_c3_mro) {
        *SubClass::DBD::Sponge::_get_table_schema = \&_get_table_schema;
        *SubClass::DBD::Sponge::_get_table_info = \&_get_table_info;
    }
}

isa_ok my $t = $dbo->table($Test::DBO::test_tbl), 'SubClass::Table::DBD::Sponge', '$t';
isa_ok my $q = $dbo->query($t), 'SubClass::Query::DBD::Sponge', '$q';

# Empty Table Subclass
@MyTable::ISA = ('DBIx::DBO::Table');
isa_ok $t = MyTable->new($dbo, $t), 'MyTable::DBD::Sponge', '$t';
isa_ok $t, 'MyTable', '$t';
isa_ok $t, 'DBIx::DBO::Table::DBD::Sponge', '$t';

# Empty Query Subclass
@MyQuery::ISA = ('DBIx::DBO::Query');
isa_ok $q = MyQuery->new($dbo, $t), 'MyQuery::DBD::Sponge', '$q';
isa_ok $q, 'MyQuery', '$q';
isa_ok $q, 'DBIx::DBO::Query::DBD::Sponge', '$q';

