use strict;
use warnings;

# Create the DBO (2 tests)
use Test::DBO Sponge => 'Sponge', tests => 16;

# DBO-only Subclass
@Only::DBO::ISA = qw(DBIx::DBO);

ok my $dbo = Only::DBO->connect('DBI:Sponge:'), 'Connect to Sponge' or die $DBI::errstr;
isa_ok $dbo, 'Only::DBO::DBD::Sponge', '$dbo';

my $quoted = $dbo->_qi($Test::DBO::test_db, $Test::DBO::test_tbl);
is $quoted, qq{"$Test::DBO::test_db"."$Test::DBO::test_tbl"}, 'Only::DBO Method _qi';

# Empty Subclasses
@SubClass::Common::ISA = qw(DBIx::DBO::Common);
sub SubClass::Common::_table_class { 'SubClass::Table' }
sub SubClass::Common::_query_class { 'SubClass::Query' }
sub SubClass::Common::_row_class   { 'SubClass::Row' }
@SubClass::ISA = qw(DBIx::DBO SubClass::Common);
@SubClass::Table::ISA = qw(DBIx::DBO::Table SubClass::Common);
@SubClass::Query::ISA = qw(DBIx::DBO::Query SubClass::Common);
@SubClass::Row::ISA = qw(DBIx::DBO::Row SubClass::Common);
BEGIN { # Fake existence of package
    package # Hide from PAUSE
    SubClass::Common::DBD::Sponge;
}

$dbo = SubClass->connect('DBI:Sponge:') or die $DBI::errstr;
isa_ok $dbo, 'SubClass::DBD::Sponge', '$dbo';

is _table_class SubClass, 'SubClass::Table', 'SubClass mro is C3';

isa_ok my $t = $dbo->table($Test::DBO::test_tbl), 'SubClass::Table::DBD::Sponge', '$t';
isa_ok my $q = $dbo->query($t), 'SubClass::Query::DBD::Sponge', '$q';
isa_ok my $r = $q->row, 'SubClass::Row::DBD::Sponge', '$r';

# Empty Table Subclass
@MyTable::ISA = qw(DBIx::DBO::Table);
isa_ok $t = MyTable->new($dbo, $t), 'MyTable', '$t';

# Empty Query Subclass
@MyQuery::ISA = qw(DBIx::DBO::Query);
isa_ok $q = MyQuery->new($dbo, $t), 'MyQuery', '$q';

# Empty Row Subclass
@MyRow::ISA = qw(DBIx::DBO::Row);
isa_ok $r = MyRow->new($dbo, $t), 'MyRow', '$r';

# Create a Query and Row class for a table
{
    package # hide from PAUSE
        My::Query;
    use base 'SubClass::Query';
    my @tables;
    sub new {
        my $me = shift;
        my $dbo = shift;
        @tables = map $dbo->table($_), $Test::DBO::test_tbl unless @tables;
        $me = $me->SUPER::new($dbo, @tables, @_);
        return $me;
    }
    sub _row_class { 'My::Row' }
}
{
    package # hide from PAUSE
        My::Row;
    use base 'SubClass::Row';
    my @tables;
    sub new {
        my $me = shift;
        my $dbo = shift;
        @tables = map $dbo->table($_), $Test::DBO::test_tbl unless @tables;
        $me->SUPER::new($dbo, @tables, @_);
    }
}

my $tbl = My::Query->new($dbo);
isa_ok $tbl, 'My::Query', '$tbl';
is $tbl->_build_from, $tbl->_qi($Test::DBO::test_tbl), 'Subclass represents the table automatically';

my $row = My::Row->new($dbo);
isa_ok $row, 'My::Row', '$row';
is $row->_build_from, $row->_qi($Test::DBO::test_tbl), 'Subclass represents the table automatically';

$row = $tbl->row;
isa_ok $row, 'My::Row', '$tbl->row';

