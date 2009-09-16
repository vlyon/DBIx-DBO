package DBIx::DBO::SQLite;
our @ISA = ('DBIx::DBO');
use DBIx::DBO::Common;

use strict;
use warnings;
use DBD::SQLite 1.27;

sub _get_table_schema {
    my $me = shift;
    my $schema = shift;
    my $table = shift;

    my $q_schema = (defined $schema and length $schema) ? $me->_qi($schema).'.' : '';
    my $q_table = $me->_qi($table);
    my $info = $me->rdbh->selectall_arrayref("PRAGMA ${q_schema}table_info($q_table)")
        or ouch "Invalid table: $table";
    return $info->[0][1];
}

1;
