use strict;
use warnings;
use DBD::SQLite '1.26_04';

package DBIx::DBO::DBD::SQLite;
use DBIx::DBO::Common;

sub _get_table_schema {
    my $me = shift;
    my $schema = my $q_schema = shift;
    my $table = my $q_table = shift;
    ouch 'No table name supplied' unless defined $table and length $table;

    $q_schema =~ s/([\\_%])/\\$1/g if defined $q_schema;
    $q_table =~ s/([\\_%])/\\$1/g;

    my $info = $me->rdbh->table_info(undef, $q_schema, $q_table, undef, {Escape => '\\'})->fetchall_arrayref;
    ouch 'Invalid table: '.$me->_qi($table) unless $info and @$info == 1 and $info->[0][2] eq $table;
    return $info->[0][1];
}

1;
