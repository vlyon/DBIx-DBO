package DBIx::DBO::DBD::Pg;
use DBIx::DBO::Common;

use strict;
use warnings;

sub _get_table_schema {
    my $me = shift;
    my $schema = my $q_schema = shift;
    my $table = my $q_table = shift;
    ouch 'No table name supplied' unless defined $table and length $table;

    $q_schema =~ s/([\\_%])/\\$1/g if defined $q_schema;
    $q_table =~ s/([\\_%])/\\$1/g;

    my $info = $me->rdbh->table_info(undef, $q_schema, $q_table)->fetchall_arrayref({});
    ouch 'Invalid table: '.$me->_qi($table) unless $info and @$info == 1 and $info->[0]{pg_table} eq $table;
    return $info->[0]{pg_schema};
}

sub _get_table_info {
    my $me = shift;
    my $schema = my $q_schema = shift;
    my $table = my $q_table = shift;
    ouch 'No table name supplied' unless defined $table and length $table;

    $q_schema =~ s/([\\_%])/\\$1/g if defined $q_schema;
    $q_table =~ s/([\\_%])/\\$1/g;

    my $cols = $me->rdbh->column_info(undef, $q_schema, $q_table, '%')->fetchall_arrayref({});
    ouch 'Invalid table: '.$me->_qi($table) unless @$cols;

    my %h;
    $h{Column_Idx}{$_->{pg_column}} = $_->{ORDINAL_POSITION} for @$cols;
    if (my $keys = $me->rdbh->primary_key_info(undef, $schema, $table)) {
        $h{PrimaryKeys} = [ map $cols->[$_->{KEY_SEQ} - 1]{pg_column}, @{$keys->fetchall_arrayref({})} ];
    } else {
        $h{PrimaryKeys} = [];
    }
    $me->{TableInfo}{$schema // ''}{$table} = \%h;
}

sub table_info {
    my $me = shift;
    my $schema = '';
    my $table = shift;

    if (blessed $table and $table->isa('DBIx::DBO::Table')) {
        ($schema, $table) = @$table{qw(Schema Name)};
        return ($schema, $table, $me->{TableInfo}{$schema}{$table});
    }
    if (ref $table eq 'ARRAY') {
        ($schema, $table) = @$table;
    }
    $schema = $me->_get_table_schema($schema, $table) unless defined $schema and length $schema;

    unless (exists $me->{TableInfo}{$schema}{$table}) {
        $me->_get_table_info($schema, $table);
    }
    return ($schema, $table, $me->{TableInfo}{$schema}{$table});
}

1;
