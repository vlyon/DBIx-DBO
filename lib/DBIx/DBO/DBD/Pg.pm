use strict;
use warnings;

package # hide from PAUSE
    DBIx::DBO::DBD::Pg;
use DBIx::DBO::Common;

sub _get_table_schema {
    my $me = shift;
    my $schema = my $q_schema = shift;
    my $table = my $q_table = shift;
    ouch 'No table name supplied' unless defined $table and length $table;

    $q_schema =~ s/([\\_%])/\\$1/g if defined $q_schema;
    $q_table =~ s/([\\_%])/\\$1/g;

    # First try just these types
    my $info = $me->rdbh->table_info(undef, $q_schema, $q_table,
        'TABLE,VIEW,GLOBAL TEMPORARY,LOCAL TEMPORARY,SYSTEM TABLE')->fetchall_arrayref({});
    # Then if we found nothing, try any type
    $info = $me->rdbh->table_info(undef, $q_schema, $q_table)->fetchall_arrayref if $info and @$info == 0;
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
    $h{Columns} = [ sort { $h{Column_Idx}{$a} cmp $h{Column_Idx}{$b} } keys %{$h{Column_Idx}} ];

    $h{PrimaryKeys} = [];
    $me->_set_table_key_info($schema, $table, \%h);

    $me->{TableInfo}{defined $schema ? $schema : ''}{$table} = \%h;
}

sub _set_table_key_info {
    my $me = shift;
    my $schema = shift;
    my $table = shift;
    my $h = shift;
    if (my $keys = $me->rdbh->primary_key_info(undef, $schema, $table)) {
        # In Pg the KEY_SEQ is actually the column index! Rows returned are in key seq order
        # And the column names are quoted so we use the pg_column names instead
        $h->{PrimaryKeys} = [ map $h->{Columns}[$_->{KEY_SEQ} - 1], @{$keys->fetchall_arrayref({})} ];
    }
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
