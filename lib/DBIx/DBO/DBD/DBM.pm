package DBIx::DBO::DBD::DBM;
use DBIx::DBO::Common;

use strict;
use warnings;

sub _bless_dbo {
    my $class = shift;
    my $me = $class->SUPER::_bless_dbo(@_);
    # DBM does not supported QuoteIdentifier correctly!
    $me->config(QuoteIdentifier => 0);
    return $me;
}

sub _get_table_schema {
    my $me = shift;
    my $schema = shift; # Not used
    my $table = my $q_table = shift;
    ouch 'No table name supplied' unless defined $table and length $table;

    return;
}

sub _get_table_info {
    my $me = shift;
    my $schema = shift; # Not used
    my $table = my $q_table = shift;
    ouch 'No table name supplied' unless defined $table and length $table;

    unless (exists $me->rdbh->{dbm_tables}{$q_table}
            and exists $me->rdbh->{dbm_tables}{$q_table}{cols}
            and ref $me->rdbh->{dbm_tables}{$q_table}{cols} eq 'ARRAY') {
        $q_table = $me->_qi($table); # Try with the qouted table name
        unless (exists $me->rdbh->{dbm_tables}{$q_table}
                and exists $me->rdbh->{dbm_tables}{$q_table}{cols}
                and ref $me->rdbh->{dbm_tables}{$q_table}{cols} eq 'ARRAY') {
            ouch 'Invalid table: '.$q_table;
        }
    }

    my %h;
    my $i;
    for my $col (@{$me->rdbh->{dbm_tables}{$q_table}{cols}}) {
        $h{Column_Idx}{$col} = ++$i;
    }
    $h{PrimaryKeys} = [];
    $me->{TableInfo}{$schema // ''}{$table} = \%h;
}

1;
