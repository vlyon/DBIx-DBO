package DBIx::DBO::DBM;
our @ISA = ('DBIx::DBO');
use DBIx::DBO::Common;

use strict;
use warnings;

sub _like_to_regex {
    my $me = shift;
    my $like = shift;
    my @re = ($like =~ s'^%'' ? '' : '^', $like =~ s'%$'' ? '' : '$');
    $like = quotemeta($like);
    $like =~ s/(\\+[\%_])/length($1) & 1 ? $1 : $2 eq '%' ? '.*' : '.'/eg;
    join '', @re;
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
        $h{Fields}{$col} = ++$i;
    }
    $h{PrimaryKeys} = [];
    $me->{TableInfo}{$schema // ''}{$table} = \%h;
}

1;
