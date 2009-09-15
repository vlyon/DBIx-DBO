package DBIx::DBO::DBM;
use base DBIx::DBO;

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
    my $schema = my $q_schema = shift;
    my $table = my $q_table = shift;
    DBIx::DBO::ouch 'No table name supplied' unless defined $table and length $table;

    $q_schema =~ s/([\\_%])/\\$1/g if defined $q_schema;
    $q_table =~ s/([\\_%])/\\$1/g;

    my $info = $me->rdbh->table_info(undef, $q_schema, $q_table)->fetchall_arrayref;
use Data::Dumper;
warn 'info', substr Dumper($info), 5;
my $dbh = { map {$_ => $me->rdbh->{$_}} keys %{$me->rdbh->{dbm_valid_attrs}} };
warn 'dbh', substr Dumper($dbh), 5;
    DBIx::DBO::ouch 'Invalid table: '.$table
        unless $info and @$info == 1 and $info->[0][2] eq $table;
    return $info->[0][1];
}

1;
