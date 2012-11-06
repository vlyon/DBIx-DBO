use strict;
use warnings;
use DBD::SQLite 1.27;

package # hide from PAUSE
    DBIx::DBO::DBD::SQLite;
use Carp 'croak';

sub _get_table_schema {
    my($class, $me, $schema, $table) = @_;

    my $q_schema = $schema;
    my $q_table = $table;
    $q_schema =~ s/([\\_%])/\\$1/g if defined $q_schema;
    $q_table =~ s/([\\_%])/\\$1/g;

    # Try just these types
    my $info = $me->rdbh->table_info(undef, $q_schema, $q_table,
        'TABLE,VIEW,GLOBAL TEMPORARY,LOCAL TEMPORARY,SYSTEM TABLE', {Escape => '\\'})->fetchall_arrayref;
    croak 'Invalid table: '.$class->_qi($me, $table) unless $info and @$info == 1 and $info->[0][2] eq $table;
    return $info->[0][1];
}

# Hack to fix quoted primary keys
if (eval "$DBD::SQLite::VERSION < 1.30") {
    *_set_table_key_info = sub {
        my($class, $me, $schema, $table, $h) = @_;
        $class->SUPER::_set_table_key_info($me, $schema, $table, $h);
        s/^(["'`])(.+)\1$/$2/ for @{$h->{PrimaryKeys}}; # dequote
    };
}

sub _save_last_insert_id {
    my($class, $me, $sth) = @_;
    $sth->{Database}->last_insert_id(undef, @$me{qw(Schema Name)}, undef);
}

1;
