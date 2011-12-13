use strict;
use warnings;
use DBD::SQLite 1.27;

package # hide from PAUSE
    DBIx::DBO::DBD::SQLite;
use Carp 'croak';

sub _get_table_schema {
    my $me = shift;
    my $schema = my $q_schema = shift;
    my $table = my $q_table = shift;

    $q_schema =~ s/([\\_%])/\\$1/g if defined $q_schema;
    $q_table =~ s/([\\_%])/\\$1/g;

    # Try just these types
    my $info = $me->rdbh->table_info(undef, $q_schema, $q_table,
        'TABLE,VIEW,GLOBAL TEMPORARY,LOCAL TEMPORARY,SYSTEM TABLE', {Escape => '\\'})->fetchall_arrayref;
    croak 'Invalid table: '.$me->_qi($table) unless $info and @$info == 1 and $info->[0][2] eq $table;
    return $info->[0][1];
}

# Hack to fix quoted primary keys
if ($DBD::SQLite::VERSION < 1.30) {
    *_set_table_key_info = sub {
        my($me, $schema, $table, $h) = @_;
        $me->SUPER::_set_table_key_info($schema, $table, $h);
        s/^(["'`])(.+)\1$/$2/ for @{$h->{PrimaryKeys}}; # dequote
    }
}

package # hide from PAUSE
    DBIx::DBO::Query::DBD::SQLite;

sub fetch {
    my $me = $_[0];
    my $row = $me->SUPER::fetch;
    unless (defined $row or $me->{sth}->err) {
        $me->{Row_Count} = $me->{sth}->rows;
    }
    return $row;
}

sub rows {
    my $me = $_[0];
    $me->sql; # Ensure the Row_Count is cleared if needed
    defined $me->{Row_Count} ? $me->{Row_Count} : -1;
}

package # hide from PAUSE
    DBIx::DBO::Table::DBD::SQLite;

sub _save_last_insert_id {
    my($me, $sth) = @_;
    $sth->{Database}->last_insert_id(undef, @$me{qw(Schema Name)}, undef);
}

1;
