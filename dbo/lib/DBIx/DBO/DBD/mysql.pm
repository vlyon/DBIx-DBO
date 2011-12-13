use strict;
use warnings;

package # hide from PAUSE
    DBIx::DBO::DBD::mysql;
use Carp qw(croak);

sub _get_table_schema {
    my $me = shift;
    my $schema = shift;
    my $table = shift;
    ($schema) = $me->rdbh->selectrow_array('SELECT DATABASE()') unless defined $schema and length $schema;
    $me->SUPER::_get_table_schema($schema, $table);
}

sub _set_table_key_info {
    my $me = shift;
    my $schema = shift;
    my $table = shift;
    my $h = shift;
    if (my $keys = $me->rdbh->primary_key_info(undef, $schema, $table)) {
        $h->{PrimaryKeys}[$_->{KEY_SEQ} - 1] = $_->{COLUMN_NAME} for @{$keys->fetchall_arrayref({})};
    } else {
        # Support for older DBD::mysql - Simulate primary_key_info()
        local $me->rdbh->{FetchHashKeyName} = 'NAME_lc';
        my $info = $me->rdbh->selectall_arrayref('SHOW KEYS FROM '.$me->_qi($schema, $table), {Columns => {}});
        $_->{key_name} eq 'PRIMARY' and $h->{PrimaryKeys}[$_->{seq_in_index} - 1] = $_->{column_name} for @$info;
    }
}

sub _unquote_table {
    my $me = shift;
    $_[0] =~ /^(?:(`|"|)(.+)\1\.|)(`|"|)(.+)\3$/ or croak "Invalid table: \"$_[0]\"";
    return ($4, $2);
}

sub config {
    my $class = shift;
    my $val = $class->SUPER::config(@_);
    # MySQL supports LIMIT on UPDATE/DELETE by default
    ($_[0] ne 'LimitRowUpdate' and $_[0] ne 'LimitRowDelete' or defined $val) ? $val : 1;
}

package # hide from PAUSE
    DBIx::DBO::Common::DBD::mysql;

sub _build_sql_select {
    my $me = shift;
    my $sql = $me->SUPER::_build_sql_select(@_);
    $sql =~ s/SELECT /SELECT SQL_CALC_FOUND_ROWS / if $me->config('CalcFoundRows');
    return $sql;
}

# MySQL doesn't allow the use of aliases in the WHERE clause
sub _alias_preference {
    my $me = shift;
    my $method = shift || ((caller(2))[3] =~ /\b(\w+)$/);
    return 0 if $method eq 'join_on' or $method eq 'where';
    return 1;
}

package # hide from PAUSE
    DBIx::DBO::Table::DBD::mysql;
use Carp 'croak';

sub _save_last_insert_id {
    my $me = shift;
    my $sth = shift;
    return $sth->{mysql_insertid};
}

sub _do_bulk_insert {
    shift->_fast_bulk_insert(@_);
}

package # hide from PAUSE
    DBIx::DBO::Query::DBD::mysql;

sub found_rows {
    my $me = shift;
    my $sql = $me->sql;
    if (not defined $me->{Found_Rows} and $sql =~ / SQL_CALC_FOUND_ROWS /) {
        $me->run unless $me->_sth->{Executed};
        ($me->{Found_Rows}) = $me->rdbh->selectrow_array('SELECT FOUND_ROWS()');
    }
    $me->{Found_Rows};
}

1;
