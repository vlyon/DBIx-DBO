package DBIx::DBO::Table;
use DBIx::DBO::Common;

use strict;
use warnings;

use overload '**' => \&column, fallback => 1;

sub dbh { $_[0]{DBO}->dbh }
sub rdbh { $_[0]{DBO}->rdbh }

=head2 config

  $table_setting = $dbo->config($option)
  $dbo->config($option => $table_setting)

Get or set the global or dbo config settings.
When setting an option, the previous value is returned.

=cut

sub config {
    my $me = shift;
    my $opt = shift;
    ouch "Invalid config option '$opt'" unless exists $Config{$opt};
    my $val = $me->{Config}{$opt} // $me->{DBO}->config($opt);
    $me->{Config}{$opt} = shift if @_;
    return $val;
}

sub _new {
    my ($proto, $dbo, $table) = @_;
    my $class = ref($proto) || $proto;
    blessed $dbo and $dbo->isa('DBIx::DBO') or ouch 'Invalid DBO Object';
    (my $schema, $table, $_) = $dbo->table_info($table) or ouch 'No such table: '.$table;
    bless { %$_, Schema => $schema, Name => $table, DBO => $dbo, LastInsertID => undef }, $class;
}

sub _tables {
    $_[0];
}

sub _table_alias {
    return if $_[0] == $_[1];
    ouch 'The table is not in this query';
}

sub _quoted_name {
    my $me = shift;
    $me->{_quoted_name} //= $me->_qi(@$me{qw(Schema Name)});
}

=head2 column

  $t->column($column_name)
  $t ** $column_name

Returns the DBO column object for this column.

=cut

sub column {
    my ($me, $col) = @_;
    ouch 'Invalid column '.$me->_qi($col).' in table '.$me->_quoted_name
        unless exists $me->{Fields}{$col};
    $me->{Column}{$col} //= bless [ $me, $col ], 'DBIx::DBO::Column';
}

=head2 insert

  $t->insert(name => 'Richard', age => 103)

Insert a row into the table.

=cut

sub insert {
    my $me = shift;
    ouch 'insert called without args on table '.$me->_quoted_name unless @_;
    ouch 'Wrong number of arguments' if @_ & 1;
    my @cols;
    my @vals;
    my @bind;
    while (my ($col, $val) = splice @_, 0, 2) {
        push @cols, $me->_build_col($me->_parse_col($col));
        push @vals, $me->_build_val(\@bind, $me->_parse_val($val));
    }
    my $sql = 'INSERT INTO '.$me->_quoted_name.' ('.join(', ', @cols).') VALUES ('.join(', ', @vals).')';
    $me->_sql($sql, @bind);
    my $sth = $me->dbh->prepare($sql) or return undef;
    my $rv = $sth->execute(@bind) or return undef;
    $me->{LastInsertID} = $sth->{mysql_insertid};
    return $rv;
}

sub DESTROY {
    undef %{$_[0]};
}

1;
