use strict;
use warnings;

BEGIN { die "DBM is not yet supported!\n" unless $ENV{DBO_ALLOW_DBM} }
use SQL::Statement;

package # hide from PAUSE
    DBIx::DBO::DBD::DBM;
use Carp 'croak';

sub _init {
    my $class = shift;
    my $me = $class->SUPER::_init(@_);
    # DBM does not support QuoteIdentifier correctly!
    $me->config(QuoteIdentifier => 0);
    return $me;
}

sub _get_table_schema {
    my $me = shift;
    my $schema = shift; # Not used
    my $table = shift;
    return;
}

sub _get_table_info {
    my $me = shift;
    my $schema = shift; # Not used
    my $table = my $q_table = shift;

    unless (exists $me->rdbh->{dbm_tables}{$q_table}) {
        $q_table = $me->_qi($table); # Try with the quoted table name
        unless (exists $me->rdbh->{dbm_tables}{$q_table}) {
            croak 'Invalid table: '.$q_table;
        }
    }
    # The DBM internal table_name may be different.
    $q_table = $me->rdbh->{dbm_tables}{$q_table}{table_name};

    unless (exists $me->rdbh->{f_meta}{$q_table}
            and exists $me->rdbh->{f_meta}{$q_table}{col_names}
            and ref $me->rdbh->{f_meta}{$q_table}{col_names} eq 'ARRAY') {
        croak 'Invalid DBM table info, could be an incompatible version';
    }
    my $col_names = $me->rdbh->{f_meta}{$q_table}{col_names};

    my %h;
    my $i;
    for my $col (@$col_names) {
        $h{Column_Idx}{$col} = ++$i;
    }
    $h{Columns} = [ sort { $h{Column_Idx}{$a} cmp $h{Column_Idx}{$b} } keys %{$h{Column_Idx}} ];
    $h{PrimaryKeys} = [];
    $me->{TableInfo}{defined $schema ? $schema : ''}{$table} = \%h;
}

1;
