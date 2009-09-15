package DBIx::DBO::Table;

use strict;
use warnings;
use DBIx::DBO::Common;

sub dbh { $_[0]->{'DBO'}->dbh }
sub rdbh { $_[0]->{'DBO'}->rdbh }

sub new {
    my ($proto, $dbo, $table) = @_;
    my $class = ref($proto) || $proto;
    blessed $dbo and $dbo->isa('DBIx::DBO') or ouch 'Invalid DBO Object';
    (my $schema, $table, $_) = $dbo->table_info($table) or ouch 'No such table: '.$table;
    bless { %$_, Schema => $schema, Name => $table, DBO => $dbo, LastInsertID => undef }, $class;
}

sub DESTROY {
    undef %{$_[0]};
}

1;
