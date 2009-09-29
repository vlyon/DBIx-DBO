package DBIx::DBO::Row;
use DBIx::DBO::Common;

use strict;
use warnings;

use overload '@{}' => sub {${$_[0]}->{row} || []}, '%{}' => sub {${$_[0]}->{hash}};
use overload fallback => 1;

sub dbh { ${$_[0]}->{DBO}->dbh }
sub rdbh { ${$_[0]}->{DBO}->rdbh }

sub _new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $me = \{ DBO => shift, row => undef, hash => {} };
    blessed $$me->{DBO} and $$me->{DBO}->isa('DBIx::DBO') or ouch 'Invalid DBO Object';
    bless $me, $class;
}

sub DESTROY {
    undef ${$_[0]};
}

1;
