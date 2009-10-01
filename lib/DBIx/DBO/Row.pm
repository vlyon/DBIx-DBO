package DBIx::DBO::Row;
use DBIx::DBO::Common;
use Scalar::Util 'weaken';

use strict;
use warnings;

use overload '@{}' => sub {${$_[0]}->{array} || []}, '%{}' => sub {${$_[0]}->{hash}};
use overload '**' => \&value, fallback => 1;

sub dbh { ${$_[0]}->{DBO}->dbh }
sub rdbh { ${$_[0]}->{DBO}->rdbh }

sub config {
    my $me = shift;
    ($$me->{Parent} || $$me->{DBO})->config(@_);
}

sub _new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $me = \{ DBO => shift, Parent => shift, array => undef, hash => {} };
    blessed $$me->{DBO} and $$me->{DBO}->isa('DBIx::DBO') or ouch 'Invalid DBO Object';
    if (defined $$me->{Parent}) {
        ouch 'Invalid Parent Object' unless blessed $$me->{Parent};
        weaken $$me->{Parent};
    }
    bless $me, $class;
}

sub tables {
    my $me = shift;
    return unless $$me->{Parent};
    $$me->{Parent}->tables;
}

sub _column_idx {
    my $me = shift;
    my $col = shift;
    my $idx = -1;
    # TODO: Select fields ?
    for my $t ($me->tables) {
        return $idx + $t->{Column_Idx}{$col->[1]} if exists $t->{Column_Idx}{$col->[1]};
        $idx += keys %{$t->{Column_Idx}};
    }
    return undef;
}

sub value {
    my $me = shift;
    my $col = shift;
    ouch 'The record is empty' unless $$me->{array};
    if (blessed $col and $col->isa('DBIx::DBO::Column')) {
        my $i = $me->_column_idx($col);
        return $$me->{array}[$i] if defined $i;
        ouch 'The field '.$me->_qi($col->[0]{Name}, $col->[1]).' was not included in this query';
    }
    return $$me->{hash}{$col} if exists $$me->{hash}{$col};
    ouch 'No such column: '.$col;
}

sub _detach {
    my $me = shift;
    if ($$me->{array}) {
        $$me->{array} = [ @$me ];
        $$me->{hash} = { %$me };
    }
    undef $$me->{Parent}{Row};
    undef $$me->{Parent};
}

sub DESTROY {
    undef ${$_[0]};
}

1;
