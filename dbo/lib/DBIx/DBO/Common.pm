package DBIx::DBO::Common;

use strict;
use warnings;
use Carp;
use Scalar::Util 'blessed';
use constant PLACEHOLDER => "\x{b1}\x{a4}\x{221e}";

=head1 NAME

DBIx::DBO::Common - Common routines and variables exported to all DBO classes.

=head1 DESCRIPTION

This module automatically exports ALL the methods and variables for use in the other DBO modules.

=head2 do

  $dbo->do($statement)         or die $dbo->dbh->errstr;
  $dbo->do($statement, \%attr) or die $dbo->dbh->errstr;
  $dbo->do($statement, \%attr, @bind_values) or die ...

This provides access to DBI C<do> method.

=cut

use subs qw(ouch oops);
*oops = \&Carp::carp;
*ouch = \&Carp::croak;

our $QuoteIdentifier = 1;
our $_Debug_SQL = 0;
our @CARP_NOT;
our $placeholder = PLACEHOLDER;
$placeholder = qr/\Q$placeholder/;

sub import {
    my $caller = caller;
    push @CARP_NOT, $caller;
    no strict 'refs';
    for (qw(QuoteIdentifier _Debug_SQL)) {
        *{$caller.'::'.$_} = \${__PACKAGE__.'::'.$_};
    }
    *{$caller.'::CARP_NOT'} = \@{__PACKAGE__.'::CARP_NOT'};
    for (qw(oops ouch blessed _qi _last_sql _carp_last_sql _sql do _parse_col _build_col _parse_val _build_val)) {
        *{$caller.'::'.$_} = \&{$_};
    }
}

sub _qi {
    my $me = shift;
    $QuoteIdentifier ? $me->dbh->quote_identifier(@_) : join '.', @_;
}

sub _last_sql {
    my $me = shift;
    my $ref = (Scalar::Util::reftype($me) eq 'REF' ? $$me : $me)->{LastSQL} ||= [];
    @$ref = @_ if @_;
    $ref;
}

sub _carp_last_sql {
    my $me = shift;
    my ($cmd, $sql, @bind) = @{$me->_last_sql};
    local $Carp::Verbose = 1 if $_Debug_SQL > 1;
    my @mess = split /\n/, Carp::shortmess("\t$cmd called");
    splice @mess, 0, 3 if $Carp::Verbose;
    warn join "\n", $sql, '('.join(', ', map $me->rdbh->quote($_), @bind).')', @mess;
}

sub _sql {
    my ($me, $sql) = splice @_, 0, 2;
    my $cmd = (caller(1))[3];
    $me->_last_sql($cmd, $sql, @_);
    $me->_carp_last_sql if $_Debug_SQL;
}

sub do {
    my ($me, $sql, $attr, @bind) = @_;
    $me->_sql($sql, @bind);
    $me->dbh->do($sql, $attr, @bind);
}

sub _parse_col {
    my ($me, $col) = @_;
    if (blessed $col and $col->isa('DBIx::DBO::Column')) {
        for my $tbl ($me->_tables) {
            return $col if $col->[0] == $tbl;
        }
        # TODO: Flesh out this ouch a bit
        ouch 'Invalid table';
    }
    ouch 'Invalid column: '.$col if ref $col;
    for my $tbl ($me->_tables) {
        return $tbl->column($col) if exists $tbl->{Fields}{$col};
    }
    ouch 'No such column: '.$col;
}

sub _build_col {
    my ($me, $col) = @_;
    $me->_qi($me->_table_alias($col->[0]), $col->[1]);
}

sub _parse_val {
    my ($me, $fld, $nochk) = @_;
    my @field;
    if (ref $fld eq 'SCALAR') {
        $field[0] = [];
        $field[1] = $$fld;
    } elsif (ref $fld eq 'HASH') {
        if (exists $fld->{COL}) {
            ouch 'Invalid HASH containing both COL and VAL' if exists $fld->{VAL};
            $field[0] = $me->_parse_col($fld->{COL});
        } else {
            $field[0] = exists $fld->{VAL} ? $fld->{VAL} : [];
        }
        $field[1] = $fld->{FUNC} if defined $fld->{FUNC};
        $field[2] = $fld->{AS} if defined $fld->{AS};
        if (defined $fld->{ORDER}) {
            $field[3] = $fld->{ORDER};
            ouch 'Invalid ORDER, must be ASC or DESC' if $field[3] !~ /^(A|DE)SC$/;
        }
    } else {
        $field[0] = $fld;
    }
    $field[0] = [ $field[0] ] unless ref $field[0] eq 'ARRAY';
    # Swap placeholders
    my $with = @{$field[0]};
    if (defined $field[1]) {
        my $need = 0;
        $field[1] =~ s/((?<!\\)(['"`]).*?[^\\]\2|\?)/$1 eq '?' ? scalar($need++, PLACEHOLDER) : $1/eg;
        ouch 'Wrong number of fields/values, called with '.$with.' while needing '.$need if $need != $with;
    } elsif (!$nochk and $with != 1) {
        ouch 'Wrong number of fields/values, called with '.$with.' while needing 1';
    }
    return (@field);
}

sub _build_val {
    my ($me, $bind, $fld, $func, $alias, $order) = @_;
    if (defined $alias) {
        $alias = ' AS '.$me->_qi($alias);
    } elsif (defined $order) {
        $alias = ' '.$order;
    } else {
        $alias = '';
    }
    my @ary = map {
        if (!ref $_) {
            push @$bind, $_;
            '?';
        } elsif (blessed $_ and $_->isa('DBIx::DBO::Column')) {
            $me->_build_col($_);
        } elsif (ref $_ eq 'SCALAR') {
            $$_;
        } else {
            ouch 'Invalid field: '.$_;
        }
    } @$fld;
    return $ary[0].$alias unless defined $func;
    $func =~ s/$placeholder/shift @ary/eg;
    return $func.$alias;
}

1;
