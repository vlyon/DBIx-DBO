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

=head2 dbh

The read-write DBI handle.

=head2 rdbh

The read-only DBI handle, or if there is no read-only connection, the read-write DBI handle.

=cut

use subs qw(ouch oops);
*oops = \&Carp::carp;
*ouch = \&Carp::croak;

our %Config = (
    QuoteIdentifier => 1,
    _Debug_SQL => 0,
);
our @CARP_NOT;
our $placeholder = PLACEHOLDER;
$placeholder = qr/\Q$placeholder/;

sub import {
    my $caller = caller;
    push @CARP_NOT, $caller;
    no strict 'refs';
    *{$caller.'::Config'} = \%{__PACKAGE__.'::Config'};
    *{$caller.'::CARP_NOT'} = \@{__PACKAGE__.'::CARP_NOT'};
    for (qw(oops ouch blessed)) {
        *{$caller.'::'.$_} = \&{$_};
    }
}

sub dbh { $_[0]{DBO}->dbh }
sub rdbh { $_[0]{DBO}->rdbh }

sub _qi {
    my $me = shift;
    return $me->dbh->quote_identifier(@_) if $me->config('QuoteIdentifier');
    # Strip off any null/undef elements (ie schema)
    shift while @_ and !defined $_[0];
    join '.', @_;
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
    local $Carp::Verbose = 1 if $me->config('_Debug_SQL') > 1;
    my @mess = split /\n/, Carp::shortmess("\t$cmd called");
    splice @mess, 0, 3 if $Carp::Verbose;
    warn join "\n", $sql, '('.join(', ', map $me->rdbh->quote($_), @bind).')', @mess;
}

sub _sql {
    my ($me, $sql) = splice @_, 0, 2;
    my $cmd = (caller(1))[3];
    $me->_last_sql($cmd, $sql, @_);
    $me->_carp_last_sql if $me->config('_Debug_SQL');
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
        return $tbl->column($col) if exists $tbl->{Column_Idx}{$col};
    }
    ouch 'No such column: '.$col;
}

sub _build_col {
    my ($me, $col) = @_;
    $me->_qi($me->_table_alias($col->[0]), $col->[1]);
}

sub _parse_val {
    my $me = shift;
    my $fld = shift;
    my $check_fld = shift || '';
    my @field;
    if (ref $fld eq 'SCALAR') {
        $field[0] = [];
        $field[1] = $$fld;
        ouch 'Invalid '.($check_fld eq 'Column' ? 'column' : 'field').' reference (scalar ref to undef)'
            unless defined $field[1];
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
        my $need = $me->_substitute_placeholders($field[1]);
        ouch "The number of params ($with) does not match the number of placeholders ($need)" if $need != $with;
    } elsif ($with != 1 and $check_fld ne 'Auto') {
        ouch 'Invalid '.($check_fld eq 'Column' ? 'column' : 'field')." reference (passed $with params instead of 1)";
    }
    return (@field);
}

sub _substitute_placeholders {
    my $me = shift;
    my $num_placeholders = 0;
    $_[0] =~ s/((?<!\\)(['"`]).*?[^\\]\2|\?)/$1 eq '?' ? ++$num_placeholders && PLACEHOLDER : $1/eg;
    return $num_placeholders;
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
    # Add one value to @ary to make sure the number of placeholders & values match
    push @ary, 'Error';
    $func =~ s/$placeholder/shift @ary/eg;
    # At this point all the values should have been used and @ary must only have 1 item!
    die "Number of placeholders and values don't match!" if @ary != 1;
    return $func.$alias;
}

sub _build_where {
    ouch 'Wrong number of arguments' if @_ & 1;
    my ($me, $bind) = splice @_, 0, 2;
    my @where;
    while (my ($col, $val) = splice @_, 0, 2) {
        push @where, $me->_build_col($me->_parse_col($col)) .
            ( defined $val ? (ref $val ne 'SCALAR' or $$val !~ /^\s*(?:NOT\s+)NULL\s*$/is) ?
                ' = '.$me->_build_val($bind, $me->_parse_val($val)) :
                ' IS '.$$val :
                ' IS NULL' );
    }
    return @where ? ' WHERE '.join(' AND ', @where) : '';
}

1;
