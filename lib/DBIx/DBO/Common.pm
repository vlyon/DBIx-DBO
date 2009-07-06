package DBIx::DBO::Common;

use strict;
use warnings;
use Scalar::Util 'blessed';

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

our $QuoteIdentifier = 1;
our $_Debug_SQL = 0;
our @CARP_NOT;

use subs qw(ouch oops);
*oops = \&Carp::carp;
*ouch = \&Carp::croak;

sub import {
  my $caller = caller;
  push @CARP_NOT, $caller;
  no strict 'refs';
  for (qw(QuoteIdentifier _Debug_SQL)) {
    *{$caller.'::'.$_} = \${__PACKAGE__.'::'.$_};
  }
  *{$caller.'::CARP_NOT'} = \@{__PACKAGE__.'::CARP_NOT'};
  for (qw(oops ouch blessed _qi _last_sql _carp_last_sql _sql do)) {
    *{$caller.'::'.$_} = \&{$_};
  }
}

sub _qi {
  my $me = shift;
  $QuoteIdentifier ? $me->dbh->quote_identifier(@_) : join '.', @_;
}

sub _last_sql {
  my $me = shift;
  my $ref = (Scalar::Util::reftype($me) eq 'REF' ? $$me : $me)->{'LastSQL'} ||= [];
  @$ref = @_ if @_;
  $ref;
}

sub _carp_last_sql {
  my $me = shift;
  my ($cmd, $sql, @bind) = @{$me->_last_sql};
  local $Carp::Verbose = 1 if $_Debug_SQL > 1;
  my @mess = split /\n/, Carp::shortmess("\t$cmd called");
  while ($#mess > 0 and $mess[1] =~ /^\tDBIx::DBO::/) {
    shift @mess;
  }
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

1;
