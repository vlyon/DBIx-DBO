package # hide from PAUSE
    DBIx::DBO::Common;

use 5.008;
use strict;
use warnings;
use Carp;
use Scalar::Util 'blessed';
use constant PLACEHOLDER => "\x{b1}\x{a4}\x{221e}";

# Common routines and variables exported to all DBO classes.
# This module automatically exports ALL the methods and variables for use in the other DBO modules.

use subs qw(ouch oops);
*oops = \&Carp::carp;
*ouch = \&Carp::croak;

our %Config = (
    QuoteIdentifier => 1,
    DebugSQL => 0,
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
    shift while @_ and not (defined $_[0] and length $_[0]);
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
    local $Carp::Verbose = 1 if $me->config('DebugSQL') > 1;
    my @mess = split /\n/, Carp::shortmess("\t$cmd called");
    splice @mess, 0, 3 if $Carp::Verbose;
    warn join "\n", $sql, '('.join(', ', map $me->rdbh->quote($_), @bind).')', @mess;
}

sub _sql {
    my ($me, $sql) = splice @_, 0, 2;
    my $cmd = (caller(1))[3];
    $me->_last_sql($cmd, $sql, @_);
    $me->_carp_last_sql if $me->config('DebugSQL');
}

sub do {
    my ($me, $sql, $attr, @bind) = @_;
    $me->_sql($sql, @bind);
    $me->dbh->do($sql, $attr, @bind);
}

sub _build_sql_select {
    my $me = shift;
    my $h = shift;
    my $sql = 'SELECT '.$me->_build_show($h);
    $sql .= ' FROM '.$me->_build_from($h);
    $sql .= ' WHERE '.$_ if $_ = $me->_build_where($h);
    $sql .= ' GROUP BY '.$_ if $_ = $me->_build_group($h);
    $sql .= ' HAVING '.$_ if $_ = $me->_build_having($h);
    $sql .= ' ORDER BY '.$_ if $_ = $me->_build_order($h);
    $sql .= ' '.$_ if $_ = $me->_build_limit($h);
    $sql;
}

sub _bind_params_select {
    my $me = shift;
    my $h = shift;
    map {
        exists $h->{$_} ? @{$h->{$_}} : ()
    } qw(Show_Bind From_Bind Where_Bind Group_Bind Having_Bind Order_Bind);
}

# TODO: Should we die if GROUP BY is set?
sub _build_sql_update {
    my $me = shift;
    my $h = shift;
    my $sql = 'UPDATE '.$me->_build_from($h);
    $sql .= ' SET '.$me->_build_set($h, @_);
    $sql .= ' WHERE '.$_ if $_ = $me->_build_where($h);
    $sql .= ' ORDER BY '.$_ if $_ = $me->_build_order($h);
    $sql .= ' '.$_ if $_ = $me->_build_limit($h);
    $sql;
}

sub _bind_params_update {
    my $me = shift;
    my $h = shift;
    map {
        exists $h->{$_} ? @{$h->{$_}} : ()
    } qw(From_Bind Set_Bind Where_Bind Order_Bind);
}

sub _build_sql_delete {
    my $me = shift;
    my $h = shift;
    my $sql = 'DELETE FROM '.$me->_build_from($h);
    $sql .= ' WHERE '.$_ if $_ = $me->_build_where($h);
    $sql .= ' ORDER BY '.$_ if $_ = $me->_build_order($h);
    $sql .= ' '.$_ if $_ = $me->_build_limit($h);
    $sql;
}

sub _bind_params_delete {
    my $me = shift;
    my $h = shift;
    map {
        exists $h->{$_} ? @{$h->{$_}} : ()
    } qw(From_Bind Where_Bind Order_Bind);
}

sub _build_table {
    my $me = shift;
    my $t = shift;
    my $alias = $me->_table_alias($t);
    $alias = $alias ? ' AS '.$me->_qi($alias) : '';
    $t->_quoted_name.$alias;
}

sub _build_show {
    my $me = shift;
    my $h = shift;
    return $h->{show} if defined $h->{show};
    undef @{$h->{Show_Bind}};
    return $h->{show} = '*' unless @{$h->{Showing}};
    my @flds;
    for my $fld (@{$h->{Showing}}) {
        push @flds, (blessed $fld and $fld->isa('DBIx::DBO::Table'))
            ? $me->_qi($me->_table_alias($fld) || $fld->{Name}).'.*'
            : $me->_build_val($h->{Show_Bind}, @$fld);
    }
    $h->{show} = join ', ', @flds;
}

sub _build_from {
    my $me = shift;
    my $h = shift;
    return $h->{from} if defined $h->{from};
    undef @{$h->{From_Bind}};
    $h->{from} = $me->_build_table(($me->tables)[0]);
    for (my $i = 1; $i < $me->tables; $i++) {
        $h->{from} .= $h->{Join}[$i].$me->_build_table(($me->tables)[$i]);
        if ($h->{Join_On}[$i]) {
            $h->{from} .= ' ON '.join(' AND ', $me->_build_where_chunk($h->{From_Bind}, 'OR', $h->{Join_On}[$i]));
        }
    }
    $h->{from};
}

sub _valid_col {
    my ($me, $col) = @_;
    for my $tbl ($me->tables) {
        return $col if $col->[0] == $tbl;
    }
    ouch 'Invalid column, the column is from a table not included in this query';
}

sub _parse_col {
    my ($me, $col, $_check_aliases) = @_;
    if (ref $col) {
        return $me->_valid_col($col) if blessed $col and $col->isa('DBIx::DBO::Column');
        ouch 'Invalid column: '.$col;
    }
    $me->column($col, $_check_aliases);
}

sub _build_col {
    my ($me, $col) = @_;
    $me->_qi($me->_table_alias($col->[0]), $col->[1]);
}

sub _parse_val {
    my $me = shift;
    my $fld = shift;
    my %c = (Check => '', @_);

    my $func;
    my %opt;
    if (ref $fld eq 'SCALAR') {
        ouch 'Invalid '.($c{Check} eq 'Column' ? 'column' : 'field').' reference (scalar ref to undef)'
            unless defined $$fld;
        $func = $$fld;
        $fld = [];
    } elsif (ref $fld eq 'HASH') {
        $func = $fld->{FUNC} if exists $fld->{FUNC};
        $opt{AS} = $fld->{AS} if exists $fld->{AS};
        if (exists $fld->{ORDER}) {
            ouch 'Invalid ORDER, must be ASC or DESC' if $fld->{ORDER} !~ /^(A|DE)SC$/i;
            $opt{ORDER} = uc $fld->{ORDER};
        }
        $opt{COLLATE} = $fld->{COLLATE} if exists $fld->{COLLATE};
        if (exists $fld->{COL}) {
            ouch 'Invalid HASH containing both COL and VAL' if exists $fld->{VAL};
            my @cols = ref $fld->{COL} eq 'ARRAY' ? @{$fld->{COL}} : $fld->{COL};
            $fld = [ map $me->_parse_col($_, $c{Aliases}), @cols ];
        } else {
            $fld = exists $fld->{VAL} ? $fld->{VAL} : [];
        }
    } elsif (blessed $fld and $fld->isa('DBIx::DBO::Column')) {
        return [ $me->_valid_col($fld) ];
    }
    $fld = [$fld] unless ref $fld eq 'ARRAY';

    # Swap placeholders
    my $with = @$fld;
    if (defined $func) {
        my $need = $me->_substitute_placeholders($func);
        ouch "The number of params ($with) does not match the number of placeholders ($need)" if $need != $with;
    } elsif ($with != 1 and $c{Check} ne 'Auto') {
        ouch 'Invalid '.($c{Check} eq 'Column' ? 'column' : 'field')." reference (passed $with params instead of 1)";
    }
    return ($fld, $func, \%opt);
}

sub _substitute_placeholders {
    my $me = shift;
    my $num_placeholders = 0;
    $_[0] =~ s/((?<!\\)(['"`]).*?[^\\]\2|\?)/$1 eq '?' ? ++$num_placeholders && PLACEHOLDER : $1/eg;
    return $num_placeholders;
}

sub _build_val {
    my ($me, $bind, $fld, $func, $opt) = @_;
    my $extra = '';
    $extra .= ' COLLATE '.$me->rdbh->quote($opt->{COLLATE}) if exists $opt->{COLLATE};
    $extra .= ' AS '.$me->_qi($opt->{AS}) if exists $opt->{AS};
    $extra .= " $opt->{ORDER}" if exists $opt->{ORDER};

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
    unless (defined $func) {
        die "Number of placeholders and values don't match!" if @ary != 1;
        return $ary[0].$extra;
    }
    # Add one value to @ary to make sure the number of placeholders & values match
    push @ary, 'Error';
    $func =~ s/$placeholder/shift @ary/eg;
    # At this point all the values should have been used and @ary must only have 1 item!
    die "Number of placeholders and values don't match @ary!" if @ary != 1;
    return $func.$extra;
}

# Construct the WHERE clause
sub _build_where {
    my $me = shift;
    my $h = shift;
    return $h->{where} if defined $h->{where};
    undef @{$h->{Where_Bind}};
    my @where;
    push @where, $me->_build_quick_where($h->{Where_Bind}, @{$h->{Quick_Where}}) if exists $h->{Quick_Where};
    push @where, $me->_build_where_chunk($h->{Where_Bind}, 'OR', $h->{Where_Data}) if exists $h->{Where_Data};
    $h->{where} = join ' AND ', @where;
}

# Construct the WHERE contents of one set of parentheses
sub _build_where_chunk {
    my $me = shift;
    my ($bind, $ag, $whs) = @_;
    my @str;
    # Make a copy so we can hack at it
    my @whs = @$whs;
    while (my $wh = shift @whs) {
        my @ary;
        if (ref $wh->[0]) {
            @ary = $me->_build_where_chunk($bind, $ag eq 'OR' ? 'AND' : 'OR', $wh);
        } else {
            @ary = $me->_build_where_piece($bind, @$wh);
            my ($op, $fld, $fld_func, $fld_opt, $val, $val_func, $val_opt, $force) = @$wh;
            # Group AND/OR'ed for same fld if $force or $op requires it
            if ($ag eq ($force || _op_ag($op))) {
                for (my $i = $#whs; $i >= 0; $i--) {
                    # Right now this starts with the last @whs and works backwards
                    # It splices when the ag is the correct AND/OR and the funcs match and all flds match
                    next if (ref $whs[$i][0] or $ag ne ($whs[$i][7] || _op_ag($whs[$i][0])));
                    no warnings 'uninitialized';
                    next if $whs[$i][2] ne $fld_func;
                    use warnings 'uninitialized';
#                    next unless $fld_func ~~ $whs[$i][2];
                    my $l = $whs[$i][1];
                    next if ((ref $l eq 'ARRAY' ? "@$l" : $l) ne (ref $fld eq 'ARRAY' ? "@$fld" : $fld));
#                    next unless $fld ~~ $whs[$i][1];
                    push @ary, $me->_build_where_piece($bind, @{splice @whs, $i, 1});
                }
            }
        }
        push @str, @ary == 1 ? $ary[0] : '('.join(' '.$ag.' ', @ary).')';
    }
    return @str;
}

sub _op_ag {
    return 'OR' if $_[0] eq '=' or $_[0] eq 'IS' or $_[0] eq '<=>' or $_[0] eq 'IN' or $_[0] eq 'BETWEEN';
    return 'AND' if $_[0] eq '!=' or $_[0] eq 'IS NOT' or $_[0] eq '<>' or $_[0] eq 'NOT IN' or $_[0] eq 'NOT BETWEEN';
}

# Construct one WHERE expression
sub _build_where_piece {
    my ($me, $bind, $op, $fld, $fld_func, $fld_opt, $val, $val_func, $val_opt) = @_;
    $me->_build_val($bind, $fld, $fld_func, $fld_opt)." $op ".$me->_build_val($bind, $val, $val_func, $val_opt);
}

# Construct one WHERE expression (simple)
sub _build_quick_where {
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
    join ' AND ', @where;
}

sub _build_set {
    ouch 'Wrong number of arguments' if @_ & 1;
    my $me = shift;
    my $h = shift;
    undef @{$h->{Set_Bind}};
    my @set;
    while (my ($col, $val) = splice @_, 0, 2) {
        push @set, $me->_build_col($me->_parse_col($col)).' = '.$me->_build_val($h->{Set_Bind}, $me->_parse_val($val));
    }
    join ', ', @set;
}

sub _build_group {
    my $me = shift;
    my $h = shift;
    return $h->{group} if defined $h->{group};
    undef @{$h->{Group_Bind}};
    $h->{group} = join ', ', map $me->_build_val($h->{Group_Bind}, @$_), @{$h->{GroupBy}};
}

# Construct the HAVING clause
sub _build_having {
    my $me = shift;
    my $h = shift;
    return $h->{having} if defined $h->{having};
    undef @{$h->{Having_Bind}};
    my @having;
    push @having, $me->_build_where_chunk($h->{Having_Bind}, 'OR', $h->{Having_Data}) if exists $h->{Having_Data};
    $h->{having} = join ' AND ', @having;
}

sub _build_order {
    my $me = shift;
    my $h = shift;
    return $h->{order} if defined $h->{order};
    undef @{$h->{Order_Bind}};
    $h->{order} = join ', ', map $me->_build_val($h->{Order_Bind}, @$_), @{$h->{OrderBy}};
}

sub _build_limit {
    my $me = shift;
    my $h = shift;
    return $h->{limit} if defined $h->{limit};
    return $h->{limit} = '' unless defined $h->{LimitOffset};
    $h->{limit} = 'LIMIT '.$h->{LimitOffset}[0];
    $h->{limit} .= ' OFFSET '.$h->{LimitOffset}[1] if $h->{LimitOffset}[1];
    $h->{limit};
}

sub _set_config {
    my $me = shift;
    my ($ref, $opt, $val) = @_;
    ouch "Invalid value for the 'UseHandle' setting"
        if $opt eq 'UseHandle' and $val and $val ne 'read-only' and $val ne 'read-write';
    my $old = $ref->{$opt};
    $ref->{$opt} = $val;
    return $old;
}

sub _set_dbd_inheritance {
    my $class = shift;
    my $dbd = shift;
    $class =~ s/::DBD::\w+$//;
    # Inheritance
    no strict 'refs';
    unless (@{$class.'::DBD::'.$dbd.'::ISA'}) {
        my @isa = grep $_->can('_set_dbd_inheritance'), @_ ? @_ : @{$class.'::ISA'};
        $_->_set_dbd_inheritance($dbd) for @isa;
        @{$class.'::DBD::'.$dbd.'::ISA'} = ($class, map $_.'::DBD::'.$dbd, @isa);
        mro::set_mro($class.'::DBD::'.$dbd, 'c3');
        Class::C3::initialize() if $] < 5.009_005;
    }
    return $class.'::DBD::'.$dbd;
}

1;
