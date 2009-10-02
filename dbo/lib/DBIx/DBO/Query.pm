package DBIx::DBO::Query;
use DBIx::DBO::Common;
use Devel::Peek 'SvREFCNT';

use strict;
use warnings;

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
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $me = { DBO => shift, sql => undef };
    blessed $me->{DBO} and $me->{DBO}->isa('DBIx::DBO') or ouch 'Invalid DBO Object';
    ouch 'No table specified in new Query' unless @_;
    bless $me, $class;

    for my $table (@_) {
        $me->add_table($table);
    }
    $me->_blank;
    return wantarray ? ($me, $me->_tables) : $me;
}

=head2 add_table

  $query->add_table($table);
  $query->add_table([$schema, $table]);
  $query->add_table($table_object);

Add a new table object for the table specified to this query.
This will perform a comma (", ") join.

Returns the table object.

=cut

sub add_table {
    my ($me, $tbl) = @_;
    $tbl = $me->{DBO}->table($tbl) unless blessed $tbl and $tbl->isa('DBIx::DBO::Table');
    push @{$me->{Tables}}, $tbl;
    push @{$me->{Join}}, ', ';
#    push @{$me->{JoinOn}}, undef;
    return $tbl;
}

sub _tables {
    my $me = shift;
    @{$me->{'Tables'}};
}

sub _table_idx {
    my ($me, $tbl) = @_;
    for my $i (0 .. $#{$me->{'Tables'}}) {
        return $i if $tbl == $me->{'Tables'}[$i];
    }
    return undef;
}

sub _table_alias {
    my ($me, $tbl) = @_;
    my $i = $me->_table_idx($tbl);
    ouch 'The table is not in this query' unless defined $i;
    $#{$me->{'Tables'}} > 0 ? 't'.($i + 1) : ();
}

sub _blank {
    my $me = shift;
    $me->unwhere;
#    $me->{'IsDistinct'} = 0;
    $me->{Row_Count} = undef;
    $me->{OrderBy} = [];
    $me->{GroupBy} = [];
    $me->{Showing} = [];
}

sub show {
    my $me = shift;
    undef $me->{'sql'};
    my @flds;
    while (my $fld = shift @_) {
        if (blessed $fld and $fld->isa('DBIx::DBO::Table')) {
            ouch 'Invalid table field' unless defined $me->_table_idx($fld);
            push @flds, $fld;
            next;
        }
        # If the $fld is just a scalar use it as a column name not a value
        push @flds, [ $me->_parse_col_val($fld) ];
    }
    $me->{Showing} = \@flds;
}

sub where {
    my $me = shift;
    # Find the current Where_Logic
    my $ref = $me->{Where_Logic};
    $ref = $ref->[$_] for (@{$me->{Bracket_Refs}});

    $me->_add_where($ref, @_);
}

sub unwhere {
    my $me = shift;
    # TODO: ...
    $me->{Where_Logic} = [];
    $me->{Bracket_Refs} = [];
    $me->{Brackets} = [];
    # This forces a new search
    undef $me->{sql};
}

##
# This will ad an arrayref to the $ref given.
# The arrayref will contain 5 values:
#  $op, $fld_func, $fld, $val_func, $val, $force
#  $op is the operator (those supported differ by DBD)
#  $fld_func is a SCALAR of the form ...
##
sub _add_where {
    my $me = shift;
    my ($ref, $fld, $op, $val, %opt) = @_;

    undef $me->{sql}; # Force a new search
    if ($opt{FORCE}) {
        ouch 'Invalid option, FORCE must be AND or OR' if $opt{FORCE} ne 'AND' and $opt{FORCE} ne 'OR';
    }

    # If the $fld is just a scalar use it as a column name not a value
    ($fld, my $fld_func) = $me->_parse_col_val($fld);
    ($val, my $val_func) = $me->_parse_val($val, 1);

    # Deal with NULL values
    if (@$val == 1 and !defined $val->[0] and !defined $val_func) {
        if ($op eq '=') { $op = 'IS'; $val_func = 'NULL'; delete $val->[0]; }
        elsif ($op eq '!=') { $op = 'IS NOT'; $val_func = 'NULL'; delete $val->[0]; }
    }

    # Deal with array values: BETWEEN & IN
    unless (defined $val_func) {
        if ($op eq 'BETWEEN' or $op eq 'NOT BETWEEN') {
            ouch 'Invalid value argument, BETWEEN requires 2 values'
                if ref $val ne 'ARRAY' or @$val != 2;
            $val_func = $me->PLACEHOLDER.' AND '.$me->PLACEHOLDER;
        } elsif ($op eq 'IN' or $op eq 'NOT IN') {
            if (ref $val eq 'ARRAY') {
                ouch 'Invalid value argument, IN requires at least 1 value' if @$val == 0;
            } else {
                $val = [ $val ];
            }
            # Add to previous 'IN' and 'NOT IN' Wheres
            unless ($opt{FORCE} and $opt{FORCE} ne _op_ag($op)) {
                for my $lim (grep $$_[0] eq $op, @{$ref}) {
                    next if defined $$lim[1] xor defined $fld;
                    next if defined $$lim[1] and defined $fld and $$lim[1] != $fld;
                    last if ($$lim[5] and $$lim[5] ne _op_ag($op));
                    last if $$lim[4] ne '('.join(',', ($me->PLACEHOLDER) x @{$$lim[2]}).')';
                    push @{$$lim[2]}, @$val;
                    $$lim[4] = '('.join(',', ($me->PLACEHOLDER) x @{$$lim[2]}).')';
                    return;
                }
            }
            $val_func = '('.join(',', ($me->PLACEHOLDER) x @$val).')';
        } elsif (@$val != 1) {
            # Check that there is only 1 placeholder
            ouch 'Wrong number of fields/values, called with '.@$val.' while needing 1';
        }
    }

    # Validate the fields
    for my $f (@$fld, @$val) {
        if (blessed $f and $f->isa('DBIx::DBO::Column')) {
            ouch 'Invalid table field' unless defined $me->_table_idx($f->[0]);
        } elsif (my $type = ref $f) {
            ouch 'Invalid value type: '.$type;
        }
    }

    push @{$ref}, [ $op, $fld_func, $fld, $val_func, $val, $opt{FORCE} ];
#use Data::Dumper; warn Data::Dumper->Dump([$me->{DBO}, $ref], [qw(dbo ref)]);
}

sub _parse_col_val {
    my $me = shift;
    my $col = shift;
    return $me->_parse_val($col) if ref $col;
    for my $tbl ($me->_tables) {
        return [ $tbl->column($col) ] if exists $tbl->{Column_Idx}{$col};
    }
    ouch 'No such column: '.$col;
}

sub _op_ag {
    return 'OR' if $_[0] eq '=' or $_[0] eq 'IS' or $_[0] eq '<=>' or $_[0] eq 'IN' or $_[0] eq 'BETWEEN';
    return 'AND' if $_[0] eq '!=' or $_[0] eq 'IS NOT' or $_[0] eq '<>' or $_[0] eq 'NOT IN' or $_[0] eq 'NOT BETWEEN';
}

=head2 arrayref

  $query->arrayref($slice);

Run the query using L<DBI-E<gt>selectall_arrayref|DBI/"selectall_arrayref"> which returns the result as an arrayref.
C<$slice> is optional, if given it will be added to $attr{Slice} - See L<DBI-E<gt>selectall_arrayref|DBI/"selectall_arrayref">.

=cut

sub arrayref {
    my $me = shift;
    my $slice = shift;
    $slice = {Slice => $slice} if $slice;
    $me->_sql($me->sql, $me->_bind_params);
    $me->rdbh->selectall_arrayref($me->sql, $slice, $me->_bind_params);
}

=head2 hashref

  $query->hashref($key_field);

Run the query using L<DBI-E<gt>selectall_hashref|DBI/"selectall_hashref"> which returns the result as an hashref.
C<$key_field> defines which column, or columns, are used as keys in the returned hash.

=cut

sub hashref {
    my $me = shift;
    my $key = shift;
    $me->_sql($me->sql, $me->_bind_params);
    $me->rdbh->selectall_hashref($me->sql, $key, undef, $me->_bind_params);
}

sub _bind_params {
    my $me = shift;
    @{$me->{Show_Bind}}, @{$me->{From_Bind}}, @{$me->{Where_Bind}};
}

sub fetch {
    my $me = shift;
    $me->run unless $me->sth->{Active};

    # Detach the old record if there is still another referance to it
    $me->{Row}->_detach if defined $me->{Row} and SvREFCNT(${$me->{Row}}) > 1;

    my $row = $me->row;
#    $$row->{columns} ||= [ @{$me->{sth}{NAME}} ]; # Is this needed?
    $$row->{hash} = $me->{hash};

    # Fetch and store the data then return the Row on success and undef on failure or no more rows
    ($$row->{array} = $me->{sth}->fetch) ? $me->{Row} : undef %$row;
}

sub row {
    my $me = shift;
    return $me->{Row} if $me->{Row} and $me->{sql};
#    $me->{Row} = \{ DBO => $me->{DBO}, array => undef, hash => {}, Query => $me };
#    bless $me->{Row}, 'DBIx::DBO::Row';
    $me->{Row} = $me->{DBO}->row($me);
}

sub run {
    my $me = shift;
    my $row = $me->row;
    undef $$row->{array};
    undef %$row;

    my $rv = $me->_execute;
    $me->_bind_cols_to_hash;
    return $rv;
}

sub _execute {
    my $me = shift;
    $me->_sql($me->sql, $me->_bind_params);
    $me->sth->execute($me->_bind_params);
}

sub _bind_cols_to_hash {
    my $me = shift;
    unless ($me->{hash}) {
        # Bind only to the first column of the same name
        my $i = 1;
        for (@{$me->{sth}{NAME}}) {
            $me->{sth}->bind_col($i, \$me->{hash}{$_}) unless exists $me->{hash}{$_};
            $i++;
        }
    }
}

sub rows {
    my $me = shift;
    unless (defined $me->{Row_Count}) {
        $me->run unless $me->sth->{Active}; # Should this be $me->sth->{Active}?
        $me->{Row_Count} = $me->sth->rows;
        if ($me->{Row_Count} == -1) {
            # TODO: Handle DISTINCT and GROUP BY
            (my $sql = $me->sql) =~ s/\Q $me->{show} FROM / COUNT(*) FROM /;
            $me->{Row_Count} = ( $me->rdbh->selectrow_array($sql, undef, $me->_bind_params) )[0];
        }
    }
    $me->{Row_Count};
}

sub sth {
    my $me = shift;
    # Ensure the sql is rebuilt if needed
    my $sql = $me->sql;
    $me->{sth} ||= $me->rdbh->prepare($sql);
}

sub sql {
    my $me = shift;
    $me->{sql} ||= $me->_build_sql;
}

sub _build_sql {
    my $me = shift;
    undef $me->{sth};
    my $sql = 'SELECT ';
    $sql .= $me->_build_show;
    $sql .= ' FROM '.$me->_build_from;
    $sql .= ' WHERE '.$_ if $_ = $me->_build_complex_where;
    $sql .= ' ORDER BY '.$_ if $_ = $me->_build_order;
    $sql .= ' LIMIT '.$me->{Limit} if defined $me->{Limit};
    $me->{sql} = $sql;
}

sub _build_show {
    my $me = shift;
    undef @{$me->{Show_Bind}};
    return $me->{show} = '*' unless @{$me->{Showing}};
    my @flds;
    for my $fld (@{$me->{Showing}}) {
        push @flds, (blessed $fld and $fld->isa('DBIx::DBO::Table'))
            ? $me->_qi($me->_table_alias($fld) || $fld->{Name}).'.*'
            : $me->_build_val($me->{Show_Bind}, @$fld);
    }
    $me->{show} = join ', ', @flds;
}

sub _build_from {
    my $me = shift;
    undef @{$me->{From_Bind}};
    $me->{from} = $me->_build_table($me->{Tables}[0]);
    for (my $i = 1; $i < @{$me->{Tables}}; $i++) {
        $me->{from} .= $me->{Join}[$i].$me->_build_table($me->{Tables}[$i]);
        # TODO: JoinOn
    }
    $me->{from};
}

sub _build_table {
    my $me = shift;
    my $t = shift;
    my $alias = $me->_table_alias($t);
    $alias = $alias ? ' AS '.$me->_qi($alias) : '';
    $t->_quoted_name.$alias;
}

sub _build_complex_where {
    my $me = shift;
    undef @{$me->{Where_Bind}};
    my @chunks = $me->_build_complex_chunk($me->{Where_Bind}, 'OR', $me->{Where_Logic});
    $me->{where} = join ' AND ', @chunks;
}

sub _build_complex_chunk {
    my ($me, $bind, $ag, $lims) = @_;
    my @str;
    # Make a copy so we can hack at it
    my @lims = @$lims;
    while (my $lim = shift @lims) {
        my @ary;
        if (ref $lim->[0]) {
            @ary = $me->_build_complex_chunk($bind, $ag eq 'OR' ? 'AND' : 'OR', $lim);
        } else {
            @ary = $me->_build_complex_piece($bind, @$lim);
            my ($op, $fld_func, $fld, $val_func, $val, $force) = @$lim;
            # Group AND/OR'ed for same fld if $force or $op requires it
            if ($ag eq ($force || _op_ag($op))) {
                for (my $i = $#lims; $i >= 0; $i--) {
                    # Right now this starts with the last @lims and works backward
                    # It splices when the ag is the correct AND/OR and the funcs match and all flds match
                    next if (ref $lims[$i]->[0] or $ag ne ($lims[$i]->[5] || _op_ag($lims[$i]->[0])));
                    no warnings 'uninitialized';
                    next if $lims[$i]->[1] ne $fld_func;
                    use warnings 'uninitialized';
                    my $l = $lims[$i]->[2];
                    next if ((ref $l eq 'ARRAY' ? "@$l" : $l) ne (ref $fld eq 'ARRAY' ? "@$fld" : $fld));
                    push @ary, $me->_build_complex_piece($bind, @{splice @lims, $i, 1});
                }
            }
        }
        push @str, @ary == 1 ? $ary[0] : '('.join(' '.$ag.' ', @ary).')';
    }
    return @str;
}

sub _build_complex_piece {
    my ($me, $bind, $op, $fld_func, $fld, $val_func, $val) = @_;
    $me->_build_val($bind, $fld, $fld_func) .' '.$op.' '.$me->_build_val($bind, $val, $val_func);
}

sub _build_order {
    my $me = shift;
    # TODO: ...
    $me->{order} = '';
}

sub DESTROY {
    undef %{$_[0]};
}

1;
