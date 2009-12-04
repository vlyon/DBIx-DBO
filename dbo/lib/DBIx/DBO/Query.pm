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
        $me->join_table($table);
    }
    $me->_blank;
    return wantarray ? ($me, $me->_tables) : $me;
}

=head2 join_table

  $query->join_table($table, $join_type);
  $query->join_table([$schema, $table], $join_type);
  $query->join_table($table_object, $join_type);

Join a new table object for the table specified to this query.
This will perform a comma (", ") join unless $join_type is specified.

Returns the table object.

=cut

##
# Comma, INNER, NATURAL, LEFT, RIGHT, FULL
##
sub join_table {
    my ($me, $tbl, $type) = @_;
    if (blessed $tbl and $tbl->isa('DBIx::DBO::Table')) {
        ouch 'This table is already in this query' if $me->_table_idx($tbl);
    } else {
        $tbl = $me->{DBO}->table($tbl);
    }
    if (defined $type) {
        $type =~ s/^\s*/ /;
        $type =~ s/\s*$/ /;
        $type = uc $type;
        $type .= 'JOIN ' if $type !~ /\bJOIN\b/;
    } else {
        $type = ', ';
    }
    push @{$me->{Tables}}, $tbl;
    push @{$me->{build_data}{Join}}, $type;
    push @{$me->{build_data}{JoinOn}}, undef;
    undef $me->{sql};
    undef $me->{build_data}{from};
    return $tbl;
}

sub _tables {
    my $me = shift;
    @{$me->{Tables}};
}

sub _table_idx {
    my ($me, $tbl) = @_;
    for my $i (0 .. $#{$me->{Tables}}) {
        return $i if $tbl == $me->{Tables}[$i];
    }
    return undef;
}

sub _table_alias {
    my ($me, $tbl) = @_;
    my $i = $me->_table_idx($tbl);
    ouch 'The table is not in this query' unless defined $i;
    @{$me->{Tables}} > 1 ? 't'.($i + 1) : ();
}

sub _showing {
    my $me = shift;
    @{$me->{build_data}{Showing}} ? @{$me->{build_data}{Showing}} : @{$me->{Tables}};
}

sub _blank {
    my $me = shift;
    $me->unwhere;
#    $me->{IsDistinct} = 0;
    undef @{$me->{build_data}{Showing}};
    undef @{$me->{build_data}{GroupBy}};
    undef @{$me->{build_data}{OrderBy}};
    undef $me->{build_data}{LimitOffset};
}

=head2 show

  $query->show(@columns);
  $query->show($table1 ** 'id', {FUNC => 'UCASE(?)', COL => 'name', AS => 'NAME'}, ...

Specify which columns to show as an array. If the array is empty all columns will be shown.

=cut

sub show {
    my $me = shift;
    undef $me->{sql};
    undef $me->{build_data}{show};
    undef @{$me->{build_data}{Showing}};
    for my $fld (@_) {
        if (blessed $fld and $fld->isa('DBIx::DBO::Table')) {
            ouch 'Invalid table field' unless defined $me->_table_idx($fld);
            push @{$me->{build_data}{Showing}}, $fld;
            next;
        }
        # If the $fld is just a scalar use it as a column name not a value
        push @{$me->{build_data}{Showing}}, [ $me->_parse_col_val($fld) ];
    }
}

=head2 join_on

  $query->join_on($table_object, $expression1, $operator, $expression2);
  $query->join_on($table2, $table1 ** 'id', '=', $table2 ** 'id');

Join tables on a specific WHERE clause. The first argument is the table object being joined onto.
Then a join on condition follows.

=cut

sub join_on {
    my $me = shift;
    my $t2 = shift;
    my $i = $me->_table_idx($t2) or ouch 'Invalid table object to join onto';

    my ($col1, $col1_func, $col1_opt) = $me->_parse_col_val(shift);
    my $op = shift;
    my ($col2, $col2_func, $col2_opt) = $me->_parse_col_val(shift);

    # Validate the fields
    for my $c (@$col1, @$col2) {
        if (blessed $c and $c->isa('DBIx::DBO::Column')) {
            ouch 'Invalid table field' unless defined $me->_table_idx($c->[0]);
        } elsif (my $type = ref $c) {
            ouch 'Invalid value type: '.$type;
        }
    }

    # Force a new search
    undef $me->{sql};
    undef $me->{build_data}{from};

    $me->{build_data}{Join}[$i] = ' JOIN ' if $me->{build_data}{Join}[$i] eq ', ';
    $me->_add_where($me->{build_data}{JoinOn}[$i] //= [], $op,
        $col1, $col1_func, $col1_opt, $col2, $col2_func, $col2_opt, @_);
}

=head2 where

  $query->where($expression1, $operator, $expression2);
  $query->where($table1 ** 'id', '=', $table2 ** 'id');

Restrict the query with the condition specified (WHERE clause).

=cut

sub where {
    my $me = shift;

    # If the $fld is just a scalar use it as a column name not a value
    my ($fld, $fld_func, $fld_opt) = $me->_parse_col_val(shift);
    my $op = shift;
    my ($val, $val_func, $val_opt) = $me->_parse_val(shift, 'Auto');

    # Validate the fields
    for my $f (@$fld, @$val) {
        if (blessed $f and $f->isa('DBIx::DBO::Column')) {
            ouch 'Invalid table field' unless defined $me->_table_idx($f->[0]);
        } elsif (my $type = ref $f) {
            ouch 'Invalid value type: '.$type;
        }
    }

    # Force a new search
    undef $me->{sql};
    undef $me->{build_data}{where};

    # Find the current Where_Data reference
    my $ref = $me->{build_data}{Where_Data} //= [];
    $ref = $ref->[$_] for (@{$me->{Bracket_Refs}});

    $me->_add_where($ref, $op, $fld, $fld_func, $fld_opt, $val, $val_func, $val_opt, @_);
}

=head2 unwhere

  $query->unwhere();
  $query->unwhere($column);
  $query->unwhere($table1 ** 'id');

Removes all previously added where() restrictions for a column.
If no column is provided, ALL where() restrictions are removed.

=cut

sub unwhere {
    my $me = shift;
    my $col = shift;
    # TODO: Remove a condition by specifying the whole condition
    if ($col) {
        ouch 'Invalid column' unless blessed $col and $col->isa('DBIx::DBO::Column');
        if (exists $me->{build_data}{Where_Data}) {
            # Find the current Where_Data reference
            my $ref = $me->{build_data}{Where_Data};
            $ref = $ref->[$_] for (@{$me->{Bracket_Refs}});

            for (my $i = $#$ref; $i >= 0; $i--) {
                # Remove this Where piece if there is no FUNC and it refers to this column only
                splice @{$ref}, $i, 1
                    if !defined $ref->[$i][2] and @{$ref->[$i][1]} == 1 and $ref->[$i][1][0] == $col;
            }
        }
    } else {
        delete $me->{build_data}{Where_Data};
        $me->{Bracket_Refs} = [];
        $me->{Brackets} = [];
    }
    # This forces a new search
    undef $me->{sql};
    undef $me->{build_data}{where};
}

##
# This will ad an arrayref to the $ref given.
# The arrayref will contain 5 values:
#  $op, $fld_func, $fld, $val_func, $val, $force
#  $op is the operator (those supported differ by DBD)
#  $fld_func is undef or a SCALAR of the form '? AND ?' or 'POSITION(? IN ?)'
#  $fld is an arrayref of columns/values for use with $fld_func
#  $val_func is similar to $fld_func
#  $val is an arrayref of values for use with $val_func
#  $force is one of undef / 'AND' / 'OR' which if defined, overrides the default aggregator
##
sub _add_where {
    my $me = shift;
    my ($ref, $op, $fld, $fld_func, $fld_opt, $val, $val_func, $val_opt, %opt) = @_;

    ouch 'Invalid option, FORCE must be AND or OR'
        if defined $opt{FORCE} and $opt{FORCE} ne 'AND' and $opt{FORCE} ne 'OR';

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

    # Collation
#    if (exists $fld_opt->{COLLATE}) {
#        $fld_func .= " COLLATE $fld_opt->{COLLATE}";
#    }
#    if (exists $val_opt->{COLLATE}) {
#        $val_func .= " COLLATE $val_opt->{COLLATE}";
#    }

    push @{$ref}, [ $op, $fld, $fld_func, $fld_opt, $val, $val_func, $val_opt, $opt{FORCE} ];
}

sub _parse_col_val {
    my $me = shift;
    my $col = shift;
    return $me->_parse_val($col, 'Column') if ref $col;
    for my $tbl ($me->_tables) {
        return [ $tbl->column($col) ] if exists $tbl->{Column_Idx}{$col};
    }
    ouch 'No such column: '.$col;
}

sub group_by {
    my $me = shift;
    undef $me->{sql};
    undef $me->{build_data}{group};
    undef @{$me->{build_data}{GroupBy}};
    for my $col (@_) {
        my @group = $me->_parse_col_val($col);
        push @{$me->{build_data}{GroupBy}}, \@group;
    }
}

sub order_by {
    my $me = shift;
    undef $me->{sql};
    undef $me->{build_data}{order};
    undef @{$me->{build_data}{OrderBy}};
    for my $col (@_) {
        my @order = $me->_parse_col_val($col);
        push @{$me->{build_data}{OrderBy}}, \@order;
    }
}

sub limit {
    my ($me, $rows, $offset) = @_;
    undef $me->{sql};
    undef $me->{build_data}{limit};
    return undef $me->{build_data}{LimitOffset} unless defined $rows;
    eval { use warnings FATAL => 'numeric'; $rows+=0; $offset+=0 };
    ouch 'Non-numeric arguments in limit' if $@;
    @{$me->{build_data}{LimitOffset}} = ($rows, $offset);
}

=head2 arrayref

  $query->arrayref;
  $query->arrayref(\%attr);

Run the query using L<DBI-E<gt>selectall_arrayref|DBI/"selectall_arrayref"> which returns the result as an arrayref.
You can specify a slice by including a 'Slice' or 'Columns' attribute in \%attr - See L<DBI-E<gt>selectall_arrayref|DBI/"selectall_arrayref">.

=cut

sub arrayref {
    my $me = shift;
    my $attr = shift;
    $me->_sql($me->sql, $me->_bind_params);
    my $sql_or_sth = $me->sth->{Active} ? 'sql' : 'sth';
    $me->rdbh->selectall_arrayref($me->{$sql_or_sth}, $attr, $me->_bind_params);
}

=head2 hashref

  $query->hashref($key_field);
  $query->hashref($key_field, \%attr);

Run the query using L<DBI-E<gt>selectall_hashref|DBI/"selectall_hashref"> which returns the result as an hashref.
C<$key_field> defines which column, or columns, are used as keys in the returned hash.

=cut

sub hashref {
    my $me = shift;
    my $key = shift;
    my $attr = shift;
    $me->_sql($me->sql, $me->_bind_params);
    my $sql_or_sth = $me->sth->{Active} ? 'sql' : 'sth';
    $me->rdbh->selectall_hashref($me->{$sql_or_sth}, $key, $attr, $me->_bind_params);
}

=head2 col_arrayref

  $query->col_arrayref;
  $query->col_arrayref(\%attr);

Run the query using L<DBI-E<gt>selectcol_arrayref|DBI/"selectcol_arrayref"> which returns the result as an arrayref of the values of each row in one array. By default it pushes all the columns requested by the L<show> method onto the result array (this differs from the DBI). To specify which columns to include in the result use the 'Columns' attribute in %attr - see L<DBI-E<gt>selectcol_arrayref|DBI/"selectcol_arrayref">.

=cut

sub col_arrayref {
    my $me = shift;
    my $attr = shift;
    $me->_sql($me->sql, $me->_bind_params);
    my $sth = $me->sth->{Active} ? $me->rdbh->prepare($me->{sql}) : $me->{sth};
    return unless $sth and $sth->execute($me->_bind_params);
    my @columns = ($attr->{Columns}) ? @{$attr->{Columns}} : (1 .. $sth->{NUM_OF_FIELDS});
    my @ary;
    while (my $ref = $sth->fetch) {
        push @ary, @$ref;
    }
    \@ary;
}

sub _bind_params {
    my $me = shift;
    @{$me->{build_data}{Show_Bind}}, @{$me->{build_data}{From_Bind}}, @{$me->{build_data}{Where_Bind}}, @{$me->{build_data}{Group_Bind}}, @{$me->{build_data}{Order_Bind}};
}

sub fetch {
    my $me = shift;
    unless ($me->sth->{Active}) {
        $me->run or return undef;
    }

    # Detach the old record if there is still another reference to it
    my $row;
    if (defined $me->{Row} and SvREFCNT(${$me->{Row}}) > 1) {
        $me->{Row}->_detach;
        $row = $me->row;
    } else {
        $row = $me->row;
    }

    $$row->{hash} = $me->{hash};

    # Fetch and store the data then return the Row on success and undef on failure or no more rows
    ($$row->{array} = $me->{sth}->fetch) ? $me->{Row} : undef %$row;
}

sub row {
    my $me = shift;
    $me->sql; # Detach if needed
    $me->{Row} //= $me->{DBO}->row($me);
}

sub run {
    my $me = shift;
    my $row = $me->row;
    undef $$row->{array};
    undef %$row;

    my $rv = $me->_execute or return undef;
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
    $me->sql; # Ensure the Row_Count is cleared if needed
    unless (defined $me->{Row_Count}) {
        unless ($me->sth->{Executed}) {
            $me->run or return undef;
        }
        $me->{Row_Count} = $me->sth->rows;
        if ($me->{Row_Count} == -1) {
            # TODO: Handle DISTINCT and GROUP BY
            (my $sql = $me->sql) =~ s/\Q $me->{build_data}{show} FROM / COUNT(*) FROM /;
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

sub finish {
    my $me = shift;
    $me->{sth}->finish if $me->{sth} and $me->{sth}{Active};
}

sub sql {
    my $me = shift;
    $me->{sql} ||= $me->_build_sql;
}

sub _build_sql {
    my $me = shift;
    undef $me->{sth};
    undef $me->{hash};
    undef $me->{Row_Count};
    undef $me->{Found_Rows};
    if (defined $me->{Row}) {
        if (SvREFCNT(${$me->{Row}}) > 1) {
            $me->{Row}->_detach;
        } else {
            undef ${$me->{Row}}{array};
            undef %{$me->{Row}};
        }
    }

    $me->{sql} = $me->_build_sql_select($me->{build_data});
}

sub DESTROY {
    undef %{$_[0]};
}

1;