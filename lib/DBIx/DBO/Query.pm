package DBIx::DBO::Query;
use DBIx::DBO::Common;
use Devel::Peek 'SvREFCNT';

use strict;
use warnings;

=head1 NAME

DBIx::DBO::Query - An OO interface to SQL queries and results.  Encapsulates an entire query in an object.

=head1 SYNOPSIS

  # Create a Query object by JOINing 2 tables
  my $query = $dbo->query('my_table', 'my_other_table');

  # Get the Table objects from the query
  my ($table1, $table2) = $query->tables;

  # Add a JOIN ON clause
  $query->join_on($table1 ** 'login', '=', $table2 ** 'username');

  # Find our ancestors, and order by age (oldest first)
  $query->where('name', '=', 'Adam');
  $query->where('name', '=', 'Eve');
  $query->order_by({ COL => 'age', ORDER => 'DESC' });

  # New Query using a LEFT JOIN
  ($query, $table1) = $dbo->query('my_table');
  $table2 = $query->join_table('another_table', 'LEFT');
  $query->join_on($table1 ** 'parent_id', '=', $table2 ** 'child_id');

  # Find those not aged between 20 and 30.
  $query->where($table1 ** 'age', '<', 20, FORCE => 'OR'); # Force OR so that we get: (age < 20 OR age > 30)
  $query->where($table1 ** 'age', '>', 30, FORCE => 'OR'); # instead of the default: (age < 20 AND age > 30)

=head1 DESCRIPTION

A C<Query> object represents rows from a database (from one or more tables). This module makes it easy, not only to fetch and use the data in the returned rows, but also to modify the query to return a different result set.

=head1 SUBCLASSING

When subclassing C<DBIx::DBO::Query>, please note that C<Query> objects created with the L</new> method are blessed into a DBD driver specific module. For example if using MySQL, a new C<Query> object will be blessed into C<DBIx::DBO::Query::DBD::mysql> which inherits from C<DBIx::DBO::Query>.  However if objects are created from a subclass called C<MySubClass> the new object will be blessed into C<MySubClass::DBD::mysql> which will inherit from both C<MySubClass> and C<DBIx::DBO::Query::DBD::mysql>.

For this reason it's advisable that L<MRO::Compat|MRO::Compat> is installed if the perl version you are using is less than 5.9.5 as this module will try to ensure that the 'C3' method resolution order is used.

=head1 METHODS

=head3 C<new>

  DBIx::DBO::Query->new($dbo, $table1, ...);

Create a new C<Query> object from the tables specified.
In scalar context, just the C<Query> object will be returned.
In list context, the C<Query> object and L<DBIx::DBO::Table|DBIx::DBO::Table> objects will be returned for each table specified.

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $me = { DBO => shift, sql => undef };
    blessed $me->{DBO} and $me->{DBO}->isa('DBIx::DBO') or ouch 'Invalid DBO Object';
    ouch 'No table specified in new Query' unless @_;
    bless $me, $me->{DBO}->_create_dbd_class($class, __PACKAGE__);

    for my $table (@_) {
        $me->join_table($table);
    }
    $me->reset;
    return wantarray ? ($me, $me->tables) : $me;
}

=head3 C<reset>

  $query->reset;

Reset the query, start over with a clean slate.

B<NB>: This will not remove the JOINs or JOIN ON clauses.

=cut

sub reset {
    my $me = shift;
    $me->unwhere;
#    $me->{IsDistinct} = 0;
    $me->show;
    $me->group_by;
    $me->order_by;
    $me->limit;
}

=head3 C<tables>

Return a list of L<DBIx::DBO::Table|DBIx::DBO::Table> objects for this query.

=cut

sub tables {
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

=head3 C<show>

  $query->show(@columns);
  $query->show($table1 ** 'id', {FUNC => 'UCASE(?)', COL => 'name', AS => 'NAME'}, ...

Specify which columns to show as an array.  If the array is empty all columns will be shown.

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

=head3 C<join_table>

  $query->join_table($table, $join_type);
  $query->join_table([$schema, $table], $join_type);
  $query->join_table($table_object, $join_type);

Join a table onto the query, creating a L<DBIx::DBO::Table|DBIx::DBO::Table> object if needed.
This will perform a comma (", ") join unless $join_type is specified.

Valid join types are any accepted by the DB.  Eg: C<'JOIN'>, C<'LEFT'>, C<'RIGHT'>, C<undef> (for comma join), C<'INNER'>, C<'OUTER'>, ...

Returns the C<Table> object.

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
    push @{$me->{build_data}{Join_On}}, undef;
#    push @{$me->{Join_Bracket_Refs}}, [];
#    push @{$me->{Join_Brackets}}, [];
    undef $me->{sql};
    undef $me->{build_data}{from};
    return $tbl;
}

=head3 C<join_on>

  $query->join_on($table_object, $expression1, $operator, $expression2);
  $query->join_on($table2, $table1 ** 'id', '=', $table2 ** 'id');

Join tables on a specific WHERE clause.  The first argument is the table object being joined onto.
Then a JOIN ON condition follows, which uses the same arguments as L</where>.

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
    $me->_add_where($me->{build_data}{Join_On}[$i] ||= [], $op,
        $col1, $col1_func, $col1_opt, $col2, $col2_func, $col2_opt, @_);
}

#=head3 C<open_join_on_bracket>, C<close_join_on_bracket>
#
#  $query->open_join_on_bracket($table, 'OR');
#  $query->join_on(...
#  $query->close_join_on_bracket($table);
#
#Equivalent to L<open_bracket|/open_bracket__close_bracket>, but for the JOIN ON clause.
#The first argument is the table being joined onto.
#
#=cut
#
#sub open_join_on_bracket {
#    my $me = shift;
#    my $i = $me->_table_idx(shift) or ouch 'No such table object in the join';
#    $me->_open_bracket($me->{Join_Brackets}[$i], $me->{Join_Bracket_Refs}[$i], $me->{build_data}{Join_On}[$i] ||= [], @_);
#}
#
#sub close_join_on_bracket {
#    my $me = shift;
#    my $i = $me->_table_idx(shift) or ouch 'No such table object in the join';
#    $me->_close_bracket($me->{Join_Brackets}[$i], $me->{Join_Bracket_Refs}[$i]);
#}

=head3 C<where>

Restrict the query with the condition specified (WHERE clause).

  $query->where($expression1, $operator, $expression2);
  $query->where($table1 ** 'id', '=', $table2 ** 'id');

C<$operator> is one of: C<'=', '<', 'E<gt>', 'IN', 'NOT IN', 'BETWEEN', 'NOT BETWEEN', ...>

C<$expression>s can be any of the following:

=over 4

=item *
A scalar value: C<123> or C<'hello'> (or for C<$expression1> a column name: C<'id'>)

=item *
A scalar reference: C<\"22 * 3">  (These are passed unquoted in the SQL statement!)

=item *
An array reference: C<[1, 3, 5]>  (Used with C<IN> and C<BETWEEN> etc)

=item *
A Column object: C<$table ** 'id'> or C<$table->column('id')>

=item *
A hash reference: (Described below)

=back

For a more complex where expression it can be passed as a hash reference.
Possibly containing scalars, arrays or Column objects.

  $query->where('name', '=', { FUNC => 'COALESCE(?,?)', VAL => [$name, 'Unknown'] });
  $query->where('string', '=', { FUNC => "CONCAT('Mr. ',?)", COL => 'name' });

The keys to the hash in a complex expression are:

=over 4

=item *
C<VAL> => A scalar, scalar reference or an array reference.

=item *
C<COL> => The name of a column or a Column object.

=item *
C<AS> => An alias name.

=item *
C<FUNC> => A string to be inserted into the SQL, possibly containing "?" placeholders.

=item *
C<ORDER> => To order by a column (Used only in C<group_by> and C<order_by>).

=back

Multiple C<where> expressions are combined I<cleverly> using the preferred aggregator C<'AND'> (unless L<open_bracket|/open_bracket__close_bracket> was used to change this).  So that when you add where expressions to the query, they will be C<AND>ed together.  However some expressions that refer to the same caloumn will automatically be C<OR>ed instead where this makes sense, currently: C<'='>, C<'IS NULL'>, C<E<lt>=E<gt>>, C<IN> and C<'BETWEEN'>.  Similarly, when the preferred aggregator is C<'OR'> the following operators will be C<AND>ed together: C<'!='>, C<'IS NOT NULL'>, C<E<lt>E<gt>>, C<NOT IN> and C<'NOT BETWEEN'>.

  $query->where('id', '=', 5);
  $query->where('name', '=', 'Bob');
  $query->where('id', '=', 7);
  $query->where(...
  # Produces: WHERE ("id" = 5 OR "id" = 7) AND "name" = 'Bob' AND ...

=cut

sub where {
    my $me = shift;

    # If the $fld is just a scalar use it as a column name not a value
    my ($fld, $fld_func, $fld_opt) = $me->_parse_col_val(shift);
    my $op = shift;
    my ($val, $val_func, $val_opt) = $me->_parse_val(shift, Check => 'Auto');

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
    my $ref = $me->{build_data}{Where_Data} ||= [];
    $ref = $ref->[$_] for (@{$me->{Where_Bracket_Refs}});

    $me->_add_where($ref, $op, $fld, $fld_func, $fld_opt, $val, $val_func, $val_opt, @_);
}

=head3 C<unwhere>

  $query->unwhere();
  $query->unwhere($column);

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
            $ref = $ref->[$_] for (@{$me->{Where_Bracket_Refs}});

            for (my $i = $#$ref; $i >= 0; $i--) {
                # Remove this Where piece if there is no FUNC and it refers to this column only
                splice @{$ref}, $i, 1
                    if !defined $ref->[$i][2] and @{$ref->[$i][1]} == 1 and $ref->[$i][1][0] == $col;
            }
        }
    } else {
        delete $me->{build_data}{Where_Data};
        $me->{Where_Bracket_Refs} = [];
        $me->{Where_Brackets} = [];
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
#  $fld_func is undef or a scalar of the form '? AND ?' or 'POSITION(? IN ?)'
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
            # Add to previous 'IN' and 'NOT IN' Where expressions
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
    my %c = (Check => 'Column', @_);
    return $me->_parse_val($col, %c) if ref $col;
    return [ $me->_parse_col($col, $c{Aliases}) ];
}

=head3 C<open_bracket>, C<close_bracket>

  $query->open_bracket('OR');
  $query->where( ...
  $query->where( ...
  $query->close_bracket;

Used to group C<where> expressions together in parenthesis using either C<'AND'> or C<'OR'> as the preferred aggregator.
All the C<where> calls made between C<open_bracket> and C<close_bracket> will be inside the parenthesis.

Without any parenthesis C<'AND'> is the preferred aggregator.

=cut

sub open_bracket {
    my $me = shift;
    $me->_open_bracket($me->{Where_Brackets}, $me->{Where_Bracket_Refs}, $me->{build_data}{Where_Data} ||= [], @_);
}

sub _open_bracket {
    my ($me, $brackets, $bracket_refs, $ref, $ag) = @_;
    ouch 'Invalid argument MUST be AND or OR' if !$ag or $ag !~ /^(AND|OR)$/;
    my $last = @$brackets ? $brackets->[-1] : 'AND';
    if ($ag ne $last) {
        # Find the current data reference
        $ref = $ref->[$_] for @$bracket_refs;

        push @$ref, [];
        push @$bracket_refs, $#$ref;
    }
    push @$brackets, $ag;
}

sub close_bracket {
    my $me = shift;
    $me->_close_bracket($me->{Where_Brackets}, $me->{Where_Bracket_Refs});
}

sub _close_bracket {
    my ($me, $brackets, $bracket_refs) = @_;
    my $ag = pop @{$brackets} or ouch "Can't close bracket with no open bracket!";
    my $last = @$brackets ? $brackets->[-1] : 'AND';
    pop @$bracket_refs if $last ne $ag;
    return $ag;
}

=head3 C<group_by>

  $query->group_by('column', ...);
  $query->group_by($table ** 'column', ...);
  $query->group_by({ COL => $table ** 'column', ORDER => 'DESC' }, ...);

Group the results by the column(s) listed.  This will replace the GROUP BY clause.
To remove the GROUP BY clause simply call C<group_by> without any columns.

=cut

sub group_by {
    my $me = shift;
    undef $me->{sql};
    undef $me->{build_data}{group};
    undef @{$me->{build_data}{GroupBy}};
    for my $col (@_) {
        my @group = $me->_parse_col_val($col, Aliases => 1);
        push @{$me->{build_data}{GroupBy}}, \@group;
    }
}

=head3 C<order_by>

  $query->order_by('column', ...);
  $query->order_by($table ** 'column', ...);
  $query->order_by({ COL => $table ** 'column', ORDER => 'DESC' }, ...);

Order the results by the column(s) listed.  This will replace the ORDER BY clause.
To remove the ORDER BY clause simply call C<order_by> without any columns.

=cut

sub order_by {
    my $me = shift;
    undef $me->{sql};
    undef $me->{build_data}{order};
    undef @{$me->{build_data}{OrderBy}};
    for my $col (@_) {
        my @order = $me->_parse_col_val($col, Aliases => 1);
        push @{$me->{build_data}{OrderBy}}, \@order;
    }
}

=head3 C<limit>

  $query->limit;
  $query->limit($rows);
  $query->limit($rows, $offset);

Limit the maximum number of rows returned to C<$rows>, optionally skipping the first C<$offset> rows.
When called without arguments or if C<$rows> is undefined, the limit is removed.

=cut

sub limit {
    my ($me, $rows, $offset) = @_;
    undef $me->{sql};
    undef $me->{build_data}{limit};
    return undef $me->{build_data}{LimitOffset} unless defined $rows;
    /^\d+$/ or ouch "Invalid argument '$_' in limit" for grep defined, $rows, $offset;
    @{$me->{build_data}{LimitOffset}} = ($rows, $offset);
}

=head3 C<arrayref>

  $query->arrayref;
  $query->arrayref(\%attr);

Run the query using L<DBI-E<gt>selectall_arrayref|DBI/"selectall_arrayref"> which returns the result as an arrayref.
You can specify a slice by including a 'Slice' or 'Columns' attribute in C<%attr> - See L<DBI-E<gt>selectall_arrayref|DBI/"selectall_arrayref">.

=cut

sub arrayref {
    my $me = shift;
    my $attr = shift;
    $me->_sql($me->sql, $me->_bind_params_select($me->{build_data}));
    $me->rdbh->selectall_arrayref($me->{sql}, $attr, $me->_bind_params_select($me->{build_data}));
}

=head3 C<hashref>

  $query->hashref($key_field);
  $query->hashref($key_field, \%attr);

Run the query using L<DBI-E<gt>selectall_hashref|DBI/"selectall_hashref"> which returns the result as an hashref.
C<$key_field> defines which column, or columns, are used as keys in the returned hash.

=cut

sub hashref {
    my $me = shift;
    my $key = shift;
    my $attr = shift;
    $me->_sql($me->sql, $me->_bind_params_select($me->{build_data}));
    $me->rdbh->selectall_hashref($me->{sql}, $key, $attr, $me->_bind_params_select($me->{build_data}));
}

=head3 C<col_arrayref>

  $query->col_arrayref;
  $query->col_arrayref(\%attr);

Run the query using L<DBI-E<gt>selectcol_arrayref|DBI/"selectcol_arrayref"> which returns the result as an arrayref of the values of each row in one array.  By default it pushes all the columns requested by the L</show> method onto the result array (this differs from the C<DBI>).  Or to specify which columns to include in the result use the 'Columns' attribute in C<%attr> - see L<DBI-E<gt>selectcol_arrayref|DBI/"selectcol_arrayref">.

=cut

sub col_arrayref {
    my $me = shift;
    my $attr = shift;
    $me->_sql($me->sql, $me->_bind_params_select($me->{build_data}));
    my $sth = $me->rdbh->prepare($me->{sql}, $attr) or return;
    unless (defined $attr->{Columns}) {
        # Some drivers don't provide $sth->{NUM_OF_FIELDS} until after execute is called
        if ($sth->{NUM_OF_FIELDS}) {
            $attr->{Columns} = [1 .. $sth->{NUM_OF_FIELDS}];
        } else {
            $sth->execute($me->_bind_params_select($me->{build_data})) or return;
            my @col;
            if (my $max = $attr->{MaxRows}) {
                push @col, @$_ while 0 < $max-- and $_ = $sth->fetch;
            } else {
                push @col, @$_ while $_ = $sth->fetch;
            }
            return \@col;
        }
    }
    return $me->rdbh->selectcol_arrayref($sth, $attr, $me->_bind_params_select($me->{build_data}));
}

=head3 C<fetch>

  my $row = $query->fetch;

Fetch the next row from the query.  This will run/rerun the query if needed.

Returns a L<DBIx::DBO::Row|DBIx::DBO::Row> object or undefined if there are no more rows.

=cut

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

=head3 C<row>

  my $row = $query->row;

Returns the L<DBIx::DBO::Row|DBIx::DBO::Row> object for the current row from the query or an empty L<DBIx::DBO::Row|DBIx::DBO::Row> object if there is no current row.

=cut

sub row {
    my $me = shift;
    $me->sql; # Detach if needed
    $me->{Row} ||= $me->{DBO}->row($me);
}

=head3 C<run>

  $query->run;

Run/rerun the query.
This is called automatically before fetching the first row.

=cut

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
    $me->_sql($me->sql, $me->_bind_params_select($me->{build_data}));
    $me->sth or return;
    $me->{sth}->execute($me->_bind_params_select($me->{build_data}));
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

=head3 C<rows>

  my $row_count = $query->rows;

Count the number of rows returned.
Returns undefined if there is an error or the number is unknown.

=cut

sub rows {
    my $me = shift;
    $me->sql; # Ensure the Row_Count is cleared if needed
    unless (defined $me->{Row_Count}) {
        unless ($me->sth->{Executed}) {
            $me->run or return undef;
        }
        $me->{Row_Count} = $me->sth->rows;
        $me->{Row_Count} = $me->count_rows if $me->{Row_Count} == -1;
    }
    $me->{Row_Count};
}

=head3 C<count_rows>

  my $row_count = $query->count_rows;

Count the number of rows that would be returned.
Returns undefined if there is an error.

=cut

sub count_rows {
    my $me = shift;
    my $old_fr = $me->{Config}{CalcFoundRows};
    $me->{Config}{CalcFoundRows} = 0;
    my $old_sb = delete $me->{build_data}{Show_Bind};
    $me->{build_data}{show} = '1';

    my $sql = 'SELECT COUNT(*) FROM ('.$me->_build_sql_select($me->{build_data}).') t';
    $me->_sql($sql, $me->_bind_params_select($me->{build_data}));
    my ($count) = $me->rdbh->selectrow_array($sql, undef, $me->_bind_params_select($me->{build_data}));

    $me->{Config}{CalcFoundRows} = $old_fr if defined $old_fr;
    $me->{build_data}{Show_Bind} = $old_sb if $old_sb;
    undef $me->{build_data}{show};
    return $count;
}

=head3 C<found_rows>

  $query->config(CalcFoundRows => 1); # Only applicable to MySQL
  my $total_rows = $query->found_rows;

Return the number of rows that would have been returned if there was no limit clause.  Before runnning the query the C<CalcFoundRows> config option can be enabled for improved performance on supported databases.

Returns undefined if there is an error or is unable to determine the number of found rows.

=cut

sub found_rows {
    my $me = shift;
    if (not defined $me->{Found_Rows}) {
        $me->{build_data}{limit} = '';
        $me->{Found_Rows} = $me->count_rows;
        undef $me->{build_data}{limit};
    }
    $me->{Found_Rows};
}

=head3 C<sql>

  my $sql = $query->sql;

Returns the SQL query statement string.

=cut

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

            $me->{sql} = $me->_build_sql_select($me->{build_data});
            $me->{Row}->_copy_build_data;
            return $me->{sql};
        }
    }

    $me->{sql} = $me->_build_sql_select($me->{build_data});
}

=head3 C<sth>

  my $sth = $query->sth;

Reutrns the C<DBI> statement handle from the query.
This will run/rerun the query if needed.

=cut

sub sth {
    my $me = shift;
    # Ensure the sql is rebuilt if needed
    my $sql = $me->sql;
    $me->{sth} ||= $me->rdbh->prepare($sql);
}

=head3 C<finish>

  $query->finish;

Calls L<DBI-E<gt>finish|DBI/"finish"> on the statement handle, if it's active.

=cut

sub finish {
    my $me = shift;
    $me->{sth}->finish if $me->{sth} and $me->{sth}{Active};
}

=head2 Common Methods

These methods are accessible from all DBIx::DBO* objects.

=head3 C<dbh>

The I<read-write> C<DBI> handle.

=head3 C<rdbh>

The I<read-only> C<DBI> handle, or if there is no I<read-only> connection, the I<read-write> C<DBI> handle.

=head3 C<do>

  $dbo->do($statement)         or die $dbo->dbh->errstr;
  $dbo->do($statement, \%attr) or die $dbo->dbh->errstr;
  $dbo->do($statement, \%attr, @bind_values) or die ...

This provides access to L<DBI-E<gt>do|DBI/"do"> method.  It defaults to using the I<read-write> C<DBI> handle.

=head3 C<config>

  $query_setting = $dbo->config($option);
  $dbo->config($option => $query_setting);

Get or set this C<Query> object's config settings.  When setting an option, the previous value is returned.  When getting an option's value, if the value is undefined, the L<DBIx::DBO|DBIx::DBO>'s value is returned.

See L<DBIx::DBO/available_config_options>.

=cut

sub config {
    my $me = shift;
    my $opt = shift;
    return $me->_set_config($me->{Config} ||= {}, $opt, shift) if @_;
    return defined $me->{Config}{$opt} ? $me->{Config}{$opt} : $me->{DBO}->config($opt);
}

sub DESTROY {
    undef %{$_[0]};
}

1;

__END__

=head1 TODO LIST

=over

=item *

Better explanation of how to construct complex queries.  This module is currently still in development (including the documentation), but I will be adding to/completing it in the near future.

=item *

Add C<DISTINCT> and C<COLLATE> functionality.

=item *

Possibly add C<open_join_on_bracket> for adding parentheses in the JOIN ON clause, but I'm not sure if this would ever be useful.

=back

=head1 SEE ALSO

L<DBIx::DBO>


=cut

