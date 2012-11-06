package DBIx::DBO::Query;

use strict;
use warnings;
use Devel::Peek 'SvREFCNT';
use Carp 'croak';

BEGIN {
    if ($] < 5.008_009) {
        require XSLoader;
        XSLoader::load(__PACKAGE__, $DBIx::DBO::VERSION);
    } else {
        require Hash::Util;
        *_hv_store = \&Hash::Util::hv_store;
    }
}

sub _table_class { $_[0]{DBO}->_table_class }
sub _row_class { $_[0]{DBO}->_row_class }

=head1 NAME

DBIx::DBO::Query - An OO interface to SQL queries and results.  Encapsulates an entire query in an object.

=head1 SYNOPSIS

  # Create a Query object by JOINing 2 tables
  my $query = $dbo->query('my_table', 'my_other_table');
  
  # Get the Table objects from the query
  my($table1, $table2) = $query->tables;
  
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

=head1 METHODS

=head3 C<new>

  DBIx::DBO::Query->new($dbo, $table1, ...);

Create a new C<Query> object from the tables specified.
In scalar context, just the C<Query> object will be returned.
In list context, the C<Query> object and L<Table|DBIx::DBO::Table> objects will be returned for each table specified.

=cut

sub new {
    my $proto = shift;
    UNIVERSAL::isa($_[0], 'DBIx::DBO') or croak 'Invalid DBO Object';
    my $class = ref($proto) || $proto;
    $class->_init(@_);
}

sub _init {
    my $class = shift;
    my $me = { DBO => shift, sql => undef, Columns => [] };
    croak 'No table specified in new Query' unless @_;
    bless $me, $class;

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
    $me->finish;
    $me->unwhere;
    $me->distinct(0);
    $me->show;
    $me->group_by;
    $me->order_by;
    $me->unhaving;
    $me->limit;
    # FIXME: Should we be deleting this?
    delete $me->{Config};
}

=head3 C<tables>

Return a list of L<Table|DBIx::DBO::Table> objects for this query.

=cut

sub tables {
    @{$_[0]->{Tables}};
}

sub _table_idx {
    my($me, $tbl) = @_;
    for my $i (0 .. $#{$me->{Tables}}) {
        return $i if $tbl == $me->{Tables}[$i];
    }
    return undef;
}

sub _table_alias {
    my($me, $tbl) = @_;
    return undef if $me == $tbl; # This means it's checking for an aliased column
    my $i = $me->_table_idx($tbl);
    croak 'The table is not in this query' unless defined $i;
    # Don't use aliases, when there's only 1 table
    @{$me->{Tables}} > 1 ? 't'.($i + 1) : ();
}

=head3 C<columns>

Return a list of column names.

=cut

sub columns {
    @{$_[0]->{Columns}};
}

=head3 C<column>

  $query->column($alias_or_column_name);

Returns a reference to a column for use with other methods.

=cut

sub column {
    my($me, $col, $_check_aliases) = @_;
    $_check_aliases = $me->{DBO}{dbd_class}->_alias_preference($me, 'column') unless defined $_check_aliases;
    my $column;
    return $column if $_check_aliases == 1 and $column = $me->_check_alias($col);
    for my $tbl ($me->tables) {
        return $tbl->column($col) if exists $tbl->{Column_Idx}{$col};
    }
    return $column if $_check_aliases == 2 and $column = $me->_check_alias($col);
    croak 'No such column'.($_check_aliases ? '/alias' : '').': '.$me->{DBO}{dbd_class}->_qi($me, $col);
}

sub _check_alias {
    my($me, $col) = @_;
    for my $fld (@{$me->{build_data}{Showing}}) {
        return $me->{Column}{$col} ||= bless [$me, $col], 'DBIx::DBO::Column'
            if ref($fld) eq 'ARRAY' and exists $fld->[2]{AS} and $col eq $fld->[2]{AS};
    }
}

=head3 C<show>

  $query->show(@columns);
  $query->show($table1, {COL => $table2 ** 'name', AS => 'name2'});
  $query->show($table1 ** 'id', {FUNC => 'UCASE(?)', COL => 'name', AS => 'alias'}, ...

Specify which columns to show as an array.  If the array is empty all columns will be shown.
If you use a Table object, all the columns from that table will be shown.

=cut

# TODO: Keep track of all aliases in use and die if a used alias is removed
sub show {
    my $me = shift;
    undef $me->{sql};
    undef $me->{build_data}{from};
    undef $me->{build_data}{show};
    undef @{$me->{build_data}{Showing}};
    for my $fld (@_) {
        if (UNIVERSAL::isa($fld, 'DBIx::DBO::Table')) {
            croak 'Invalid table to show' unless defined $me->_table_idx($fld);
            push @{$me->{build_data}{Showing}}, $fld;
            next;
        }
        # If the $fld is just a scalar use it as a column name not a value
        push @{$me->{build_data}{Showing}}, [ $me->_parse_col_val($fld, Aliases => 0) ];
    }
}

=head3 C<distinct>

  $query->distinct(1);
  my $is_distinct = $query->distinct();

Takes a boolean argument to add or remove the DISTINCT clause for the returned rows.
Returns the previous setting.

=cut

sub distinct {
    my $me = shift;
    undef $me->{sql};
    undef $me->{build_data}{show};
    my $distinct = $me->{build_data}{Show_Distinct};
    $me->{build_data}{Show_Distinct} = shift() ? 1 : undef if @_;
    return $distinct;
}

=head3 C<join_table>

  $query->join_table($table, $join_type);
  $query->join_table([$schema, $table], $join_type);
  $query->join_table($table_object, $join_type);

Join a table onto the query, creating a L<Table|DBIx::DBO::Table> object if needed.
This will perform a comma (", ") join unless $join_type is specified.

Valid join types are any accepted by the DB.  Eg: C<'JOIN'>, C<'LEFT'>, C<'RIGHT'>, C<undef> (for comma join), C<'INNER'>, C<'OUTER'>, ...

Returns the C<Table> object.

=cut

sub join_table {
    my($me, $tbl, $type) = @_;
    if (UNIVERSAL::isa($tbl, 'DBIx::DBO::Table')) {
        croak 'This table is already in this query' if $me->_table_idx($tbl);
    } else {
        $tbl = $me->_table_class->new($me->{DBO}, $tbl);
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
    push @{$me->{Join_Bracket_Refs}}, [];
    push @{$me->{Join_Brackets}}, [];
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
    my $i = $me->_table_idx($t2) or croak 'Invalid table object to join onto';

    my($col1, $col1_func, $col1_opt) = $me->_parse_col_val(shift);
    my $op = shift;
    my($col2, $col2_func, $col2_opt) = $me->_parse_col_val(shift);

    # Validate the fields
    $me->_validate_where_fields(@$col1, @$col2);

    # Force a new search
    undef $me->{sql};
    undef $me->{build_data}{from};

    # Find the current Join_On reference
    my $ref = $me->{build_data}{Join_On}[$i] ||= [];
    $ref = $ref->[$_] for (@{$me->{Join_Bracket_Refs}[$i]});

    $me->{build_data}{Join}[$i] = ' JOIN ' if $me->{build_data}{Join}[$i] eq ', ';
    $me->_add_where($ref, $op, $col1, $col1_func, $col1_opt, $col2, $col2_func, $col2_opt, @_);
}

=head3 C<open_join_on_bracket>, C<close_join_on_bracket>

  $query->open_join_on_bracket($table, 'OR');
  $query->join_on(...
  $query->close_join_on_bracket($table);

Equivalent to L<open_bracket|/open_bracket__close_bracket>, but for the JOIN ON clause.
The first argument is the table being joined onto.

=cut

sub open_join_on_bracket {
    my $me = shift;
    my $tbl = shift or croak 'Invalid table object for join on clause';
    my $i = $me->_table_idx($tbl) or croak 'No such table object in the join';
    $me->_open_bracket($me->{Join_Brackets}[$i], $me->{Join_Bracket_Refs}[$i], $me->{build_data}{Join_On}[$i] ||= [], @_);
}

sub close_join_on_bracket {
    my $me = shift;
    my $tbl = shift or croak 'Invalid table object for join on clause';
    my $i = $me->_table_idx($tbl) or croak 'No such table object in the join';
    $me->_close_bracket($me->{Join_Brackets}[$i], $me->{Join_Bracket_Refs}[$i]);
}

=head3 C<where>

Restrict the query with the condition specified (WHERE clause).

  $query->where($expression1, $operator, $expression2);

C<$operator> is one of: C<'=', '<E<gt>', '<', 'E<gt>', 'IN', 'NOT IN', 'LIKE', 'NOT LIKE', 'BETWEEN', 'NOT BETWEEN', ...>

C<$expression>s can be any of the following:

=over 4

=item *

A scalar value: C<123> or C<'hello'> (or for C<$expression1> a column name: C<'id'>)

  $query->where('name', '<>', 'John');

=item *

A scalar reference: C<\"22 * 3">  (These are passed unquoted in the SQL statement!)

  $query->where(\'CONCAT(id, name)', '=', \'"22John"');

=item *

An array reference: C<[1, 3, 5]>  (Used with C<IN> and C<BETWEEN> etc)

  $query->where('id', 'NOT IN', [21, 22, 25, 39]);

=item *

A Column object: C<$table ** 'id'> or C<$table-E<gt>column('id')>

  $query->where($table1 ** 'id', '=', $table2 ** 'id');

=item *

A hash reference: see L</Complex_expressions>

=back

Multiple C<where> expressions are combined I<cleverly> using the preferred aggregator C<'AND'> (unless L<open_bracket|/open_bracket__close_bracket> was used to change this).  So that when you add where expressions to the query, they will be C<AND>ed together.  However some expressions that refer to the same column will automatically be C<OR>ed instead where this makes sense, currently: C<'='>, C<'IS NULL'>, C<'E<lt>=E<gt>'>, C<'IN'> and C<'BETWEEN'>.  Similarly, when the preferred aggregator is C<'OR'> the following operators will be C<AND>ed together: C<'!='>, C<'IS NOT NULL'>, C<'E<lt>E<gt>'>, C<'NOT IN'> and C<'NOT BETWEEN'>.

  $query->where('id', '=', 5);
  $query->where('name', '=', 'Bob');
  $query->where('id', '=', 7);
  $query->where(...
  # Produces: WHERE ("id" = 5 OR "id" = 7) AND "name" = 'Bob' AND ...

=cut

sub where {
    my $me = shift;

    # If the $fld is just a scalar use it as a column name not a value
    my($fld, $fld_func, $fld_opt) = $me->_parse_col_val(shift);
    my $op = shift;
    my($val, $val_func, $val_opt) = $me->{DBO}{dbd_class}->_parse_val($me, shift, Check => 'Auto');

    # Validate the fields
    $me->_validate_where_fields(@$fld, @$val);

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

Removes all previously added L</where> restrictions for a column.
If no column is provided, the I<whole> WHERE clause is removed.

=cut

sub unwhere {
    my $me = shift;
    $me->_del_where('Where', @_);
}

sub _validate_where_fields {
    my $me = shift;
    for my $f (@_) {
        if (UNIVERSAL::isa($f, 'DBIx::DBO::Column')) {
            $me->{DBO}{dbd_class}->_valid_col($me, $f);
        } elsif (my $type = ref $f) {
            croak 'Invalid value type: '.$type if $type ne 'SCALAR';
        }
    }
}

sub _del_where {
    my $me = shift;
    my $clause = shift;

    if (@_) {
        require Data::Dumper;
        my($fld, $fld_func, $fld_opt) = $me->_parse_col_val(shift);
        # TODO: Validate the fields?

        return unless exists $me->{build_data}{$clause.'_Data'};
        # Find the current Where_Data reference
        my $ref = $me->{build_data}{$clause.'_Data'};
        $ref = $ref->[$_] for (@{$me->{$clause.'_Bracket_Refs'}});

        local $Data::Dumper::Indent = 0;
        local $Data::Dumper::Maxdepth = 2;
        my @match = grep {
            Data::Dumper::Dumper($fld, $fld_func, $fld_opt) eq Data::Dumper::Dumper(@{$ref->[$_]}[1,2,3])
        } 0 .. $#$ref;

        if (@_) {
            my $op = shift;
            my($val, $val_func, $val_opt) = $me->{DBO}{dbd_class}->_parse_val($me, shift, Check => 'Auto');

            @match = grep {
                Data::Dumper::Dumper($op, $val, $val_func, $val_opt) eq Data::Dumper::Dumper(@{$ref->[$_]}[0,4,5,6])
            } @match;
        }
        splice @$ref, $_, 1 for reverse @match;
    } else {
        delete $me->{build_data}{$clause.'_Data'};
        $me->{$clause.'_Bracket_Refs'} = [];
        $me->{$clause.'_Brackets'} = [];
    }
    # This forces a new search
    undef $me->{sql};
    undef $me->{build_data}{lc $clause};
}

##
# This will add an arrayref to the $ref given.
# The arrayref will contain 8 values:
#  $op, $fld, $fld_func, $fld_opt, $val, $val_func, $val_opt, $force
#  $op is the operator (those supported differ by DBD)
#  $fld_func is undef or a scalar of the form '? AND ?' or 'POSITION(? IN ?)'
#  $fld is an arrayref of columns/values for use with $fld_func
#  $val_func is similar to $fld_func
#  $val is an arrayref of values for use with $val_func
#  $force is one of undef / 'AND' / 'OR' which if defined, overrides the default aggregator
##
sub _add_where {
    my $me = shift;
    my($ref, $op, $fld, $fld_func, $fld_opt, $val, $val_func, $val_opt, %opt) = @_;

    croak 'Invalid option, FORCE must be AND or OR'
        if defined $opt{FORCE} and $opt{FORCE} ne 'AND' and $opt{FORCE} ne 'OR';

    # Deal with NULL values
    $op = '<>' if $op eq '!='; # Use the valid SQL op
    if (@$val == 1 and !defined $val->[0] and !defined $val_func) {
        if ($op eq '=') { $op = 'IS'; $val_func = 'NULL'; delete $val->[0]; }
        elsif ($op eq '<>') { $op = 'IS NOT'; $val_func = 'NULL'; delete $val->[0]; }
    }

    # Deal with array values: BETWEEN & IN
    unless (defined $val_func) {
        if ($op eq 'BETWEEN' or $op eq 'NOT BETWEEN') {
            croak 'Invalid value argument, BETWEEN requires 2 values'
                if ref $val ne 'ARRAY' or @$val != 2;
            $val_func = $me->{DBO}{dbd_class}->PLACEHOLDER.' AND '.$me->{DBO}{dbd_class}->PLACEHOLDER;
        } elsif ($op eq 'IN' or $op eq 'NOT IN') {
            if (ref $val eq 'ARRAY') {
                croak 'Invalid value argument, IN requires at least 1 value' if @$val == 0;
            } else {
                $val = [ $val ];
            }
            # Add to previous 'IN' and 'NOT IN' Where expressions
            my $op_ag = $me->{DBO}{dbd_class}->_op_ag($op);
            unless ($opt{FORCE} and $opt{FORCE} ne $op_ag) {
                for my $lim (grep $$_[0] eq $op, @$ref) {
                    # $fld and $$lim[1] are always ARRAY refs
                    next if "@{$$lim[1]}" ne "@$fld";
                    last if $$lim[7] and $$lim[7] ne $op_ag;
                    last if $$lim[5] ne '('.join(',', ($me->{DBO}{dbd_class}->PLACEHOLDER) x @{$$lim[4]}).')';
                    push @{$$lim[4]}, @$val;
                    $$lim[5] = '('.join(',', ($me->{DBO}{dbd_class}->PLACEHOLDER) x @{$$lim[4]}).')';
                    return;
                }
            }
            $val_func = '('.join(',', ($me->{DBO}{dbd_class}->PLACEHOLDER) x @$val).')';
        } elsif (@$val != 1) {
            # Check that there is only 1 placeholder
            croak 'Wrong number of fields/values, called with '.@$val.' while needing 1';
        }
    }

    push @{$ref}, [ $op, $fld, $fld_func, $fld_opt, $val, $val_func, $val_opt, $opt{FORCE} ];
}

sub _parse_col_val {
    my $me = shift;
    my $col = shift;
    my %c = (Check => 'Column', @_);
    unless (defined $c{Aliases}) {
        (my $method = (caller(1))[3]) =~ s/.*:://;
        $c{Aliases} = $me->{DBO}{dbd_class}->_alias_preference($me, $method);
    }
    return $me->{DBO}{dbd_class}->_parse_val($me, $col, %c) if ref $col;
    return [ $me->{DBO}{dbd_class}->_parse_col($me, $col, $c{Aliases}) ];
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
    my($me, $brackets, $bracket_refs, $ref, $ag) = @_;
    croak 'Invalid argument MUST be AND or OR' if !$ag or $ag !~ /^(AND|OR)$/;
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
    my($me, $brackets, $bracket_refs) = @_;
    my $ag = pop @{$brackets} or croak "Can't close bracket with no open bracket!";
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
        my @group = $me->_parse_col_val($col);
        push @{$me->{build_data}{GroupBy}}, \@group;
    }
}

=head3 C<having>

Restrict the query with the condition specified (HAVING clause).  This takes the same arguments as L</where>.

  $query->having($expression1, $operator, $expression2);

=cut

sub having {
    my $me = shift;

    # If the $fld is just a scalar use it as a column name not a value
    my($fld, $fld_func, $fld_opt) = $me->_parse_col_val(shift);
    my $op = shift;
    my($val, $val_func, $val_opt) = $me->{DBO}{dbd_class}->_parse_val($me, shift, Check => 'Auto');

    # Validate the fields
    $me->_validate_where_fields(@$fld, @$val);

    # Force a new search
    undef $me->{sql};
    undef $me->{build_data}{having};

    # Find the current Having_Data reference
    my $ref = $me->{build_data}{Having_Data} ||= [];
    $ref = $ref->[$_] for (@{$me->{Having_Bracket_Refs}});

    $me->_add_where($ref, $op, $fld, $fld_func, $fld_opt, $val, $val_func, $val_opt, @_);
}

=head3 C<unhaving>

  $query->unhaving();
  $query->unhaving($column);

Removes all previously added L</having> restrictions for a column.
If no column is provided, the I<whole> HAVING clause is removed.

=cut

sub unhaving {
    my $me = shift;
    $me->_del_where('Having', @_);
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
        my @order = $me->_parse_col_val($col);
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
    my($me, $rows, $offset) = @_;
    undef $me->{sql};
    undef $me->{build_data}{limit};
    return undef $me->{build_data}{LimitOffset} unless defined $rows;
    /^\d+$/ or croak "Invalid argument '$_' in limit" for grep defined, $rows, $offset;
    @{$me->{build_data}{LimitOffset}} = ($rows, $offset);
}

=head3 C<arrayref>

  $query->arrayref;
  $query->arrayref(\%attr);

Run the query using L<DBI-E<gt>selectall_arrayref|DBI/"selectall_arrayref"> which returns the result as an arrayref.
You can specify a slice by including a 'Slice' or 'Columns' attribute in C<%attr> - See L<DBI-E<gt>selectall_arrayref|DBI/"selectall_arrayref">.

=cut

sub arrayref {
    my($me, $attr) = @_;
    $me->{DBO}{dbd_class}->_selectall_arrayref($me, $me->sql, $attr,
        $me->{DBO}{dbd_class}->_bind_params_select($me, $me->{build_data}));
}

=head3 C<hashref>

  $query->hashref($key_field);
  $query->hashref($key_field, \%attr);

Run the query using L<DBI-E<gt>selectall_hashref|DBI/"selectall_hashref"> which returns the result as an hashref.
C<$key_field> defines which column, or columns, are used as keys in the returned hash.

=cut

sub hashref {
    my($me, $key, $attr) = @_;
    $me->{DBO}{dbd_class}->_selectall_hashref($me, $me->sql, $key, $attr,
        $me->{DBO}{dbd_class}->_bind_params_select($me, $me->{build_data}));
}

=head3 C<col_arrayref>

  $query->col_arrayref;
  $query->col_arrayref(\%attr);

Run the query using L<DBI-E<gt>selectcol_arrayref|DBI/"selectcol_arrayref"> which returns the result as an arrayref of the values of each row in one array.  By default it pushes all the columns requested by the L</show> method onto the result array (this differs from the C<DBI>).  Or to specify which columns to include in the result use the 'Columns' attribute in C<%attr> - see L<DBI-E<gt>selectcol_arrayref|DBI/"selectcol_arrayref">.

=cut

sub col_arrayref {
    my($me, $attr) = @_;
    my($sql, @bind) = ($me->sql, $me->{DBO}{dbd_class}->_bind_params_select($me, $me->{build_data}));
    $me->{DBO}{dbd_class}->_sql($me, $sql, @bind);
    my $sth = $me->rdbh->prepare($sql, $attr) or return;
    unless (defined $attr->{Columns}) {
        # Some drivers don't provide $sth->{NUM_OF_FIELDS} until after execute is called
        if ($sth->{NUM_OF_FIELDS}) {
            $attr->{Columns} = [1 .. $sth->{NUM_OF_FIELDS}];
        } else {
            $sth->execute(@bind) or return;
            my @col;
            if (my $max = $attr->{MaxRows}) {
                push @col, @$_ while 0 < $max-- and $_ = $sth->fetch;
            } else {
                push @col, @$_ while $_ = $sth->fetch;
            }
            return \@col;
        }
    }
    return $me->rdbh->selectcol_arrayref($sth, $attr, @bind);
}

=head3 C<fetch>

  my $row = $query->fetch;

Fetch the next row from the query.  This will run/rerun the query if needed.

Returns a L<Row|DBIx::DBO::Row> object or undefined if there are no more rows.

=cut

sub fetch {
    my $me = $_[0];
    # Prepare and/or execute the query if needed
    $me->_sth and ($me->{Active} or $me->run)
        or croak $me->rdbh->errstr;
    # Detach the old row if there is still another reference to it
    if (defined $me->{Row} and SvREFCNT(${$me->{Row}}) > 1) {
        $me->{Row}->_detach;
    }

    my $row = $me->row;
    if (exists $me->{cache}) {
        if ($me->{cache}{idx} < @{$me->{cache}{data}}) {
            @{$me->{cache}{array}}[0..$#{$me->{cache}{array}}] = @{$me->{cache}{data}[$me->{cache}{idx}++]};
            $$row->{array} = $me->{cache}{array};
            $$row->{hash} = $me->{hash};
            return $row;
        }
        undef $$row->{array};
        $me->{cache}{idx} = 0;
    } else {
        # Fetch and store the data then return the Row on success and undef on failure or no more rows
        if ($$row->{array} = $me->{sth}->fetch) {
            $$row->{hash} = $me->{hash};
            return $row;
        }
        $me->{Active} = 0;
    }
    $$row->{hash} = {};
    return;
}

=head3 C<row>

  my $row = $query->row;

Returns the L<Row|DBIx::DBO::Row> object for the current row from the query or an empty L<Row|DBIx::DBO::Row> object if there is no current row.

=cut

sub row {
    my $me = $_[0];
    $me->sql; # Build the SQL and detach the Row if needed
    $me->{Row} ||= $me->_row_class->new($me->{DBO}, $me);
}

=head3 C<run>

  $query->run;

Run/rerun the query.
This is called automatically before fetching the first row.

=cut

sub run {
    my $me = shift;
    $me->sql; # Build the SQL and detach the Row if needed
    if (defined $me->{Row}) {
        undef ${$me->{Row}}{array};
        ${$me->{Row}}{hash} = {};
    }

    my $rv = $me->_execute or return undef;
    $me->{Active} = 1;
    $me->_bind_cols_to_hash;
    if ($me->config('CacheQuery')) {
        $me->{cache}{data} = $me->{sth}->fetchall_arrayref;
        $me->{cache}{idx} = 0;
    } else {
        delete $me->{cache};
    }
    return $rv;
}

sub _execute {
    my $me = shift;
    $me->{DBO}{dbd_class}->_sql($me, $me->sql, $me->{DBO}{dbd_class}->_bind_params_select($me, $me->{build_data}));
    $me->_sth or return;
    $me->{sth}->execute($me->{DBO}{dbd_class}->_bind_params_select($me, $me->{build_data}));
}

sub _bind_cols_to_hash {
    my $me = shift;
    unless ($me->{hash}) {
        # Bind only to the first column of the same name
        @{$me->{Columns}} = @{$me->{sth}{NAME}};
        if ($me->config('CacheQuery')) {
            @{$me->{cache}{array}} = (undef) x @{$me->{Columns}};
            $me->{hash} = \my %hash;
            my $i = 0;
            for (@{$me->{Columns}}) {
                _hv_store(%hash, $_, $me->{cache}{array}[$i]) unless exists $hash{$_};
                $i++;
            }
        } else {
            my $i;
            for (@{$me->{Columns}}) {
                $i++;
                $me->{sth}->bind_col($i, \$me->{hash}{$_}) unless exists $me->{hash}{$_};
            }
        }
    }
}

=head3 C<rows>

  my $row_count = $query->rows;

Count the number of rows returned.
Returns undefined if the number is unknown.
This uses the DBI C<rows> method which is unreliable in some situations (See L<DBI-E<gt>rows|DBI/"rows">).

=cut

sub rows {
    my $me = shift;
    $me->sql; # Ensure the Row_Count is cleared if needed
    $me->{DBO}{dbd_class}->_rows($me) unless defined $me->{Row_Count};
    $me->{Row_Count};
}

=head3 C<count_rows>

  my $row_count = $query->count_rows;

Count the number of rows that would be returned.
Returns undefined if there is an error.

=cut

sub count_rows {
    my $me = shift;
    local $me->{Config}{CalcFoundRows} = 0;
    my $old_sb = delete $me->{build_data}{Show_Bind};
    $me->{build_data}{show} = '1';

    my $sql = 'SELECT COUNT(*) FROM ('.$me->{DBO}{dbd_class}->_build_sql_select($me, $me->{build_data}).') t';
    my($count) = $me->{DBO}{dbd_class}->_selectrow_array($me, $sql, undef,
        $me->{DBO}{dbd_class}->_bind_params_select($me, $me->{build_data}));

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
    $me->{DBO}{dbd_class}->_calc_found_rows($me) unless defined $me->{Found_Rows};
    $me->{Found_Rows};
}

=head3 C<sql>

  my $sql = $query->sql;

Returns the SQL statement string.

=cut

sub sql {
    my $me = shift;
    $me->{sql} || $me->_build_sql;
}

sub _build_sql {
    my $me = shift;
    undef $me->{sth};
    undef $me->{hash};
    undef $me->{Row_Count};
    undef $me->{Found_Rows};
    delete $me->{cache};
    $me->{Active} = 0;
    if (defined $me->{Row}) {
        if (SvREFCNT(${$me->{Row}}) > 1) {
            $me->{Row}->_detach;
        } else {
            undef ${$me->{Row}}{array};
            undef %{$me->{Row}};

            $me->{sql} = $me->{DBO}{dbd_class}->_build_sql_select($me, $me->{build_data});
            $me->{Row}->_copy_build_data;
            return $me->{sql};
        }
    }
    undef @{$me->{Columns}};

    $me->{sql} = $me->{DBO}{dbd_class}->_build_sql_select($me, $me->{build_data});
}

# Get the DBI statement handle for the query.
# It may not have been executed yet.
sub _sth {
    my $me = shift;
    # Ensure the sql is rebuilt if needed
    my $sql = $me->sql;
    $me->{sth} ||= $me->rdbh->prepare($sql);
}

=head3 C<finish>

  $query->finish;

Calls L<DBI-E<gt>finish|DBI/"finish"> on the statement handle, if it's active.
Restarts cached queries from the first row (if created using the C<CacheQuery> config).
This ensures that the next call to L</fetch> will return the first row from the query.

=cut

sub finish {
    my $me = shift;
    if (defined $me->{Row}) {
        if (SvREFCNT(${$me->{Row}}) > 1) {
            $me->{Row}->_detach;
        } else {
            undef ${$me->{Row}}{array};
            ${$me->{Row}}{hash} = {};
        }
    }
    if (exists $me->{cache}) {
        $me->{cache}{idx} = 0;
    } else {
        $me->{sth}->finish if $me->{sth} and $me->{sth}{Active};
        $me->{Active} = 0;
    }
}

=head2 Common Methods

These methods are accessible from all DBIx::DBO* objects.

=head3 C<dbo>

The C<DBO> object.

=head3 C<dbh>

The I<read-write> C<DBI> handle.

=head3 C<rdbh>

The I<read-only> C<DBI> handle, or if there is no I<read-only> connection, the I<read-write> C<DBI> handle.

=cut

sub dbo { $_[0]{DBO} }
sub dbh { $_[0]{DBO}->dbh }
sub rdbh { $_[0]{DBO}->rdbh }

=head3 C<config>

  $query_setting = $query->config($option);
  $query->config($option => $query_setting);

Get or set this C<Query> object's config settings.  When setting an option, the previous value is returned.  When getting an option's value, if the value is undefined, the L<DBIx::DBO|DBIx::DBO>'s value is returned.

See: L<DBIx::DBO/Available_config_options>.

=cut

sub config {
    my $me = shift;
    my $opt = shift;
    return $me->{DBO}{dbd_class}->_set_config($me->{Config} ||= {}, $opt, shift) if @_;
    $me->{DBO}{dbd_class}->_get_config($opt, $me->{Config} ||= {}, $me->{DBO}{Config}, \%DBIx::DBO::Config);
}

sub STORABLE_freeze {
    my($me, $cloning) = @_;
    return unless defined $me->{sth};

    local $me->{sth};
    local $me->{Active};
    local $me->{cache}{idx} = 0 if exists $me->{cache};
    return Storable::nfreeze($me);
}

sub STORABLE_thaw {
    my($me, $cloning, @frozen) = @_;
    %$me = %{ Storable::thaw(@frozen) };
    if (exists $me->{Row} and exists ${$me->{Row}}->{Parent}) {
        ${$me->{Row}}->{Parent} = $me;
        Scalar::Util::weaken ${$me->{Row}}->{Parent};
    }
}

sub DESTROY {
    undef %{$_[0]};
}

1;

__END__

=head2 Complex expressions

More complex expressions can be passed as hash references.
These expressions can be used in the L</show>, L</join_on>, L</where>, L</having>, L</group_by> and L</order_by> methods.

  $query->show({ FUNC => 'SUBSTR(?, 1, 1)', COL => 'name', AS => 'initial' });
  # MySQL would produce:  SELECT SUBSTR(`name`, 1, 1) AS `initial` FROM ...
  
  $query->where({ FUNC => "CONCAT(COALESCE(?, 'Mr.'), ' ', ?)", VAL => [$title, $t ** 'name'] }, '=', 'Dr. Jones');
  # MySQL would produce:  ... WHERE CONCAT(COALESCE(?, 'Mr.'), ' ', `name`) = 'Dr. Jones' ...
  
  $query->order_by('id', { FUNC => "COALESCE(?,'?')", COL => 'name', ORDER => 'DESC' });
  # MySQL would produce:  ... ORDER BY `id`, COALESCE(`name`,'?') DESC

The keys to the hash in a complex expression are:

=over 4

=item *

C<VAL> => A scalar, scalar reference or an array reference.

=item *

C<COL> => The name of a column or a Column object.

=item *

C<AS> => An alias name.

=item *

C<FUNC> => A string to be inserted B<unquoted> into the SQL, possibly containing B<?> placeholders.

=item *

C<COLLATE> => The collation for this value/field.

=item *

C<ORDER> => To order by a column (Used only in C<group_by> and C<order_by>).

=back

=head1 SUBCLASSING

Classes can easily be created for tables in your database.
Assume you want to create a C<Query> and C<Row> class for a "Users" table:

  package My::Users;
  our @ISA = qw(DBIx::DBO::Query);
  
  sub new {
      my($class, $dbo) = @_;
      
      # Create the Query for the "Users" table
      my $self = $class->SUPER::new($dbo, 'Users');
      
      # We could even add some JOINs or other clauses here ...
      
      return $self;
  }
  
  sub _row_class { 'My::User' } # Rows are blessed into this class

  package My::User;
  our @ISA = qw(DBIx::DBO::Row);
  
  sub new {
      my($class, $dbo, $parent) = @_;
      
      $parent ||= My::Users->new($dbo); # The Row will use the same table as it's parent
      
      $class->SUPER::new($dbo, $parent);
  }

=head1 SEE ALSO

L<DBIx::DBO>


=cut

