#!perl -T

use strict;
use warnings;
use File::Temp;
my $dbo;
use Test::DBO SQLite => 8, tempdir => 1, connect_ok => [\$dbo];

my $test_tbl = $Test::DBO::prefix.'_tbl';

# Test methods: do, select* (4 tests)
Test::DBO::basic_methods($dbo, undef, $test_tbl);

