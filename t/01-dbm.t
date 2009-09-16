#!perl -T

use strict;
use warnings;
my $dir;
use Test::DBO DBM => 7, tempdir => \$dir;

# Create the DBO
my $dbo = Test::DBO::connect_ok("f_dir=$dir");
#my $dbo = Test::DBO::connect_ok("f_dir=$dir;mldbm=Storable");

my $test_tbl = $Test::DBO::prefix.'_tbl';

# Test methods: do, select* (4 tests)
Test::DBO::basic_methods($dbo, undef, $test_tbl);

