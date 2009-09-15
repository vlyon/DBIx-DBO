#!perl -T

use strict;
use warnings;
use File::Temp;
use Test::DBO DBM => 7;

# Run in a temporary directory
my $dir = File::Temp::tempdir('tmp_XXXX', CLEANUP => 1);

# Create the DBO
my $dbo = Test::DBO::connect_ok("f_dir=$dir");
#my $dbo = Test::DBO::connect_ok("f_dir=$dir;mldbm=Storable");

my $test_tbl = $Test::DBO::prefix.'_tbl';
my $quoted_tbl = $dbo->_qi($test_tbl);

# Test methods: do, select* (4 tests)
Test::DBO::basic_methods($dbo, $test_tbl, $quoted_tbl);

