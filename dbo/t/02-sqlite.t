#!perl -T

use strict;
use warnings;
use File::Temp;
use Test::DBO SQLite => 6;

# Run in a temporary directory
my $dir = File::Temp::tempdir('tmpXXXX', CLEANUP => 1);
chdir $dir or die $!;

# Create the DBO
my $dbo = Test::DBO::connect_ok;

my $test_tbl = $Test::DBO::prefix.'_tbl';
my $quoted_tbl = $dbo->_qi($test_tbl);

# Test methods: do, select* (4 tests)
Test::DBO::basic_methods($dbo, $test_tbl, $quoted_tbl);

END {
    chdir '..';
}
